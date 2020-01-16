(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2020 Nomadic Labs. <contact@nomadic-labs.com>               *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

open Chain_validator_worker_state

module Name = struct
  type t = Chain_id.t

  let encoding = Chain_id.encoding

  let base = ["validator"; "chain"]

  let pp = Chain_id.pp_short
end

module Request = struct
  include Request

  type _ t = Validated : Store.Block.t -> Event.update t

  let view (type a) (Validated block : a t) : view = Store.Block.hash block
end

type bootstrap_conf = {
  max_latency : int;
  chain_stuck_delay : int;
  sync_polling_period : int;
  bootstrap_threshold : int;
}

type limits = {
  bootstrap_conf : bootstrap_conf;
  worker_limits : Worker_types.limits;
}

module Types = struct
  include Worker_state

  type parameters = {
    parent : (Name.t * bool) option;
    (* inherit bootstrap status from parent chain validator *)
    db : Distributed_db.t;
    chain_store : Store.chain_store;
    chain_db : Distributed_db.chain_db;
    block_validator : Block_validator.t;
    block_validator_process : Block_validator_process.t;
    global_valid_block_input : Store.Block.t Lwt_watcher.input;
    global_chains_input : (Chain_id.t * bool) Lwt_watcher.input;
    prevalidator_limits : Prevalidator.limits;
    peer_validator_limits : Peer_validator.limits;
    limits : limits;
  }

  type state = {
    parameters : parameters;
    mutable bootstrapped : bool;
    bootstrapped_waiter : unit Lwt.t;
    bootstrapped_wakener : unit Lwt.u;
    valid_block_input : Store.Block.t Lwt_watcher.input;
    new_head_input : Store.Block.t Lwt_watcher.input;
    mutable child : (state * (unit -> unit Lwt.t (* shutdown *))) option;
    mutable prevalidator : Prevalidator.t option;
    active_peers : Peer_validator.t P2p_peer.Error_table.t;
  }

  let view (state : state) _ : view =
    let {bootstrapped; active_peers; _} = state in
    {
      bootstrapped;
      active_peers =
        P2p_peer.Error_table.fold_keys (fun id l -> id :: l) active_peers [];
    }
end

module Logger =
  Worker_logger.Make (Event) (Request)
    (struct
      let worker_name = "node_chain_validator"
    end)

module Worker = Worker.Make (Name) (Event) (Request) (Types) (Logger)
open Types

type t = Worker.infinite Worker.queue Worker.t

let table = Worker.create_table Queue

let shutdown w = Worker.shutdown w

let shutdown_child nv active_chains =
  Lwt_utils.may
    ~f:
      (fun ({parameters = {chain_store; global_chains_input; _}; _}, shutdown) ->
      let test_chain_id = Store.Chain.chain_id chain_store in
      Lwt_watcher.notify global_chains_input (test_chain_id, false) ;
      Chain_id.Table.remove active_chains test_chain_id ;
      Store.Chain.shutdown_testchain nv.parameters.chain_store
      >>= function
      | Error _err ->
          (* FIXME *)
          Lwt.return_unit
      | Ok () ->
          shutdown ()
          >>= fun () ->
          nv.child <- None ;
          Lwt.return_unit)
    nv.child

let notify_new_block w block =
  let nv = Worker.state w in
  Option.iter nv.parameters.parent ~f:(fun (id, _) ->
      try
        let w = List.assoc id (Worker.list table) in
        let nv = Worker.state w in
        Lwt_watcher.notify nv.valid_block_input block
      with Not_found -> ()) ;
  Lwt_watcher.notify nv.valid_block_input block ;
  Lwt_watcher.notify nv.parameters.global_valid_block_input block ;
  Worker.Queue.push_request_now w (Validated block)

(* Return [None] if there are less than [n] active peers. (an active peer
   has updated its head at least once).
   Otherwise, for the n active peers with the most recent head
   returns [Some (min_head_time, max_head_time, most_recent_validation)] where
   - [min_head_time] is the timestamp of the least recent validated head
   - [max_head_time] is the timestamp of the most recent validated head
   - [most_recent_validation] is most recent time a head has been validated

   TODO this is inefficient, could be replaced by
   structure updated on peers head update *)
let time_peers active_peers n event_recorder =
  let head_time peer : Time.Protocol.t * Time.Protocol.t =
    Peer_validator.(current_head_timestamp peer, time_last_validated_head peer)
  in
  let f _peer_id peer acc =
    if Peer_validator.updated_once peer then head_time peer :: acc else acc
  in
  let time_list = P2p_peer.Error_table.fold_resolved f active_peers [] in
  let active_peers_count = List.length time_list in
  if active_peers_count < n then (
    event_recorder
      (Event.Bootstrap_active_peers {active = active_peers_count; needed = n}) ;
    None )
  else
    let compare (head_time_a, last_validation_a)
        (head_time_b, last_validation_b) =
      let u = Time.Protocol.compare head_time_a head_time_b in
      let v = Time.Protocol.compare last_validation_a last_validation_b in
      if u != 0 then u else v
    in
    (* take_n returns the n greatest elements, in *sorted* order *)
    let sorted_time_trunc = List.take_n ~compare n time_list in
    let (max_head_time, most_recent_validation) =
      List.last_exn sorted_time_trunc
    in
    let (min_head_time, _) = List.hd sorted_time_trunc in
    assert (min_head_time <= max_head_time) ;
    event_recorder
      (Event.Bootstrap_active_peers_heads_time
         {min_head_time; max_head_time; most_recent_validation}) ;
    Some (min_head_time, max_head_time, most_recent_validation)

let sync_state nv event_recorder =
  let bootstrap_threshold =
    nv.parameters.limits.bootstrap_conf.bootstrap_threshold
  in
  let chain_stuck_delay =
    nv.parameters.limits.bootstrap_conf.chain_stuck_delay
  in
  let max_latency = nv.parameters.limits.bootstrap_conf.max_latency in
  if bootstrap_threshold = 0 then `Sync
  else
    match time_peers nv.active_peers bootstrap_threshold event_recorder with
    | None ->
        `Unsync
    | Some (min_head_time, max_head_time, last_heard) ->
        let now = Time.System.to_protocol @@ Systime_os.now () in
        let some_time_from_now =
          Time.Protocol.add now (Int64.of_int (-chain_stuck_delay))
        in
        let almost_now = Time.Protocol.add now (Int64.of_int (-max_latency)) in
        if Time.Protocol.(min_head_time >= almost_now) then `Sync
        else if
          min_head_time = max_head_time && last_heard <= some_time_from_now
        then `Stuck
        else `Unsync

(** Check synchronization status every [sync_polling_period]. Wake up
    bootstrap wakener and mark node as bootstrapped when synchronization status
    is either stuck or sync. *)
let poll_sync nv event_recorder =
  let sync_polling_period =
    nv.parameters.limits.bootstrap_conf.sync_polling_period
  in
  if nv.bootstrapped then Lwt.wakeup_later nv.bootstrapped_wakener ()
  else
    let wait_sync nv =
      let rec loop () =
        match sync_state nv event_recorder with
        | `Stuck ->
            event_recorder (Event.Sync_status Stuck) ;
            Lwt.return_unit
        | `Sync ->
            event_recorder (Event.Sync_status Sync) ;
            Lwt.return_unit
        | `Unsync ->
            event_recorder (Event.Sync_status Unsync) ;
            Lwt_unix.sleep (float_of_int sync_polling_period)
            >>= fun () -> loop ()
      in
      loop ()
    in
    Lwt_utils.dont_wait
      (fun exc ->
        Format.eprintf "Uncaught exception: %s\n%!" (Printexc.to_string exc))
      (fun () ->
        wait_sync nv
        >>= fun () ->
        nv.bootstrapped <- true ;
        event_recorder Event.Bootstrapped ;
        Lwt.return (Lwt.wakeup_later nv.bootstrapped_wakener ()))

let with_activated_peer_validator w peer_id f =
  let nv = Worker.state w in
  P2p_peer.Error_table.find_or_make nv.active_peers peer_id (fun () ->
      Peer_validator.create
        ~notify_new_block:(notify_new_block w)
        ~notify_termination:(fun _pv ->
          P2p_peer.Error_table.remove nv.active_peers peer_id)
        nv.parameters.peer_validator_limits
        nv.parameters.block_validator
        nv.parameters.chain_db
        peer_id)
  >>=? fun pv ->
  match Peer_validator.status pv with
  | Worker_types.Running _ ->
      f pv
  | Worker_types.Closing (_, _)
  | Worker_types.Closed (_, _, _)
  | Worker_types.Launching _ ->
      return_unit

let may_switch_test_chain w active_chains spawn_child block =
  let nv = Worker.state w in
  let may_create_child block test_protocol expiration forking_block_hash =
    let block_header = Store.Block.header block in
    let genesis_hash = Context.compute_testchain_genesis forking_block_hash in
    let testchain_id = Context.compute_testchain_chain_id genesis_hash in
    ( match nv.child with
    | None ->
        Lwt.return_false
    | Some (child, _) ->
        let child_chain_store = child.parameters.chain_store in
        let child_genesis = Store.Chain.genesis child_chain_store in
        Lwt.return (Block_hash.equal child_genesis.block forking_block_hash) )
    >>= fun activated ->
    let expired = expiration < block_header.shell.timestamp in
    if expired && activated then
      shutdown_child nv active_chains >>= fun () -> return_unit
    else
      let chain_store = nv.parameters.chain_store in
      let allow_forked_chains =
        Store.allow_testchains (Store.Chain.global_store chain_store)
      in
      if (not allow_forked_chains) || activated || expired then return_unit
      else
        let chain_store = nv.parameters.chain_store in
        Store.Block.read_block_opt chain_store forking_block_hash
        >>= function
        | None ->
            (* Forking block is not found => cannot start the testchain *)
            return_unit
        | Some forking_block ->
            Store.get_chain_store_opt
              (Store.Chain.global_store nv.parameters.chain_store)
              testchain_id
            >>= (function
                  | Some test_chain_store ->
                      return test_chain_store
                  | None ->
                      let try_init_test_chain cont =
                        let bvp = nv.parameters.block_validator_process in
                        Block_validator_process.init_test_chain
                          bvp
                          nv.parameters.chain_store
                          forking_block
                        >>= function
                        | Ok genesis_header ->
                            Store.Chain.fork_testchain
                              chain_store
                              ~testchain_id
                              ~forked_block:block
                              ~genesis_hash
                              ~genesis_header
                              ~expiration
                              ~test_protocol
                            >>=? fun testchain ->
                            let testchain_store =
                              Store.Chain.testchain_store testchain
                            in
                            Store.Chain.current_head testchain_store
                            >>= fun new_genesis_block ->
                            Lwt_watcher.notify
                              nv.parameters.global_valid_block_input
                              new_genesis_block ;
                            Lwt_watcher.notify
                              nv.valid_block_input
                              new_genesis_block ;
                            return testchain_store
                        | Error
                            (Block_validator_errors.Missing_test_protocol
                               missing_protocol
                            :: _) ->
                            Block_validator.fetch_and_compile_protocol
                              nv.parameters.block_validator
                              missing_protocol
                            >>=? fun _ -> cont ()
                        | Error _ as error ->
                            Lwt.return error
                      in
                      try_init_test_chain
                      @@ fun () ->
                      try_init_test_chain
                      @@ fun () -> failwith "Could not retrieve test protocol")
            >>=? fun chain_store ->
            (* [spawn_child] is a callback to [create_node]. Thus, it takes care of
               global initialization boilerplate (e.g. notifying [global_chains_input],
               adding the chain to the correct tables, ...) *)
            spawn_child
              ~parent:(Store.Chain.chain_id chain_store, nv.bootstrapped)
              nv.parameters.peer_validator_limits
              nv.parameters.prevalidator_limits
              nv.parameters.block_validator
              nv.parameters.global_valid_block_input
              nv.parameters.global_chains_input
              nv.parameters.db
              chain_store
              nv.parameters.limits
            (* TODO: different limits main/test ? *)
            >>=? fun child ->
            nv.child <- Some child ;
            return_unit
  in
  Store.Chain.testchain_status nv.parameters.chain_store block
  >>= (function
        | (Not_running, _) ->
            shutdown_child nv active_chains >>= fun () -> return_unit
        | ((Forking _ | Running _), None) ->
            return_unit (* only for snapshots *)
        | ( ( Forking {protocol; expiration; _}
            | Running {protocol; expiration; _} ),
            Some forking_block_hash ) ->
            may_create_child block protocol expiration forking_block_hash)
  >>= function
  | Ok () ->
      Lwt.return_unit
  | Error err ->
      Worker.record_event w (Could_not_switch_testchain err) ;
      Lwt.return_unit

let broadcast_head w ~previous block =
  let nv = Worker.state w in
  if not nv.bootstrapped then Lwt.return_unit
  else
    Store.Block.read_predecessor_opt nv.parameters.chain_store block
    >>= (function
          | None ->
              Lwt.return_true
          | Some predecessor ->
              Lwt.return (Store.Block.equal predecessor previous))
    >>= fun successor ->
    if successor then (
      Distributed_db.Advertise.current_head nv.parameters.chain_db block ;
      Lwt.return_unit )
    else Distributed_db.Advertise.current_branch nv.parameters.chain_db

let safe_get_prevalidator_filter hash =
  match Prevalidator_filters.find hash with
  | Some filter ->
      return filter
  | None -> (
    match Registered_protocol.get hash with
    | None ->
        (* FIXME. *)
        (* This should not happen: it should be handled in the validator. *)
        failwith
          "chain_validator: missing protocol '%a' for the current block."
          Protocol_hash.pp_short
          hash
    | Some protocol ->
        Chain_validator_event.(emit prevalidator_filter_not_found) hash
        >>= fun () ->
        let (module Proto) = protocol in
        let module Filter = Prevalidator_filters.No_filter (Proto) in
        return (module Filter : Prevalidator_filters.FILTER) )

let on_request (type a) w start_testchain active_chains spawn_child
    (req : a Request.t) : a tzresult Lwt.t =
  let (Request.Validated block) = req in
  let nv = Worker.state w in
  let chain_store = nv.parameters.chain_store in
  Store.Chain.current_head chain_store
  >>= fun head ->
  let head_header = Store.Block.header head
  and head_hash = Store.Block.hash head
  and block_header = Store.Block.header block
  and block_hash = Store.Block.hash block in
  ( match nv.prevalidator with
  | None ->
      Lwt.return head_header.shell.fitness
  | Some pv ->
      Prevalidator.fitness pv )
  >>= fun context_fitness ->
  let head_fitness = head_header.shell.fitness in
  let new_fitness = block_header.shell.fitness in
  let accepted_head =
    if Fitness.(context_fitness = head_fitness) then
      Fitness.(new_fitness > head_fitness)
    else Fitness.(new_fitness >= context_fitness)
  in
  if not accepted_head then return Event.Ignored_head
  else
    Store.Chain.set_head chain_store block
    >>=? fun previous ->
    broadcast_head w ~previous block
    >>= fun () ->
    ( match nv.prevalidator with
    | Some old_prevalidator ->
        Store.Block.protocol_hash nv.parameters.chain_store block
        >>=? fun new_protocol ->
        let old_protocol = Prevalidator.protocol_hash old_prevalidator in
        if not (Protocol_hash.equal old_protocol new_protocol) then (
          safe_get_prevalidator_filter new_protocol
          >>=? fun (module Filter) ->
          let (limits, chain_db) = Prevalidator.parameters old_prevalidator in
          (* TODO inject in the new prevalidator the operation
                 from the previous one. *)
          Prevalidator.create limits (module Filter) chain_db
          >>= function
          | Error errs ->
              Chain_validator_event.(emit prevalidator_reinstantiation_failure)
                errs
              >>= fun () ->
              nv.prevalidator <- None ;
              Prevalidator.shutdown old_prevalidator >>= fun () -> return_unit
          | Ok prevalidator ->
              nv.prevalidator <- Some prevalidator ;
              Prevalidator.shutdown old_prevalidator >>= fun () -> return_unit
          )
        else Prevalidator.flush old_prevalidator block_hash
    | None ->
        return_unit )
    >>=? fun () ->
    ( if start_testchain then
      may_switch_test_chain w active_chains spawn_child block
    else Lwt.return_unit )
    >>= fun () ->
    Lwt_watcher.notify nv.new_head_input block ;
    if Block_hash.equal head_hash block_header.shell.predecessor then
      return Event.Head_increment
    else return Event.Branch_switch

let on_completion (type a) w (req : a Request.t) (update : a) request_status =
  let (Request.Validated block) = req in
  let fitness = Store.Block.fitness block in
  let request = Store.Block.hash block in
  let level = Store.Block.level block in
  let timestamp = Store.Block.timestamp block in
  Worker.record_event
    w
    (Processed_block
       {request; request_status; update; fitness; level; timestamp}) ;
  Lwt.return_unit

let on_close w =
  let nv = Worker.state w in
  Distributed_db.deactivate nv.parameters.chain_db
  >>= fun () ->
  let pvs =
    P2p_peer.Error_table.fold_promises
      (fun _ pv acc ->
        ( pv
        >>= function
        | Error _ -> Lwt.return_unit | Ok pv -> Peer_validator.shutdown pv )
        :: acc)
      nv.active_peers
      []
  in
  Lwt.join
    ( ( match nv.prevalidator with
      | Some prevalidator ->
          Prevalidator.shutdown prevalidator
      | None ->
          Lwt.return_unit )
    :: Lwt_utils.may ~f:(fun (_, shutdown) -> shutdown ()) nv.child
    :: pvs )

let on_launch start_prevalidator w _ parameters =
  ( if start_prevalidator then
    Store.Chain.current_head parameters.chain_store
    >>= fun current_head ->
    Store.Block.protocol_hash parameters.chain_store current_head
    >>=? fun head_hash ->
    safe_get_prevalidator_filter head_hash
    >>= function
    | Ok (module Proto) -> (
        Prevalidator.create
          parameters.prevalidator_limits
          (module Proto)
          parameters.chain_db
        >>= function
        | Error errs ->
            Chain_validator_event.(emit prevalidator_reinstantiation_failure)
              errs
            >>= fun () -> return_none
        | Ok prevalidator ->
            return_some prevalidator )
    | Error errs ->
        Chain_validator_event.(emit prevalidator_reinstantiation_failure) errs
        >>= fun () -> return_none
  else return_none )
  >>=? fun prevalidator ->
  let valid_block_input = Lwt_watcher.create_input () in
  let new_head_input = Lwt_watcher.create_input () in
  let (bootstrapped_waiter, bootstrapped_wakener) = Lwt.wait () in
  let bootstrapped =
    match parameters.parent with
    | Some (_, bootstrap_status) ->
        bootstrap_status
    | None ->
        parameters.limits.bootstrap_conf.bootstrap_threshold <= 0
  in
  let nv =
    {
      parameters;
      valid_block_input;
      new_head_input;
      bootstrapped_wakener;
      bootstrapped_waiter;
      bootstrapped;
      active_peers = P2p_peer.Error_table.create 50;
      (* TODO use `2 * max_connection` *)
      child = None;
      prevalidator;
    }
  in
  poll_sync nv (Worker.record_event w) ;
  Distributed_db.set_callback
    parameters.chain_db
    {
      notify_branch =
        (fun peer_id locator ->
          Error_monad.dont_wait
            (fun exc ->
              Format.eprintf
                "Uncaught exception: %s\n%!"
                (Printexc.to_string exc))
            (fun trace ->
              Format.eprintf
                "Uncaught error: %a\n%!"
                Error_monad.pp_print_error
                trace)
            (fun () ->
              with_activated_peer_validator w peer_id
              @@ fun pv ->
              Peer_validator.notify_branch pv locator ;
              return_unit));
      notify_head =
        (fun peer_id block ops ->
          Error_monad.dont_wait
            (fun exc ->
              Format.eprintf
                "Uncaught exception: %s\n%!"
                (Printexc.to_string exc))
            (fun trace ->
              Format.eprintf
                "Uncaught error: %a\n%!"
                Error_monad.pp_print_error
                trace)
            (fun () ->
              with_activated_peer_validator w peer_id (fun pv ->
                  Peer_validator.notify_head pv block ;
                  return_unit)
              >>=? fun () ->
              (* TODO notify prevalidator only if head is known ??? *)
              match nv.prevalidator with
              | Some prevalidator ->
                  Prevalidator.notify_operations prevalidator peer_id ops
                  >>= fun () -> return_unit
              | None ->
                  return_unit));
      disconnection =
        (fun peer_id ->
          Error_monad.dont_wait
            (fun exc ->
              Format.eprintf
                "Uncaught exception: %s\n%!"
                (Printexc.to_string exc))
            (fun trace ->
              Format.eprintf
                "Uncaught error: %a\n%!"
                Error_monad.pp_print_error
                trace)
            (fun () ->
              let nv = Worker.state w in
              match P2p_peer.Error_table.find_opt nv.active_peers peer_id with
              | None ->
                  return_unit
              | Some pv ->
                  pv >>=? fun pv -> Peer_validator.shutdown pv >>= return));
    } ;
  return nv

let rec create ~start_prevalidator ~start_testchain ~active_chains ?parent
    ~block_validator_process peer_validator_limits prevalidator_limits
    block_validator global_valid_block_input global_chains_input db chain_store
    limits =
  let spawn_child ~parent pvl pl bl gvbi gci db n l =
    create
      ~start_prevalidator
      ~start_testchain
      ~active_chains
      ~parent
      ~block_validator_process
      pvl
      pl
      bl
      gvbi
      gci
      db
      n
      l
    >>=? fun w -> return (Worker.state w, fun () -> Worker.shutdown w)
  in
  let module Handlers = struct
    type self = t

    let on_launch = on_launch start_prevalidator

    let on_request w = on_request w start_testchain active_chains spawn_child

    let on_close = on_close

    let on_error _ _ _ errs = Lwt.return_error errs

    let on_completion = on_completion

    let on_no_request _ = return_unit
  end in
  let parameters =
    {
      parent;
      peer_validator_limits;
      prevalidator_limits;
      block_validator;
      block_validator_process;
      global_valid_block_input;
      global_chains_input;
      db;
      chain_db = Distributed_db.activate db chain_store;
      chain_store;
      limits;
    }
  in
  Worker.launch
    table
    prevalidator_limits.worker_limits
    (Store.Chain.chain_id chain_store)
    parameters
    (module Handlers)
  >>=? fun w ->
  Chain_id.Table.add active_chains (Store.Chain.chain_id chain_store) w ;
  Lwt_watcher.notify
    global_chains_input
    (Store.Chain.chain_id chain_store, true) ;
  return w

(** Current block computation *)

let create ~start_prevalidator ~start_testchain ~active_chains
    ~block_validator_process peer_validator_limits prevalidator_limits
    block_validator global_valid_block_input global_chains_input global_db
    state limits =
  (* hide the optional ?parent *)
  create
    ~start_prevalidator
    ~start_testchain
    ~active_chains
    ~block_validator_process
    peer_validator_limits
    prevalidator_limits
    block_validator
    global_valid_block_input
    global_chains_input
    global_db
    state
    limits

let chain_id w =
  let {parameters = {chain_store; _}; _} = Worker.state w in
  Store.Chain.chain_id chain_store

let chain_store w =
  let {parameters = {chain_store; _}; _} = Worker.state w in
  chain_store

let prevalidator w =
  let {prevalidator; _} = Worker.state w in
  prevalidator

let chain_db w =
  let {parameters = {chain_db; _}; _} = Worker.state w in
  chain_db

let child w =
  match (Worker.state w).child with
  | None ->
      None
  | Some ({parameters = {chain_store; _}; _}, _) -> (
    try
      Some (List.assoc (Store.Chain.chain_id chain_store) (Worker.list table))
    with Not_found -> None )

let assert_fitness_increases ?(force = false) w distant_header =
  let pv = Worker.state w in
  let chain_store = Distributed_db.chain_store pv.parameters.chain_db in
  Store.Chain.current_head chain_store
  >>= fun current_head ->
  fail_when
    ( (not force)
    && Fitness.compare
         distant_header.Block_header.shell.fitness
         (Store.Block.fitness current_head)
       <= 0 )
    (failure "Fitness too low")

let assert_checkpoint w ((hash, _) as block_descr) =
  let pv = Worker.state w in
  let chain_store = Distributed_db.chain_store pv.parameters.chain_db in
  Store.Chain.is_acceptable_block chain_store block_descr
  >>= fun acceptable ->
  fail_unless acceptable (Validation_errors.Checkpoint_error (hash, None))

let validate_block w ?force hash block operations =
  let nv = Worker.state w in
  let hash' = Block_header.hash block in
  assert (Block_hash.equal hash hash') ;
  assert_fitness_increases ?force w block
  >>=? fun () ->
  assert_checkpoint w (hash, block.Block_header.shell.level)
  >>=? fun () ->
  Block_validator.validate
    ~canceler:(Worker.canceler w)
    ~notify_new_block:(notify_new_block w)
    nv.parameters.block_validator
    nv.parameters.chain_db
    hash
    block
    operations

let bootstrapped w =
  let {bootstrapped_waiter; _} = Worker.state w in
  Lwt.protected bootstrapped_waiter

let is_bootstrapped w = (Worker.state w).bootstrapped

let valid_block_watcher w =
  let {valid_block_input; _} = Worker.state w in
  Lwt_watcher.create_stream valid_block_input

let new_head_watcher w =
  let {new_head_input; _} = Worker.state w in
  Lwt_watcher.create_stream new_head_input

let status = Worker.status

let information = Worker.information

let running_workers () = Worker.list table

let pending_requests t = Worker.Queue.pending_requests t

let pending_requests_length t = Worker.Queue.pending_requests_length t

let current_request t = Worker.current_request t

let last_events = Worker.last_events

let ddb_information t =
  let state = Worker.state t in
  let ddb = state.parameters.chain_db in
  Distributed_db.information ddb

let sync_state w =
  let nv = Worker.state w in
  (* we could pass the full worker here, but only the event recorder is
     needed *)
  sync_state nv (Worker.record_event w)
