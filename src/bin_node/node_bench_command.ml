(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2019-2021 Nomadic Labs, <contact@nomadic-labs.com>          *)
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

include Internal_event.Simple

let section = ["bench_command"]

let without_validation_event =
  declare_0
    ~section
    ~name:"without_validation_event"
    ~msg:"no validation while freezing the store"
    ~level:Notice
    ()

let with_validation_event =
  declare_3
    ~section
    ~name:"with_validation_event"
    ~msg:
      "validating blocks from {dir}, with a {delay} seconds delay between \
       blocks, using {validation_mode}, while freezing the store"
    ~level:Notice
    ("dir", Data_encoding.string)
    ("delay", Data_encoding.float)
    ("validation_mode", Data_encoding.string)

let validating_cycle_event =
  declare_2
    ~section
    ~name:"validating_cycle_event"
    ~msg:"validating cycle from block {start} to {end}"
    ~level:Notice
    ("start", Data_encoding.int32)
    ("end", Data_encoding.int32)

let validating_block_event =
  declare_2
    ~section
    ~name:"validating_block_event"
    ~msg:"validating block {level} ({hash})"
    ~level:Notice
    ("level", Data_encoding.int32)
    ~pp2:Block_hash.pp
    ("hash", Block_hash.encoding)

let validation_canceled_event =
  declare_0
    ~section
    ~name:"validation_canceled_event"
    ~msg:"the block validation was canceled"
    ~level:Notice
    ()

let freeze_info_event =
  declare_4
    ~section
    ~name:"freeze_info_event"
    ~msg:
      "freeze info:\n\
       current_min_lower: {current_min_lower_level}\n\
       target_max_upper: {max_upper_level}\n\
       target_min_upper: {min_upper_level}\n\
       target_max_lower: {max_lower_level}"
    ~level:Notice
    ("current_min_lower_level", Data_encoding.int32)
    ("max_upper_level", Data_encoding.int32)
    ("min_upper_level", Data_encoding.int32)
    ("max_lower_level", Data_encoding.int32)

let start_freeze_event =
  declare_0
    ~section
    ~name:"start_freeze_event"
    ~msg:"start freeze on context"
    ~level:Notice
    ()

let freeze_finished_event =
  declare_0
    ~section
    ~name:"freeze_finished_event"
    ~msg:"freeze on context finished!"
    ~level:Notice
    ()

let close_store_event =
  declare_0
    ~section
    ~name:"close_store_event"
    ~msg:"closing the store (forces freeze to finish)"
    ~level:Notice
    ()

(** Main *)
let chain_status_event =
  declare_2
    ~section
    ~name:"chain_status_event"
    ~msg:"chain status: current head {head_level} ({hash})"
    ~level:Notice
    ("head_level", Data_encoding.int32)
    ~pp2:Block_hash.pp
    ("hash", Block_hash.encoding)

let init_validator ~singleprocess ~data_dir ~store_root ~context_root
    ~protocol_root validator_environment genesis =
  let open Block_validator_process in
  if singleprocess then
    Store.init
      ~store_dir:store_root
      ~context_dir:context_root
      ~allow_testchains:false
      genesis
    >>=? fun store ->
    let main_chain_store = Store.main_chain_store store in
    init validator_environment (Internal main_chain_store)
    >>=? fun validator_process -> return (validator_process, store)
  else
    init
      validator_environment
      (External
         {
           data_dir;
           genesis;
           context_root;
           protocol_root;
           process_path = Sys.executable_name;
           sandbox_parameters = None;
         })
    >>=? fun validator_process ->
    Store.init
      ~readonly:true
      ~store_dir:store_root
      ~context_dir:context_root
      ~allow_testchains:false
      genesis
    >>=? fun store -> return (validator_process, store)

let ( -- ) i j =
  WithExceptions.Result.get_ok ~loc:__LOC__
  @@ List.init
       ~when_negative_length:(Failure "list init exn")
       Int32.(to_int j - to_int i + 1)
       (fun x -> Int32.(add (of_int x) i))

let validation_loop main_chain_store validator_process cemented_block_store
    ~starting_from ~delay =
  ( match Cemented_block_store.cemented_blocks_files cemented_block_store with
  | None ->
      failwith "Nothing to validate!"
  | Some cemented_files ->
      (* Filter cycles to iter over the necessary ones only *)
      let cemented_files =
        List.filter
          (fun {Cemented_block_store.start_level; end_level; _} ->
            start_level >= starting_from || starting_from <= end_level)
          (Array.to_list cemented_files)
      in
      Store.Block.read_block_by_level
        main_chain_store
        (Int32.pred starting_from)
      >>=? fun head_block ->
      List.fold_left_es
        (fun block {Cemented_block_store.start_level; end_level; _} ->
          emit validating_cycle_event (start_level, end_level)
          >>= fun () ->
          let start_level = max starting_from start_level in
          let batch = start_level -- end_level in
          List.fold_left_es
            (fun predecessor block_level ->
              Cemented_block_store.get_cemented_block_by_level
                ~read_metadata:true
                cemented_block_store
                block_level
              >>=? fun block ->
              let block =
                match block with None -> assert false | Some b -> b
              in
              let operations = Block_repr.operations block in
              let block_header = Block_repr.header block in
              let block_hash = Block_repr.hash block in
              emit validating_block_event (block_level, block_hash)
              >>= fun () ->
              Block_validator_process.apply_block
                validator_process
                main_chain_store
                ~predecessor
                block_header
                operations
              >>=? fun _validation_result ->
              Lwt_unix.sleep delay
              >>= fun () -> return (Store.Unsafe.block_of_repr block))
            block
            batch)
        head_block
        cemented_files )
  >>=? fun _ignore -> return_unit

let validate_blocks main_chain_store validator_process ~dir ~delay chain_id
    ~starting_from =
  let fake_store_dir = Naming.store_dir ~dir_path:dir in
  let fake_chain_dir = Naming.chain_dir fake_store_dir chain_id in
  Cemented_block_store.init fake_chain_dir ~readonly:true
  >>=? fun cemented_block_store ->
  Lwt.catch
    (fun () ->
      validation_loop
        main_chain_store
        validator_process
        cemented_block_store
        ~starting_from
        ~delay)
    (function
      | Lwt.Canceled ->
          emit validation_canceled_event () >>= fun () -> return_unit
      | exn ->
          Lwt.fail exn)

(* [freeze_termination_promise] aims to block while the context is
   executing a freeze. *)
let freeze_termination_promise context =
  let rec aux () =
    if Context.is_freezing context then
      (* Arbitrary long timeout to avoid important overhead, as the
         freeze is expected to be quite long *)
      Lwt_unix.sleep 10. >>= fun () -> aux ()
    else Lwt.return_unit
  in
  aux ()

let print_freeze_chunk main_chain_store ~current_min_upper_level
    ~next_min_upper_level =
  Store.Block.read_block_by_level main_chain_store next_min_upper_level
  >>=? fun block ->
  Format.printf
    "Moving %ld commits from upper to lower:@."
    (Int32.sub next_min_upper_level current_min_upper_level) ;
  let rec aux block =
    let level = Store.Block.level block in
    let hash = Store.Block.hash block in
    let context_hash = Store.Block.context_hash block in
    Format.printf
      "%ld (%a) -> %a@."
      level
      Block_hash.pp_short
      hash
      Context_hash.pp
      context_hash ;
    Store.Block.read_block main_chain_store (Store.Block.predecessor block)
    >>=? fun pred ->
    if level <= current_min_upper_level then return_unit else aux pred
  in
  aux block

module Term = struct
  let process args _sandbox_file validating delay singleprocess =
    let run =
      Internal_event_unix.init ()
      >>= fun () ->
      Node_shared_arg.read_and_patch_config_file args
      >>=? fun node_config ->
      let data_dir = node_config.data_dir in
      let ({genesis; alias; _} : Node_config_file.blockchain_network) =
        node_config.blockchain_network
      in
      let (validator_environment
            : Block_validator_process.validator_environment) =
        {
          user_activated_upgrades =
            node_config.blockchain_network.user_activated_upgrades;
          user_activated_protocol_overrides =
            node_config.blockchain_network.user_activated_protocol_overrides;
        }
      in
      let delay = Option.value delay ~default:0. in
      let context_dir = Node_data_version.context_dir data_dir in
      let store_dir = Node_data_version.store_dir data_dir in
      let protocol_dir = Node_data_version.protocol_dir data_dir in
      ( match validating with
      | None ->
          emit without_validation_event ()
          >>= fun () ->
          Store.init
            ~readonly:false
            ~store_dir
            ~context_dir
            ~allow_testchains:false
            genesis
          >>=? fun store ->
          let validation_callback ~starting_from:_ () = return_unit in
          let context = Store.context_index store in
          let freeze_callback ~max_upper ~min_upper () =
            Context.freeze context ~max_upper ~min_upper
            >>= fun () ->
            (* Force the freeze to terminate *)
            freeze_termination_promise context
          in
          return (validation_callback, freeze_callback, store)
      | Some dir ->
          init_validator
            ~singleprocess
            ~data_dir
            ~store_root:store_dir
            ~context_root:context_dir
            ~protocol_root:protocol_dir
            validator_environment
            genesis
          >>=? fun (validator_process, store) ->
          let vm =
            Format.sprintf
              "%s"
              ((function
                 | true ->
                     "single process"
                 | false ->
                     "external validation process")
                 singleprocess)
          in
          emit with_validation_event (dir, delay, vm)
          >>= fun () ->
          let main_chain_store = Store.main_chain_store store in
          let chain_id = Store.Chain.chain_id main_chain_store in
          let validation_callback ~starting_from () =
            Lwt.finalize
              (fun () ->
                validate_blocks
                  main_chain_store
                  validator_process
                  ~dir
                  ~delay
                  chain_id
                  ~starting_from)
              (fun () -> Block_validator_process.close validator_process)
          in
          let freeze_callback ~max_upper ~min_upper () =
            Block_validator_process.unload
              validator_process
              main_chain_store
              ~max_upper
              ~min_upper
            >>= function
            | Ok () ->
                let context = Store.context_index store in
                (* Force the freeze to terminate *)
                freeze_termination_promise context
            | Error errs ->
                Lwt.fail_with
                  (Format.asprintf
                     "Error while freezing: %a@."
                     Error_monad.pp_print_error
                     errs)
          in
          return (validation_callback, freeze_callback, store) )
      >>=? fun (validation_callback, freeze_callback, store) ->
      let main_chain_store = Store.main_chain_store store in
      Store.Chain.current_head main_chain_store
      >>= fun current_head ->
      let head_context_hash = Store.Block.context_hash current_head in
      let dirty_compute_blocks_per_cycle = function
        | None | Some "mainnet" ->
            return 4096l
        | Some "florencenet" ->
            return 2048l
        | Some ntwk ->
            failwith
              "Plese hardcode the blocks per cycle for that network (%s)"
              ntwk
      in
      dirty_compute_blocks_per_cycle alias
      >>=? fun blocks_per_cycle ->
      let head_level = Store.Block.level current_head in
      let head_hash = Store.Block.hash current_head in
      (* The min upper level is hardcoded to be the block
         corresponding to 5 cycles in the past. Thus we have:
         - max_upper: head
         - min_upper: head - 5 cycles
         - max_lower: head - 5 cycles
         As 6 cycles were in the upper, we move one cycle from upper to lower.
      *)
      let current_min_upper_level =
        Int32.(add (sub head_level (mul 6l blocks_per_cycle)) 1l)
      in
      let next_min_upper_level =
        Int32.(add (sub head_level (mul 5l blocks_per_cycle)) 1l)
      in
      Store.Block.read_block_by_level main_chain_store next_min_upper_level
      >>=? fun min_upper_block ->
      let min_upper_context_hash = Store.Block.context_hash min_upper_block in
      emit chain_status_event (head_level, head_hash)
      >>= fun () ->
      let freeze_promise =
        Lwt.no_cancel
          ( emit
              freeze_info_event
              ( current_min_upper_level,
                head_level,
                next_min_upper_level,
                next_min_upper_level )
          >>= fun () ->
          print_freeze_chunk
            main_chain_store
            ~current_min_upper_level
            ~next_min_upper_level
          >>=? fun () ->
          emit start_freeze_event ()
          >>= fun () ->
          freeze_callback
            ~max_upper:[head_context_hash]
            ~min_upper:min_upper_context_hash
            ()
          >>= fun () -> emit freeze_finished_event () >>= fun () -> return_unit
          )
      in
      let validation_promise =
        validation_callback ~starting_from:head_level ()
      in
      Lwt.pick [freeze_promise; validation_promise]
      >>=? fun () ->
      freeze_promise
      >>=? fun () ->
      validation_promise
      >>=? fun () ->
      emit close_store_event ()
      >>= fun () -> Store.close_store store >>= fun () -> return_unit
    in
    match Lwt_main.run @@ Lwt_exit.wrap_and_exit run with
    | Ok () ->
        `Ok ()
    | Error err ->
        `Error (false, Format.asprintf "%a" pp_print_error err)

  let sandbox =
    let open Cmdliner in
    let doc =
      "TODO DESCR"
      (* "Run the bench command in sandbox mode. P2P to non-localhost addresses \
       *  are disabled, and constants of the economic protocol can be altered \
       *  with an optional JSON file. $(b,IMPORTANT): Using sandbox mode affects \
       *  the node state and subsequent runs of Tezos node must also use sandbox \
       *  mode. In order to run the node in normal mode afterwards, a full reset \
       *  must be performed (by removing the node's data directory)." *)
    in
    Arg.(
      value
      & opt (some non_dir_file) None
      & info
          ~docs:Node_shared_arg.Manpage.misc_section
          ~doc
          ~docv:"FILE.json"
          ["sandbox"])

  let validating =
    let open Cmdliner in
    let doc = "Blocks to validate while freezing the context." in
    Arg.(
      value
      & opt (some dir) None
      & info
          ~docs:Node_shared_arg.Manpage.misc_section
          ~doc
          ~docv:"PATH"
          ["validating"])

  let delay =
    let open Cmdliner in
    let doc =
      "Delay between validated blocks. Used only if the `--validating` option \
       is given."
    in
    Arg.(
      value
      & opt (some float) None
      & info
          ~docs:Node_shared_arg.Manpage.misc_section
          ~doc
          ~docv:"float"
          ["delay"])

  let singleprocess =
    let open Cmdliner in
    let doc =
      "When enabled, it deactivates block validation using an external \
       process. Thus, the validation procedure is done in the same process as \
       the node and might not be responding when doing extensive I/Os."
    in
    Arg.(
      value & flag
      & info ~docs:Node_shared_arg.Manpage.misc_section ~doc ["singleprocess"])

  let term =
    let open Cmdliner.Term in
    ret
      ( const process $ Node_shared_arg.Term.args $ sandbox $ validating $ delay
      $ singleprocess )
end

module Manpage = struct
  let command_description = "TODO DESCR"

  let description = [`S "DESCRIPTION"; `P command_description]

  let options = []

  let examples = [`S "EXAMPLES"; `I ("$(b,example )", "./tezos-node bench ")]

  let man = description @ options @ examples @ Node_shared_arg.Manpage.bugs

  let info = Cmdliner.Term.info ~doc:"Manage storage benchmarks" ~man "bench"
end

let cmd = (Term.term, Manpage.info)
