(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2020 Nomadic Labs, <contact@nomadic-labs.com>               *)
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

open Store_types
open Store_errors
open Store_events

module Shared = struct
  type 'a t = {mutable data : 'a; lock : Lwt_idle_waiter.t}

  let create data = {data; lock = Lwt_idle_waiter.create ()}

  let use {data; lock} f = Lwt_idle_waiter.task lock (fun () -> f data)

  (* Causes a deadlock if [use] or [update_with] is called inside [f] *)
  let locked_use {data; lock} f =
    Lwt_idle_waiter.force_idle lock (fun () -> f data)

  (* Causes a deadlock if [use] or [locked_use] is called inside [f] *)
  let update_with v f =
    Lwt_idle_waiter.force_idle v.lock (fun () ->
        f v.data
        >>=? function
        | (Some new_data, res) ->
            v.data <- new_data ;
            return res
        | (None, res) ->
            return res)
end

type store = {
  store_dir : string;
  (* Mutability allows a back-reference from chain_store to store: not
     to be modified. *)
  (* Invariant : main_chain_store <> None *)
  mutable main_chain_store : chain_store option;
  context_index : Context.index;
  protocol_store : Protocol_store.t;
  allow_testchains : bool;
  protocol_watcher : Protocol_hash.t Lwt_watcher.input;
  global_block_watcher : (chain_store * block) Lwt_watcher.input;
}

and chain_store = {
  global_store : store;
  chain_id : Chain_id.t;
  chain_dir : string;
  chain_config : Chain_config.t;
  block_store : Block_store.t;
  chain_state : chain_state Shared.t;
  block_watcher : block Lwt_watcher.input;
  block_rpc_directories :
    (chain_store * block) RPC_directory.t Protocol_hash.Map.t
    Protocol_hash.Table.t;
  lockfile : Lwt_unix.file_descr;
}

and chain_state = {
  genesis : block Stored_data.t;
  current_head : block Stored_data.t;
  alternate_heads : int32 Block_hash.Map.t Stored_data.t;
  checkpoint : block_descriptor;
  checkpoint_data : block_descriptor Stored_data.t;
  savepoint : block_descriptor;
  savepoint_data : block_descriptor Stored_data.t;
  caboose : block_descriptor;
  caboose_data : block_descriptor Stored_data.t;
  protocol_levels :
    Protocol_levels.activation_block Protocol_levels.t Stored_data.t;
  invalid_blocks : invalid_block Block_hash.Map.t Stored_data.t;
  forked_chains : Block_hash.t Chain_id.Map.t Stored_data.t;
  (* In memory *)
  current_head_metadata : Block_repr.metadata;
  active_testchain : testchain option;
  live_blocks : Block_hash.Set.t;
  live_operations : Operation_hash.Set.t;
  mempool : Mempool.t;
}

and testchain = {forked_block : Block_hash.t; testchain_store : chain_store}

and block = Block_repr.t

type t = store

let current_head chain_store =
  Shared.use chain_store.chain_state (fun {current_head; _} ->
      Stored_data.read current_head)

let caboose chain_store =
  Shared.use chain_store.chain_state (fun {caboose; _} -> Lwt.return caboose)

let checkpoint chain_store =
  Shared.use chain_store.chain_state (fun {checkpoint; _} ->
      Lwt.return checkpoint)

let savepoint chain_store =
  Shared.use chain_store.chain_state (fun {savepoint; _} ->
      Lwt.return savepoint)

let genesis chain_store = chain_store.chain_config.genesis

let history_mode chain_store = chain_store.chain_config.history_mode

let read_ancestor_hash {block_store; _} ~distance hash =
  Block_store.get_hash block_store (Block (hash, distance))

let get_highest_cemented_level chain_store =
  Cemented_block_store.get_highest_cemented_level
    (Block_store.cemented_block_store chain_store.block_store)

(* Will that block be compatible with the current state of the store? *)
let locked_is_acceptable_block chain_state (hash, level) =
  let level_limit =
    Block_repr.last_allowed_fork_level chain_state.current_head_metadata
  in
  (* The block must at least be above the highest cemented block
         (or savepoint when no blocks are cemented) *)
  if Compare.Int32.(level_limit >= level) then Lwt.return_false
  else
    let (checkpoint_hash, checkpoint_level) = chain_state.checkpoint in
    if Compare.Int32.(checkpoint_level < level) then
      (* the predecessor is assumed compatible. *)
      Lwt.return_true
    else if Compare.Int32.(checkpoint_level = level) then
      Lwt.return (Block_hash.equal hash checkpoint_hash)
    else
      Stored_data.read chain_state.current_head
      >>= fun head -> Lwt.return (Block_repr.level head < checkpoint_level)

let create_lockfile ~chain_dir =
  Lwt_unix.openfile
    Naming.(chain_dir // lockfile)
    [Unix.O_CREAT; O_RDWR; O_CLOEXEC; O_SYNC]
    0o644

let lock_for_write lockfile = Lwt_unix.lockf lockfile Unix.F_LOCK 0

let lock_for_read lockfile = Lwt_unix.lockf lockfile Unix.F_RLOCK 0

let unlock lockfile = Lwt_unix.lockf lockfile Unix.F_ULOCK 0

let may_unlock lockfile =
  Lwt.catch (fun () -> unlock lockfile) (fun _ -> Lwt.return_unit)

module Block = struct
  type nonrec block = block

  type t = block

  type metadata = Block_repr.metadata = {
    message : string option;
    max_operations_ttl : int;
    last_allowed_fork_level : Int32.t;
    block_metadata : Bytes.t;
    operations_metadata : Bytes.t list list;
  }

  let equal b b' = Block_hash.equal (Block_repr.hash b) (Block_repr.hash b')

  let descriptor blk = (Block_repr.hash blk, Block_repr.level blk)

  (* I/O operations *)

  let is_known_valid {block_store; _} hash =
    Block_store.(mem block_store (Block (hash, 0)))
    >>= function
    | Ok k ->
        Lwt.return k
    | Error _ ->
        (* should never happen : (0 \in N) *)
        Lwt.return_false

  let locked_is_known_invalid chain_state hash =
    let {invalid_blocks; _} = chain_state in
    Stored_data.read invalid_blocks
    >>= fun invalid_blocks ->
    Lwt.return (Block_hash.Map.mem hash invalid_blocks)

  let is_known_invalid {chain_state; _} hash =
    Shared.use chain_state (fun chain_state ->
        locked_is_known_invalid chain_state hash)

  let is_known chain_store hash =
    is_known_valid chain_store hash
    >>= fun is_known ->
    if is_known then Lwt.return_true else is_known_invalid chain_store hash

  let validity chain_store hash =
    is_known chain_store hash
    >>= function
    | false ->
        Lwt.return Block_locator.Unknown
    | true -> (
        is_known_invalid chain_store hash
        >>= function
        | true ->
            Lwt.return Block_locator.Known_invalid
        | false ->
            Lwt.return Block_locator.Known_valid )

  let is_genesis chain_store hash =
    let genesis = genesis chain_store in
    Block_hash.equal hash genesis.Genesis.block

  let read_block {block_store; _} ?(distance = 0) hash =
    Block_store.read_block
      ~read_metadata:false
      block_store
      (Block (hash, distance))
    >>=? function
    | None ->
        (* TODO lift the error to block_store *)
        fail (Block_not_found hash)
    | Some block ->
        return block

  let read_block_metadata ?(distance = 0) chain_store hash =
    Block_store.read_block_metadata
      chain_store.block_store
      (Block (hash, distance))

  let read_block_metadata_opt ?distance chain_store hash =
    read_block_metadata ?distance chain_store hash
    >>= function Ok v -> Lwt.return v | Error _ -> Lwt.return_none

  let get_block_metadata_opt chain_store block =
    match Block_repr.metadata block with
    | Some metadata ->
        Lwt.return_some metadata
    | None -> (
        read_block_metadata_opt chain_store block.hash
        >>= function
        | Some metadata ->
            (* Put the metadata in cache *)
            block.metadata <- Some metadata ;
            Lwt.return_some metadata
        | None ->
            Lwt.return_none )

  let get_block_metadata chain_store block =
    get_block_metadata_opt chain_store block
    >>= function
    | Some metadata ->
        return metadata
    | None ->
        fail (Block_metadata_not_found (Block_repr.hash block))

  let read_block_opt chain_store ?(distance = 0) hash =
    read_block chain_store ~distance hash
    >>= function
    | Ok block -> Lwt.return_some block | Error _ -> Lwt.return_none

  let read_predecessor chain_store block =
    read_block chain_store (Block_repr.predecessor block)

  let read_predecessor_opt chain_store block =
    read_predecessor chain_store block
    >>= function
    | Ok block -> Lwt.return_some block | Error _ -> Lwt.return_none

  let read_ancestor_hash chain_store ~distance hash =
    read_ancestor_hash chain_store ~distance hash

  let read_ancestor_hash_opt chain_store ~distance hash =
    read_ancestor_hash chain_store ~distance hash
    >>= function Ok v -> Lwt.return v | Error _ -> Lwt.return_none

  let read_predecessor_of_hash_opt chain_store hash =
    read_ancestor_hash_opt chain_store ~distance:1 hash
    >>= function
    | Some hash -> read_block_opt chain_store hash | None -> Lwt.return_none

  let read_predecessor_of_hash chain_store hash =
    read_predecessor_of_hash_opt chain_store hash
    >>= function Some b -> return b | None -> fail (Block_not_found hash)

  let locked_read_block_by_level chain_store head level =
    let distance = Int32.(to_int (sub (Block_repr.level head) level)) in
    if distance < 0 then
      fail
        (Bad_level
           {
             head_level = Block_repr.level head;
             given_level = Int32.of_int distance;
           })
    else read_block chain_store ~distance (Block_repr.hash head)

  let locked_read_block_by_level_opt chain_store head level =
    let distance = Int32.(to_int (sub (Block_repr.level head) level)) in
    if distance < 0 then Lwt.return_none
    else read_block_opt chain_store ~distance (Block_repr.hash head)

  let read_block_by_level chain_store level =
    current_head chain_store
    >>= fun current_head ->
    locked_read_block_by_level chain_store current_head level

  let read_block_by_level_opt chain_store level =
    current_head chain_store
    >>= fun current_head ->
    locked_read_block_by_level_opt chain_store current_head level

  let store_block chain_store ~block_header ~operations validation_result =
    let { Block_validation.validation_store =
            {context_hash; message; max_operations_ttl; last_allowed_fork_level};
          block_metadata;
          ops_metadata } =
      validation_result
    in
    let bytes = Block_header.to_bytes block_header in
    let hash = Block_header.hash_raw bytes in
    let operations_length = List.length operations in
    let operation_metadata_length = List.length ops_metadata in
    let validation_passes = block_header.shell.validation_passes in
    fail_unless
      (validation_passes = operations_length)
      (Cannot_store_block
         ( hash,
           Invalid_operations_length
             {validation_passes; operations = operations_length} ))
    >>=? fun () ->
    fail_unless
      (validation_passes = operation_metadata_length)
      (Cannot_store_block
         ( hash,
           Invalid_operations_length
             {validation_passes; operations = operation_metadata_length} ))
    >>=? fun () ->
    fail_unless
      (List.for_all2
         (fun l1 l2 -> List.length l1 = List.length l2)
         operations
         ops_metadata)
      (let to_string : type a. a list list -> string =
        fun l ->
         Format.asprintf
           "[%a]"
           (Format.pp_print_list
              ~pp_sep:(fun fmt () -> Format.fprintf fmt "; ")
              (fun ppf (l : a list) ->
                Format.fprintf ppf "[%d]" (List.length l)))
           l
       in
       Cannot_store_block
         ( hash,
           Inconsistent_operations_lengths
             {
               operations_lengths = to_string operations;
               operations_data_lengths = to_string ops_metadata;
             } ))
    >>=? fun () ->
    (* is_known returns true for invalid blocks as well *)
    is_known chain_store hash
    >>= fun is_known ->
    if is_known then return_none
    else
      (* Safety check: never ever commit a block that is not
         compatible with the current checkpoint. *)
      Shared.use chain_store.chain_state (fun chain_state ->
          locked_is_acceptable_block
            chain_state
            (hash, block_header.shell.level))
      >>= fun acceptable_block ->
      fail_unless
        acceptable_block
        (Validation_errors.Checkpoint_error (hash, None))
      >>=? fun () ->
      fail_unless
        (Context_hash.equal block_header.shell.context context_hash)
        (Validation_errors.Inconsistent_hash
           (context_hash, block_header.shell.context))
      >>=? fun () ->
      let contents =
        ({header = block_header; operations} : Block_repr.contents)
      in
      let metadata =
        Some
          {
            message;
            max_operations_ttl;
            last_allowed_fork_level;
            block_metadata;
            operations_metadata = ops_metadata;
          }
      in
      let block = {Block_repr.hash; contents; metadata} in
      Block_store.store_block chain_store.block_store block
      >>= fun () ->
      Lwt_watcher.notify chain_store.block_watcher block ;
      Lwt_watcher.notify
        chain_store.global_store.global_block_watcher
        (chain_store, block) ;
      (* TODO? Should we specifically store the forking block ? *)
      Event.(emit store_block) (hash, block_header.shell.level)
      >>= fun () -> return_some block

  let context_exn chain_store block =
    let context_index = chain_store.global_store.context_index in
    Context.checkout_exn context_index (Block_repr.context block)

  let context_opt chain_store block =
    let context_index = chain_store.global_store.context_index in
    Context.checkout context_index (Block_repr.context block)

  let context chain_store block =
    context_opt chain_store block
    >>= function
    | Some context ->
        return context
    | None ->
        fail
          (Cannot_checkout_context
             (Block_repr.hash block, Block_repr.context block))

  let context_exists chain_store block =
    let context_index = chain_store.global_store.context_index in
    Context.exists context_index (Block_repr.context block)

  let testchain_status chain_store block =
    (* TODO wrap error *)
    context_exn chain_store block
    >>= fun context ->
    Context.get_test_chain context
    >>= fun status ->
    match status with
    | Running {genesis; _} ->
        Lwt.return (status, Some genesis)
    | Forking _ ->
        Lwt.return (status, Some (Block_repr.hash block))
    | Not_running ->
        Lwt.return (status, None)

  let protocol_hash chain_store block =
    Shared.use chain_store.chain_state (fun {protocol_levels; _} ->
        Stored_data.read protocol_levels
        >>= fun protocol_levels ->
        let open Protocol_levels in
        let proto_level = Block_repr.proto_level block in
        match find_opt proto_level protocol_levels with
        | Some {protocol; _} ->
            return protocol
        | None ->
            fail (Cannot_find_protocol proto_level))

  let protocol_hash_exn chain_store block =
    protocol_hash chain_store block
    >>= function Ok ph -> Lwt.return ph | Error _ -> Lwt.fail Not_found

  let compute_locator chain_store ?(size = 200) head seed =
    caboose chain_store
    >>= fun (caboose, _caboose_level) ->
    Block_locator.compute
      ~get_predecessor:(fun h n ->
        read_ancestor_hash_opt chain_store h ~distance:n)
      ~caboose
      ~size
      head.Block_repr.hash
      head.Block_repr.contents.header
      seed

  (** Operations on invalid blocks *)

  let read_invalid_block_opt {chain_state; _} hash =
    Shared.use chain_state (fun {invalid_blocks; _} ->
        Stored_data.read invalid_blocks
        >>= fun invalid_blocks ->
        Lwt.return (Block_hash.Map.find_opt hash invalid_blocks))

  let read_invalid_blocks {chain_state; _} =
    Shared.use chain_state (fun {invalid_blocks; _} ->
        Stored_data.read invalid_blocks)

  let mark_invalid chain_store hash ~level errors =
    if is_genesis chain_store hash then fail Invalid_genesis_marking
    else
      Shared.use chain_store.chain_state (fun {invalid_blocks; _} ->
          Stored_data.update_with invalid_blocks (fun invalid_blocks ->
              Lwt.return
                (Block_hash.Map.add hash {level; errors} invalid_blocks)))
      >>= fun () -> return_unit

  let unmark_invalid {chain_state; _} hash =
    Shared.use chain_state (fun {invalid_blocks; _} ->
        Stored_data.update_with invalid_blocks (fun invalid_blocks ->
            Lwt.return (Block_hash.Map.remove hash invalid_blocks)))

  (** Accessors *)

  let hash blk = Block_repr.hash blk

  let header blk = Block_repr.header blk

  let operations blk = Block_repr.operations blk

  let shell_header blk = Block_repr.shell_header blk

  let level blk = Block_repr.level blk

  let proto_level blk = Block_repr.proto_level blk

  let predecessor blk = Block_repr.predecessor blk

  let timestamp blk = Block_repr.timestamp blk

  let operations_hash blk = Block_repr.operations_hash blk

  let validation_passes blk = Block_repr.validation_passes blk

  let fitness blk = Block_repr.fitness blk

  let context_hash blk = Block_repr.context blk

  let protocol_data blk = Block_repr.protocol_data blk

  (** Metadata accessors *)

  let message metadata = Block_repr.message metadata

  let max_operations_ttl metadata = Block_repr.max_operations_ttl metadata

  let last_allowed_fork_level metadata =
    Block_repr.last_allowed_fork_level metadata

  let block_metadata metadata = Block_repr.block_metadata metadata

  let operations_metadata metadata = Block_repr.operations_metadata metadata

  let compute_operation_path hashes =
    let list_hashes = List.map Operation_list_hash.compute hashes in
    Operation_list_list_hash.compute_path list_hashes

  let operations_path block i =
    (* TODO proper error *)
    if i < 0 || validation_passes block <= i then invalid_arg "operations" ;
    let ops = operations block in
    let hashes = List.(map (map Operation.hash)) ops in
    let path = compute_operation_path hashes in
    (List.nth ops i, path i)

  let operations_hashes_path block i =
    (* TODO proper error *)
    if i < 0 || (header block).shell.validation_passes <= i then
      invalid_arg "operations_hashes" ;
    let opss = operations block in
    let hashes = List.(map (map Operation.hash)) opss in
    let path = compute_operation_path hashes in
    (List.nth hashes i, path i)

  let all_operation_hashes block =
    List.(map (map Operation.hash)) (operations block)
end

module Chain_traversal = struct
  let path chain_store ~from_block ~to_block =
    if not Compare.Int32.(Block.level from_block <= Block.level to_block) then
      invalid_arg "Chain_traversal.path" ;
    let rec loop acc current =
      if Block.equal from_block current then Lwt.return_some acc
      else
        Block.read_predecessor_opt chain_store current
        >>= function
        | Some pred -> loop (current :: acc) pred | None -> Lwt.return_none
    in
    loop [] to_block

  let common_ancestor chain_store b1 b2 =
    let rec loop b1 b2 =
      if Block.equal b1 b2 then Lwt.return_some b1
      else if Compare.Int32.(Block.level b1 <= Block.level b2) then
        Block.read_predecessor_opt chain_store b2
        >>= function None -> Lwt.return_none | Some b2 -> loop b1 b2
      else
        Block.read_predecessor_opt chain_store b1
        >>= function None -> Lwt.return_none | Some b1 -> loop b1 b2
    in
    loop b1 b2

  let new_blocks chain_store ~from_block ~to_block =
    common_ancestor chain_store from_block to_block
    >>= function
    | None ->
        assert false
    | Some ancestor -> (
        path chain_store ~from_block:ancestor ~to_block
        >>= function
        | None ->
            Lwt.return (ancestor, [])
        | Some path ->
            Lwt.return (ancestor, path) )

  (* TODO improve the computation by caching previous results. *)
  let live_blocks chain_store block n =
    let rec loop bacc oacc block_head n =
      let hashes = Block.all_operation_hashes block_head in
      let oacc =
        List.fold_left
          (List.fold_left (fun oacc op -> Operation_hash.Set.add op oacc))
          oacc
          hashes
      in
      let bacc = Block_hash.Set.add (Block.hash block_head) bacc in
      if n = 0 then Lwt.return (bacc, oacc)
      else
        Block.read_predecessor_opt chain_store block_head
        >>= function
        | None ->
            Lwt.return (bacc, oacc)
        | Some predecessor ->
            loop bacc oacc predecessor (pred n)
    in
    loop Block_hash.Set.empty Operation_hash.Set.empty block n
end

module Chain = struct
  type nonrec chain_store = chain_store

  type t = chain_store

  type nonrec testchain = testchain

  let global_store {global_store; _} = global_store

  let chain_id chain_store = chain_store.chain_id

  let chain_dir chain_store = chain_store.chain_dir

  let history_mode chain_store = history_mode chain_store

  let set_history_mode chain_store history_mode =
    let chain_config = {chain_store.chain_config with history_mode} in
    Chain_config.write ~chain_dir:chain_store.chain_dir chain_config

  let genesis chain_store = genesis chain_store

  let genesis_block chain_store =
    Shared.use chain_store.chain_state (fun {genesis; _} ->
        Stored_data.read genesis)

  let expiration chain_store = chain_store.chain_config.expiration

  let checkpoint chain_store = checkpoint chain_store

  let savepoint chain_store = savepoint chain_store

  let set_savepoint chain_store new_savepoint =
    Shared.update_with chain_store.chain_state (fun chain_state ->
        (* FIXME: potential data-race if called during a merge *)
        Stored_data.write chain_state.savepoint_data new_savepoint
        >>= fun () ->
        Event.(emit set_savepoint) new_savepoint
        >>= fun () ->
        return (Some {chain_state with savepoint = new_savepoint}, ()))

  let caboose chain_store = caboose chain_store

  let set_caboose chain_store new_caboose =
    Shared.update_with chain_store.chain_state (fun chain_state ->
        (* FIXME: potential data-race if called during a merge *)
        Stored_data.write chain_state.caboose_data new_caboose
        >>= fun () ->
        Event.(emit set_caboose) new_caboose
        >>= fun () -> return (Some {chain_state with caboose = new_caboose}, ()))

  let current_head chain_store = current_head chain_store

  let current_head_metadata chain_store =
    Shared.use chain_store.chain_state (fun {current_head_metadata; _} ->
        Lwt.return current_head_metadata)

  let mempool chain_store =
    Shared.use chain_store.chain_state (fun {mempool; _} -> Lwt.return mempool)

  let set_mempool chain_store ~head mempool =
    Shared.update_with
      chain_store.chain_state
      (fun ({current_head; _} as chain_state) ->
        Stored_data.read current_head
        >>= fun current_head ->
        if Block_hash.equal head (Block.hash current_head) then
          return (Some {chain_state with mempool}, ())
        else return (None, ()))

  let live_blocks chain_store =
    Shared.use
      chain_store.chain_state
      (fun {live_blocks; live_operations; _} ->
        Lwt.return (live_blocks, live_operations))

  let compute_live_blocks chain_store ~block =
    Shared.use
      chain_store.chain_state
      (fun {current_head; live_blocks; live_operations; _} ->
        Stored_data.read current_head
        >>= fun current_head ->
        if Block.equal current_head block then
          return (live_blocks, live_operations)
        else
          Block.get_block_metadata chain_store block
          >>=? fun metadata ->
          let max_operations_ttl = Block.max_operations_ttl metadata in
          Chain_traversal.live_blocks chain_store block max_operations_ttl
          >>= fun live -> return live)

  let is_ancestor chain_store ~head:(hash, lvl) ~ancestor:(hash', lvl') =
    if Compare.Int32.(lvl' > lvl) then Lwt.return_false
    else if Compare.Int32.(lvl = lvl') then
      Lwt.return (Block_hash.equal hash hash')
    else
      Block.read_ancestor_hash_opt
        chain_store
        hash
        ~distance:Int32.(to_int (sub lvl lvl'))
      >>= function
      | None ->
          Lwt.return_false
      | Some hash_found ->
          Lwt.return (Block_hash.equal hash' hash_found)

  let is_in_chain chain_store (hash, level) =
    current_head chain_store
    >>= fun current_head ->
    is_ancestor
      chain_store
      ~head:Block.(hash current_head, level current_head)
      ~ancestor:(hash, level)

  let update_alternate_heads chain_store ?filter_below alternate_heads
      ~prev_head ~new_head =
    ( match filter_below with
    | Some threshold ->
        Stored_data.update_with alternate_heads (fun alternate_heads ->
            Lwt.return
              (Block_hash.Map.filter
                 (fun _ level -> Compare.Int32.(level > threshold))
                 alternate_heads))
    | None ->
        Lwt.return_unit )
    >>= fun () ->
    let new_head_pred = Block.predecessor new_head in
    let (prev_head_hash, _prev_head_level) = Block.descriptor prev_head in
    is_ancestor
      chain_store
      ~head:Block.(hash new_head, level new_head)
      ~ancestor:Block.(hash prev_head, level prev_head)
    >>= fun is_ancestor ->
    if is_ancestor then Lwt.return_unit
    else
      Stored_data.update_with alternate_heads (fun alternate_heads ->
          (* If the previous head is not an ancestor, the new head is
             necessarly an alternate head, store the previous head as
             an alternate head *)
          let prev_head_level = Block.level prev_head in
          let alternate_heads =
            Block_hash.Map.add prev_head_hash prev_head_level alternate_heads
          in
          match Block_hash.Map.find_opt new_head_pred alternate_heads with
          | None ->
              (* It's an orphan new head: mark the previous head as
                 alternate head *)
              Lwt.return alternate_heads
          | Some _prev_branch_level ->
              (* It's a known branch, update the alternate heads *)
              Lwt.return (Block_hash.Map.remove new_head_pred alternate_heads))

  let compute_new_savepoint locked_chain_store chain_state history_mode
      ~min_level_to_preserve ~new_head =
    match history_mode with
    | History_mode.Archive ->
        (* new_savepoint = savepoint = genesis *)
        return chain_state.savepoint
    | Full {offset} | Rolling {offset} -> (
        Block.locked_read_block_by_level
          locked_chain_store
          new_head
          min_level_to_preserve
        >>=? fun min_block_to_preserve ->
        (* New savepoint = min min_level_to_preserve (min new lowest cemented block) *)
        let table =
          Cemented_block_store.cemented_blocks_files
            (Block_store.cemented_block_store locked_chain_store.block_store)
        in
        if
          Compare.Int32.(
            snd chain_state.savepoint >= Block.level min_block_to_preserve)
        then return chain_state.savepoint
        else
          let table_len = Array.length table in
          (* If the offset is 0, the minimum block to preserve will be
             the savepoint. *)
          if offset = 0 then return (Block.descriptor min_block_to_preserve)
          else if
            (* If the number of cemented cycles is not yet the offset,
               then the savepoint is unchanged. *)
            table_len < offset
          then return chain_state.savepoint
          else
            (* Else we shift the savepoint by one cycle *)
            let shifted_savepoint_level =
              (* new lowest cemented block  *)
              Int32.succ
                table.(table_len - offset).Cemented_block_store.end_level
            in
            (* If the savepoint is still higher than the shifted
               savepoint, preserve the savepoint *)
            if
              Compare.Int32.(
                snd chain_state.savepoint >= shifted_savepoint_level)
            then return chain_state.savepoint
            else if
              (* If the new savepoint is still higher than the min block
                 to preserve, we choose the min block to preserve. *)
              Compare.Int32.(
                shifted_savepoint_level >= Block.level min_block_to_preserve)
            then return (Block.descriptor min_block_to_preserve)
            else
              (* Else the new savepoint is the one-cycle shifted
                 savepoint. *)
              Block.locked_read_block_by_level_opt
                locked_chain_store
                new_head
                shifted_savepoint_level
              >>= function
              | None ->
                  fail (Cannot_retrieve_savepoint shifted_savepoint_level)
              | Some savepoint ->
                  return (Block.descriptor savepoint) )

  let compute_interval_to_cement locked_chain_store chain_state
      ~prev_head_metadata ~new_head_metadata ~new_head =
    let prev_last_lafl = Block.last_allowed_fork_level prev_head_metadata in
    ( match get_highest_cemented_level locked_chain_store with
    | None ->
        (* First merge: first cementing, rolling 0 or recent snapshot
           import *)
        Stored_data.read chain_state.genesis
        >>= fun genesis ->
        let is_first_cycle =
          Compare.Int32.(prev_last_lafl = Block.level genesis)
        in
        ( if is_first_cycle then return genesis
        else
          Block.locked_read_block_by_level
            locked_chain_store
            new_head
            (Int32.succ prev_last_lafl) )
        >>=? fun block -> return (Block.descriptor block)
    | Some level ->
        fail_unless
          Compare.Int32.(
            Block.last_allowed_fork_level prev_head_metadata = level)
          (Inconsistent_cemented_store
             (Inconsistent_highest_cemented_level
                {
                  highest_cemented_level = level;
                  head_last_allowed_fork_level =
                    Block.last_allowed_fork_level prev_head_metadata;
                }))
        >>=? fun () ->
        (* Retrieve (pred_head.last_allowed_fork_level + 1) *)
        Block.locked_read_block_by_level
          locked_chain_store
          new_head
          (Int32.succ level)
        >>=? fun from_block -> return (Block.descriptor from_block) )
    >>=? fun from_block ->
    (let new_head_lafl = Block.last_allowed_fork_level new_head_metadata in
     let distance =
       Int32.(to_int (sub (Block.level new_head) new_head_lafl))
     in
     Block.read_ancestor_hash
       locked_chain_store
       ~distance
       (Block.hash new_head)
     >>=? function
     | None ->
         fail Missing_last_allowed_fork_level_block
     | Some hash ->
         return (hash, new_head_lafl))
    >>=? fun to_block -> return (from_block, to_block)

  let compute_blocks_to_preserve locked_chain_store chain_state ~to_block =
    (* Try to preserve [max_op_ttl] predecessors from the upper-bound
       (along with their metadata) so a snapshot may always be
       created. *)
    Block.read_block locked_chain_store (fst to_block)
    >>=? fun to_block_b ->
    Block.get_block_metadata_opt locked_chain_store to_block_b
    >>= function
    | Some metadata ->
        let nb_blocks_to_preserve = Block.max_operations_ttl metadata in
        let min_level_to_preserve =
          Compare.Int32.max
            (snd chain_state.caboose)
            Int32.(sub (snd to_block) (of_int nb_blocks_to_preserve))
        in
        (* + 1, to_block + max_op_ttl(checkpoint) *)
        return (succ nb_blocks_to_preserve, min_level_to_preserve)
    | None ->
        (* If no metadata is found (e.g. may happen after a snapshot
           import), keep the blocks until the caboose *)
        let min_level_to_preserve = snd chain_state.caboose in
        (* + 1, it's a size *)
        let nb_blocks_to_preserve =
          Int32.(to_int (succ (sub (snd to_block) min_level_to_preserve)))
        in
        return (nb_blocks_to_preserve, min_level_to_preserve)

  let compute_new_caboose locked_chain_store chain_state history_mode
      ~savepoint ~min_level_to_preserve ~from_block ~new_head =
    match history_mode with
    | History_mode.Archive | Full _ ->
        (* caboose = genesis *)
        return chain_state.caboose
    | Rolling {offset} ->
        (* If caboose equals min block to preserve, we leave it
           unchanged. Note: Caboose cannot normally be >
           min_level_to_preserve. *)
        if Compare.Int32.(snd chain_state.caboose >= min_level_to_preserve)
        then return chain_state.caboose
        else if
          (* If the min level to preserve is lower than the savepoint
             or if we don't keep any extra cycles, the genesis is the
             min block to preserve. *)
          Compare.Int32.(min_level_to_preserve < snd savepoint) || offset = 0
        then
          Block.locked_read_block_by_level
            locked_chain_store
            new_head
            min_level_to_preserve
          >>=? fun min_block_to_preserve ->
          return (Block.descriptor min_block_to_preserve)
        else
          (* Else genesis = new savepoint except for the first
             cemented cycle which might be partial when cementing
             after a snapshot import. *)
          let table =
            Cemented_block_store.cemented_blocks_files
              (Block_store.cemented_block_store locked_chain_store.block_store)
          in
          let table_len = Array.length table in
          if table_len = 0 then return from_block else return savepoint

  (* Hypothesis: new_head.last_allowed_fork_level > prev_head.last_allowed_fork_level *)
  (* TODO: add a stack of merge callbacks in case the lock is taken *)
  let trigger_merge chain_store chain_state ~prev_head_metadata
      ~new_head_metadata ~new_head =
    let history_mode = history_mode chain_store in
    (* Determine the interval of blocks to cement
       [from_block ; to_block] based on the last allowed fork level of
       the new_head *)
    compute_interval_to_cement
      chain_store
      chain_state
      ~prev_head_metadata
      ~new_head_metadata
      ~new_head
    >>=? fun (from_block, to_block) ->
    compute_blocks_to_preserve chain_store chain_state ~to_block
    >>=? fun (nb_blocks_to_preserve, min_level_to_preserve) ->
    (* Determine the new values for: checkpoint, savepoint, caboose *)
    let checkpoint =
      (* Only update the checkpoint when it falls behind the upper
         bound to cement *)
      if Compare.Int32.(snd chain_state.checkpoint < snd to_block) then
        to_block
      else chain_state.checkpoint
    in
    (* FIXME: add a test to ensure that the new checkpoint is related
       to the lafl ? *)
    compute_new_savepoint
      chain_store
      chain_state
      history_mode
      ~min_level_to_preserve
      ~new_head
    >>=? fun savepoint ->
    compute_new_caboose
      chain_store
      chain_state
      history_mode
      ~savepoint
      ~min_level_to_preserve
      ~from_block
      ~new_head
    >>=? fun caboose ->
    let finalizer () =
      (* Update the *on-disk* stored values when the merge terminates. *)
      Stored_data.write chain_state.checkpoint_data checkpoint
      >>= fun () ->
      Stored_data.write chain_state.savepoint_data savepoint
      >>= fun () ->
      Stored_data.write chain_state.caboose_data caboose
      >>= fun () ->
      (* Don't forget to unlock after the merge *)
      may_unlock chain_store.lockfile
    in
    (* Lock the directories while the reordering store is happening:
       this prevents snapshots from accessing the store while a merge
       is occuring. *)
    lock_for_write chain_store.lockfile
    >>= fun () ->
    (* The main part of this function is asynchronous so it returns
       immediately *)
    Block_store.merge_stores
      chain_store.block_store
      ~finalizer
      ~nb_blocks_to_preserve
      ~history_mode
      ~from_block
      ~to_block
      ()
    >>= fun () ->
    (* The returned updated values are to be set in the memory
       state. *)
    return (checkpoint, savepoint, caboose, to_block)

  let set_head chain_store new_head =
    Shared.update_with chain_store.chain_state (fun chain_state ->
        let { current_head;
              savepoint;
              checkpoint;
              caboose;
              current_head_metadata;
              _ } =
          chain_state
        in
        Stored_data.read current_head
        >>= fun prev_head ->
        Block.get_block_metadata chain_store new_head
        >>=? fun new_head_metadata ->
        if Block.equal prev_head new_head then
          (* Nothing to do *)
          return (None, prev_head)
        else
          (* First, check that we do not try to set a head below the
             last allowed fork level of the previous head *)
          fail_unless
            Compare.Int32.(
              Block.level new_head
              > Block.last_allowed_fork_level current_head_metadata)
            (Invalid_head_switch
               {
                 minimum_allowed_level =
                   Int32.succ
                     (Block.last_allowed_fork_level current_head_metadata);
                 given_head = Block.descriptor new_head;
               })
          >>=? fun () ->
          (* This is an acceptable head *)
          let new_head_lafl =
            Block.last_allowed_fork_level new_head_metadata
          in
          let prev_head_lafl =
            Block.last_allowed_fork_level current_head_metadata
          in
          (* Should we trigger a merge? i.e. has the last allowed fork
             level changed and do we have enough blocks? *)
          let has_lafl_changed =
            Compare.Int32.(new_head_lafl > prev_head_lafl)
          in
          let has_necessary_blocks =
            Compare.Int32.(snd caboose <= prev_head_lafl)
          in
          ( if not (has_lafl_changed && has_necessary_blocks) then
            return (checkpoint, savepoint, caboose, None)
          else
            (* Make sure that the previous merge is completed before
               starting a new merge *)
            Block_store.await_merging chain_store.block_store
            >>=? fun () ->
            let is_already_cemented =
              match get_highest_cemented_level chain_store with
              | Some highest_cemented_level ->
                  Compare.Int32.(highest_cemented_level >= new_head_lafl)
              | None ->
                  false
            in
            (* It might already be cemented when the store is imported
               from a snapshot *)
            if is_already_cemented then
              return (checkpoint, savepoint, caboose, None)
            else
              trigger_merge
                chain_store
                chain_state
                ~prev_head_metadata:current_head_metadata
                ~new_head
                ~new_head_metadata
              >>=? fun (checkpoint, savepoint, caboose, to_block) ->
              (* [to_block] might be different from checkpoint
                 (e.g. checkpoint in the future) *)
              return (checkpoint, savepoint, caboose, Some (snd to_block)) )
          >>=? fun (checkpoint, savepoint, caboose, limit_opt) ->
          (* Update and filter out alternate heads that are below the
             new highest cemented block *)
          update_alternate_heads
            chain_store
            ?filter_below:limit_opt
            chain_state.alternate_heads
            ~prev_head
            ~new_head
          >>= fun () ->
          (* Update current_head and live_{blocks,ops} *)
          Stored_data.write current_head new_head
          >>= fun () ->
          let new_head_max_op_ttl =
            Block.max_operations_ttl new_head_metadata
          in
          (* Updating live blocks *)
          Chain_traversal.live_blocks chain_store new_head new_head_max_op_ttl
          >>= fun (live_blocks, live_operations) ->
          let new_chain_state =
            {
              chain_state with
              current_head_metadata = new_head_metadata;
              live_blocks;
              live_operations;
              checkpoint;
              savepoint;
              caboose;
            }
          in
          Event.(emit set_head) (Block.hash new_head, Block.level new_head)
          >>= fun () -> return (Some new_chain_state, prev_head))

  let known_heads chain_store =
    Shared.use
      chain_store.chain_state
      (fun {current_head; alternate_heads; _} ->
        Stored_data.read current_head
        >>= fun head ->
        Stored_data.read alternate_heads
        >>= fun alternate_heads ->
        Lwt.return
          (Block_hash.Map.add
             (Block.hash head)
             (Block.level head)
             alternate_heads))

  let locked_is_valid_for_checkpoint chain_store chain_state
      (given_checkpoint_hash, given_checkpoint_level) =
    Stored_data.read chain_state.current_head
    >>= fun current_head ->
    Block.get_block_metadata chain_store current_head
    >>=? fun current_head_metadata ->
    let head_lafl = Block.last_allowed_fork_level current_head_metadata in
    if Compare.Int32.(given_checkpoint_level <= head_lafl) then
      (* Cannot set a checkpoint before the current head's last
         allowed fork level *)
      return_false
    else
      Block.is_known_valid chain_store given_checkpoint_hash
      >>= function
      | false ->
          (* Given checkpoint is in the future: valid *)
          return_true
      | true -> (
          read_ancestor_hash
            chain_store
            ~distance:Int32.(to_int (sub given_checkpoint_level head_lafl))
            given_checkpoint_hash
          >>=? function
          | None ->
              (* The last allowed fork level is unknown, thus different from current head's lafl *)
              return_false
          | Some ancestor -> (
              read_ancestor_hash
                chain_store
                ~distance:
                  Int32.(to_int (sub (Block.level current_head) head_lafl))
                (Block.hash current_head)
              >>=? function
              | None ->
                  fail Missing_last_allowed_fork_level_block
              | Some lafl_hash ->
                  return (Block_hash.equal lafl_hash ancestor) ) )

  let is_valid_for_checkpoint chain_store given_checkpoint =
    Shared.use chain_store.chain_state (fun chain_state ->
        Block.locked_is_known_invalid chain_state (fst given_checkpoint)
        >>= function
        | true ->
            return_false
        | false ->
            locked_is_valid_for_checkpoint
              chain_store
              chain_state
              given_checkpoint)

  let set_checkpoint chain_store given_checkpoint =
    Shared.update_with chain_store.chain_state (fun chain_state ->
        Block.locked_is_known_invalid chain_state (fst given_checkpoint)
        >>= function
        | true ->
            fail (Cannot_set_checkpoint given_checkpoint)
        | false -> (
            locked_is_valid_for_checkpoint
              chain_store
              chain_state
              given_checkpoint
            >>=? function
            | false ->
                fail (Cannot_set_checkpoint given_checkpoint)
            | true ->
                Stored_data.write chain_state.checkpoint_data given_checkpoint
                >>= fun () ->
                Event.(emit set_checkpoint) given_checkpoint
                >>= fun () ->
                return
                  (Some {chain_state with checkpoint = given_checkpoint}, ()) ))

  let is_acceptable_block chain_store block_descr =
    Shared.use chain_store.chain_state (fun chain_state ->
        locked_is_acceptable_block chain_state block_descr)

  let best_known_head_for_checkpoint chain_store ~checkpoint =
    let (_, checkpoint_level) = checkpoint in
    current_head chain_store
    >>= fun current_head ->
    is_valid_for_checkpoint
      chain_store
      (Block.hash current_head, Block.level current_head)
    >>=? fun valid ->
    if valid then return current_head
    else
      let find_valid_predecessor hash =
        Block.read_block chain_store hash
        >>=? fun block ->
        if Compare.Int32.(Block_repr.level block < checkpoint_level) then
          return block
        else
          (* Read the checkpoint's predecessor *)
          Block.read_block
            chain_store
            hash
            ~distance:
              ( 1
              + ( Int32.to_int
                @@ Int32.sub (Block_repr.level block) checkpoint_level ) )
      in
      known_heads chain_store
      >>= fun heads ->
      genesis_block chain_store
      >>= fun genesis ->
      let best = genesis in
      Block_hash.Map.fold
        (fun hash _level best ->
          let valid_predecessor_t = find_valid_predecessor hash in
          best
          >>=? fun best ->
          valid_predecessor_t
          >>=? fun pred ->
          if Fitness.(Block.fitness pred > Block.fitness best) then return pred
          else return best)
        heads
        (return best)

  (* Create / Load / Close *)

  let create_genesis_block ~genesis context =
    let shell : Block_header.shell_header =
      {
        level = 0l;
        proto_level = 0;
        predecessor = genesis.Genesis.block;
        (* genesis' predecessor is genesis *)
        timestamp = genesis.Genesis.time;
        fitness = [];
        validation_passes = 0;
        operations_hash = Operation_list_list_hash.empty;
        context;
      }
    in
    let header : Block_header.t = {shell; protocol_data = Bytes.create 0} in
    let contents = {Block_repr.header; operations = []} in
    let metadata =
      Some
        {
          Block_repr.message = Some "Genesis";
          max_operations_ttl = 0;
          last_allowed_fork_level = 0l;
          block_metadata = Bytes.create 0;
          operations_metadata = [];
        }
    in
    {Block_repr.hash = genesis.block; contents; metadata}

  let create_testchain_genesis_block ~genesis_hash ~genesis_header =
    let header = genesis_header in
    let contents = {Block_repr.header; operations = []} in
    let metadata =
      Some
        {
          Block_repr.message = Some "Genesis";
          max_operations_ttl = 0;
          last_allowed_fork_level = genesis_header.shell.level;
          block_metadata = Bytes.create 0;
          operations_metadata = [];
        }
    in
    {Block_repr.hash = genesis_hash; contents; metadata}

  let create_chain_state ~chain_dir ~genesis_block ~genesis_protocol
      ~genesis_commit_info =
    let genesis_proto_level = Block_repr.proto_level genesis_block in
    let genesis_level = Block_repr.level genesis_block in
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.protocol_levels)
      Protocol_levels.encoding
      ~initial_data:
        Protocol_levels.(
          add
            genesis_proto_level
            {
              block = Block.descriptor genesis_block;
              protocol = genesis_protocol;
              commit_info = genesis_commit_info;
            }
            empty)
    >>=? fun protocol_levels ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.genesis)
      Block_repr.encoding
      ~initial_data:genesis_block
    >>=? fun genesis ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.current_head)
      Block_repr.encoding
      ~initial_data:genesis_block
    >>=? fun current_head ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.alternate_heads)
      (Block_hash.Map.encoding Data_encoding.int32)
      ~initial_data:Block_hash.Map.empty
    >>=? fun alternate_heads ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.checkpoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      ~initial_data:(genesis_block.hash, genesis_level)
    >>=? fun checkpoint_data ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.savepoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      ~initial_data:(genesis_block.hash, genesis_level)
    >>=? fun savepoint_data ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.caboose)
      Data_encoding.(tup2 Block_hash.encoding int32)
      ~initial_data:(genesis_block.hash, genesis_level)
    >>=? fun caboose_data ->
    Stored_data.init
      ~file:Naming.(chain_dir // Naming.Chain_data.invalid_blocks)
      (Block_hash.Map.encoding invalid_block_encoding)
      ~initial_data:Block_hash.Map.empty
    >>=? fun invalid_blocks ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.forked_chains)
      (Chain_id.Map.encoding Block_hash.encoding)
      ~initial_data:Chain_id.Map.empty
    >>=? fun forked_chains ->
    Stored_data.read checkpoint_data
    >>= fun checkpoint ->
    Stored_data.read savepoint_data
    >>= fun savepoint ->
    Stored_data.read caboose_data
    >>= fun caboose ->
    let current_head_metadata =
      Option.unopt_assert ~loc:__POS__ (Block_repr.metadata genesis_block)
    in
    let active_testchain = None in
    let mempool = Mempool.empty in
    let live_blocks = Block_hash.Set.empty in
    let live_operations = Operation_hash.Set.empty in
    return
      {
        genesis;
        current_head;
        alternate_heads;
        checkpoint;
        checkpoint_data;
        savepoint;
        savepoint_data;
        caboose;
        caboose_data;
        protocol_levels;
        invalid_blocks;
        forked_chains;
        current_head_metadata;
        active_testchain;
        mempool;
        live_blocks;
        live_operations;
      }

  (* FIXME: add integrity check to ensure that files are present? *)
  (* Files are expected to be present *)
  let load_chain_state ~chain_dir =
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.protocol_levels)
      Protocol_levels.encoding
    >>=? fun protocol_levels ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.genesis)
      Block_repr.encoding
    >>=? fun genesis ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.current_head)
      Block_repr.encoding
    >>=? fun current_head ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.alternate_heads)
      (Block_hash.Map.encoding Data_encoding.int32)
    >>=? fun alternate_heads ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.checkpoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
    >>=? fun checkpoint_data ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.savepoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
    >>=? fun savepoint_data ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.caboose)
      Data_encoding.(tup2 Block_hash.encoding int32)
    >>=? fun caboose_data ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.invalid_blocks)
      (Block_hash.Map.encoding invalid_block_encoding)
    >>=? fun invalid_blocks ->
    Stored_data.load
      ~file:Naming.(chain_dir // Naming.Chain_data.forked_chains)
      (Chain_id.Map.encoding Block_hash.encoding)
    >>=? fun forked_chains ->
    Stored_data.read checkpoint_data
    >>= fun checkpoint ->
    Stored_data.read savepoint_data
    >>= fun savepoint ->
    Stored_data.read caboose_data
    >>= fun caboose ->
    Stored_data.read current_head
    >>= fun current_head_block ->
    let current_head_metadata =
      Option.unopt_assert ~loc:__POS__ (Block_repr.metadata current_head_block)
    in
    let active_testchain = None in
    let mempool = Mempool.empty in
    let live_blocks = Block_hash.Set.empty in
    let live_operations = Operation_hash.Set.empty in
    return
      {
        genesis;
        current_head;
        alternate_heads;
        checkpoint;
        checkpoint_data;
        savepoint;
        savepoint_data;
        caboose;
        caboose_data;
        protocol_levels;
        invalid_blocks;
        forked_chains;
        current_head_metadata;
        active_testchain;
        mempool;
        live_blocks;
        live_operations;
      }

  let get_commit_info index header =
    protect
      ~on_error:(fun err ->
        Format.kasprintf
          (fun e -> fail (Missing_commit_info e))
          "%a"
          Error_monad.pp_print_error
          err)
      (fun () ->
        Context.retrieve_commit_info index header
        >>=? fun ( _protocol_hash,
                   author,
                   message,
                   _timestamp,
                   test_chain_status,
                   data_merkle_root,
                   parents_contexts ) ->
        return
          {
            Protocol_levels.author;
            message;
            test_chain_status;
            data_merkle_root;
            parents_contexts;
          })

  let get_commit_info_opt index header =
    get_commit_info index header
    >>= function Ok v -> Lwt.return_some v | Error _ -> Lwt.return_none

  let create_chain_store global_store ~chain_dir ~chain_id ?(expiration = None)
      ?genesis_block ~genesis ~genesis_context history_mode =
    (* Chain directory *)
    let genesis_block =
      match genesis_block with
      | None ->
          create_genesis_block ~genesis genesis_context
      | Some genesis_block ->
          genesis_block
    in
    (* Block_store.create also stores genesis *)
    Block_store.create ~chain_dir ~genesis_block
    >>=? fun block_store ->
    let chain_config = {Chain_config.history_mode; genesis; expiration} in
    Chain_config.write ~chain_dir chain_config
    >>=? fun () ->
    get_commit_info_opt global_store.context_index (Block.header genesis_block)
    >>= fun genesis_commit_info ->
    create_chain_state
      ~chain_dir
      ~genesis_block
      ~genesis_protocol:genesis.Genesis.protocol
      ~genesis_commit_info
    >>=? fun chain_state ->
    let chain_state = Shared.create chain_state in
    let block_watcher = Lwt_watcher.create_input () in
    let block_rpc_directories = Protocol_hash.Table.create 7 in
    create_lockfile ~chain_dir
    >>= fun lockfile ->
    let chain_store : chain_store =
      {
        global_store;
        chain_id;
        chain_dir;
        chain_config;
        chain_state;
        block_store;
        block_watcher;
        block_rpc_directories;
        lockfile;
      }
    in
    (* [set_head] also updates the chain state *)
    set_head chain_store genesis_block >>= fun _prev_head -> return chain_store

  let load_chain_store global_store ~chain_dir ~chain_id ~readonly =
    Chain_config.load ~chain_dir
    >>=? fun chain_config ->
    load_chain_state ~chain_dir
    >>=? fun chain_state ->
    Stored_data.read chain_state.genesis
    >>= fun genesis_block ->
    Block_store.load ~chain_dir ~genesis_block ~readonly
    >>=? fun block_store ->
    let chain_state = Shared.create chain_state in
    let block_watcher = Lwt_watcher.create_input () in
    let block_rpc_directories = Protocol_hash.Table.create 7 in
    create_lockfile ~chain_dir
    >>= fun lockfile ->
    let chain_store =
      {
        global_store;
        chain_id;
        chain_dir;
        chain_config;
        (* let the state handle the test chain initialization *)
        block_store;
        chain_state;
        block_watcher;
        block_rpc_directories;
        lockfile;
      }
    in
    (* Also initalize the live blocks *)
    current_head chain_store
    >>= fun head ->
    Block.get_block_metadata_opt chain_store head
    >>= function
    | None ->
        fail Inconsistent_chain_store
    | Some metadata ->
        let max_op_ttl = Block.max_operations_ttl metadata in
        Chain_traversal.live_blocks chain_store head max_op_ttl
        >>= fun (live_blocks, live_operations) ->
        Shared.update_with chain_state (fun chain_state ->
            return
              ( Some {chain_state with live_blocks; live_operations},
                chain_store ))

  (* Recursively closes all test chain stores *)
  let close_chain_store chain_store =
    Lwt_watcher.shutdown_input chain_store.block_watcher ;
    let rec loop = function
      | {block_store; lockfile; chain_state; _} ->
          Shared.use chain_state (fun {active_testchain; _} ->
              Block_store.close block_store
              >>= fun () ->
              ( match active_testchain with
              | Some {testchain_store; _} ->
                  loop testchain_store
              | None ->
                  Lwt.return_unit )
              >>= fun () ->
              may_unlock chain_store.lockfile
              >>= fun () ->
              Lwt_utils_unix.safe_close lockfile >>= fun () -> Lwt.return_unit)
    in
    loop chain_store

  (* Test chain *)

  let testchain chain_store =
    Shared.use chain_store.chain_state (fun {active_testchain; _} ->
        Lwt.return active_testchain)

  let testchain_forked_block {forked_block; _} = forked_block

  let testchain_store {testchain_store; _} = testchain_store

  let locked_load_testchain chain_store ~chain_id active_testchain =
    match active_testchain with
    | Some testchain
      when Chain_id.equal chain_id testchain.testchain_store.chain_id ->
        return_some testchain
    | _ ->
        let chain_dir = chain_store.chain_dir in
        let testchain_dir =
          Naming.(chain_dir // testchain_dir // chain_store chain_id)
        in
        Shared.use chain_store.chain_state (fun {forked_chains; _} ->
            Stored_data.read forked_chains
            >>= fun forked_chains ->
            match Chain_id.Map.find_opt chain_id forked_chains with
            | None ->
                return_none
            | Some forked_block ->
                load_chain_store
                  chain_store.global_store
                  ~chain_dir:testchain_dir
                  ~chain_id
                  ~readonly:false
                >>=? fun testchain_store ->
                let testchain = {forked_block; testchain_store} in
                return_some testchain)

  let fork_testchain chain_store ~testchain_id ~forked_block ~genesis_hash
      ~genesis_header ~test_protocol ~expiration =
    let forked_block_hash = Block.hash forked_block in
    let genesis_hash' = Context.compute_testchain_genesis forked_block_hash in
    assert (Block_hash.equal genesis_hash genesis_hash') ;
    fail_unless
      chain_store.global_store.allow_testchains
      Fork_testchain_not_allowed
    >>=? fun () ->
    Shared.update_with
      chain_store.chain_state
      (fun ({active_testchain; _} as chain_state) ->
        match active_testchain with
        | Some ({testchain_store; forked_block} as testchain) ->
            (* Already forked and active *)
            if Chain_id.equal testchain_store.chain_id testchain_id then (
              assert (Block_hash.equal forked_block forked_block_hash) ;
              return (None, testchain) )
            else fail (Cannot_fork_testchain testchain_id)
        | None ->
            let chain_dir = chain_store.chain_dir in
            let testchain_dir =
              Naming.(chain_dir // testchain_dir // chain_store testchain_id)
            in
            if Sys.file_exists testchain_dir && Sys.is_directory testchain_dir
            then
              locked_load_testchain
                chain_store
                ~chain_id:testchain_id
                active_testchain
              >>=? function
              | None ->
                  fail (Cannot_load_testchain testchain_dir)
              | Some testchain ->
                  return (None, testchain)
            else
              (* Inherit history mode *)
              let history_mode = history_mode chain_store in
              (* Inherit history mode *)
              let genesis_block =
                create_testchain_genesis_block ~genesis_hash ~genesis_header
              in
              let genesis =
                {
                  Genesis.block = genesis_hash;
                  time = Block.timestamp genesis_block;
                  protocol = test_protocol;
                }
              in
              let genesis_context = Block.context_hash genesis_block in
              create_chain_store
                chain_store.global_store
                ~chain_dir:testchain_dir
                ~chain_id:testchain_id
                ~expiration:(Some expiration)
                ~genesis_block
                ~genesis
                ~genesis_context
                history_mode
              >>=? fun testchain_store ->
              Stored_data.update_with
                chain_state.forked_chains
                (fun forked_chains ->
                  Lwt.return
                    (Chain_id.Map.add
                       testchain_id
                       forked_block_hash
                       forked_chains))
              >>= fun () ->
              Lwt.return {forked_block = forked_block_hash; testchain_store}
              >>= fun testchain ->
              Event.(emit fork_testchain)
                ( testchain_id,
                  test_protocol,
                  genesis_hash,
                  forked_block_hash,
                  Block.level forked_block )
              >>= fun () ->
              return
                ( Some {chain_state with active_testchain = Some testchain},
                  testchain ))

  (* Look for chain_store's testchains - does not look recursively *)
  let load_testchain chain_store ~chain_id =
    Shared.use chain_store.chain_state (fun {active_testchain; _} ->
        locked_load_testchain chain_store ~chain_id active_testchain)

  (* FIXME: How test chains are cleaned? Regarding both context and store. *)
  let shutdown_testchain chain_store =
    Shared.update_with
      chain_store.chain_state
      (fun ({active_testchain; _} as chain_state) ->
        match active_testchain with
        | Some testchain ->
            close_chain_store testchain.testchain_store
            >>= fun () ->
            return (Some {chain_state with active_testchain = None}, ())
        | None ->
            return (None, ()))

  (* Protocols *)

  let compute_commit_info chain_store block =
    let index = chain_store.global_store.context_index in
    protect
      ~on_error:(fun _ -> return_none)
      (fun () ->
        get_commit_info index block
        >>=? fun commit_info -> return_some commit_info)

  let set_protocol_level chain_store ~protocol_level (block, protocol_hash) =
    Shared.locked_use chain_store.chain_state (fun {protocol_levels; _} ->
        compute_commit_info chain_store (Block.header block)
        >>=? fun commit_info_opt ->
        Stored_data.update_with protocol_levels (fun protocol_levels ->
            Lwt.return
              Protocol_levels.(
                add
                  protocol_level
                  {
                    block = Block.descriptor block;
                    protocol = protocol_hash;
                    commit_info = commit_info_opt;
                  }
                  protocol_levels))
        >>= fun () -> return_unit)

  let find_activation_block chain_store ~protocol_level =
    Shared.use chain_store.chain_state (fun {protocol_levels; _} ->
        Stored_data.read protocol_levels
        >>= fun protocol_levels ->
        Lwt.return (Protocol_levels.find_opt protocol_level protocol_levels))

  let find_protocol chain_store ~protocol_level =
    find_activation_block chain_store ~protocol_level
    >>= function
    | None ->
        Lwt.return_none
    | Some {Protocol_levels.protocol; _} ->
        Lwt.return_some protocol

  let may_update_protocol_level chain_store ~protocol_level
      (block, protocol_hash) =
    find_activation_block chain_store ~protocol_level
    >>= function
    | Some _ ->
        return_unit
    | None ->
        set_protocol_level chain_store ~protocol_level (block, protocol_hash)

  let all_protocol_levels chain_store =
    Shared.use chain_store.chain_state (fun {protocol_levels; _} ->
        Stored_data.read protocol_levels)

  let watcher chain_store = Lwt_watcher.create_stream chain_store.block_watcher

  let get_rpc_directory chain_store block =
    Block.read_predecessor_opt chain_store block
    >>= function
    | None ->
        Lwt.return_none (* genesis *)
    | Some pred when Block_hash.equal (Block.hash pred) (Block.hash block) ->
        Lwt.return_none (* genesis *)
    | Some pred -> (
        savepoint chain_store
        >>= fun (_, save_point_level) ->
        ( if Compare.Int32.(Block.level pred < save_point_level) then
          find_activation_block
            chain_store
            ~protocol_level:(Block.proto_level pred)
          >>= function
          | Some {Protocol_levels.protocol; _} ->
              Lwt.return protocol
          | None ->
              Lwt.fail Not_found
        else Block.protocol_hash_exn chain_store pred )
        >>= fun protocol ->
        match
          Protocol_hash.Table.find_opt
            chain_store.block_rpc_directories
            protocol
        with
        | None ->
            Lwt.return_none
        | Some map ->
            Block.protocol_hash_exn chain_store block
            >>= fun next_protocol ->
            Lwt.return (Protocol_hash.Map.find_opt next_protocol map) )

  let set_rpc_directory chain_store protocol_hash dir =
    let map =
      Option.unopt
        ~default:Protocol_hash.Map.empty
        (Protocol_hash.Table.find_opt
           chain_store.block_rpc_directories
           protocol_hash)
    in
    Protocol_hash.Table.replace
      chain_store.block_rpc_directories
      protocol_hash
      (Protocol_hash.Map.add protocol_hash dir map) ;
    Lwt.return_unit
end

module Protocol = struct
  let all {protocol_store; _} = Protocol_store.all protocol_store

  let store {protocol_store; protocol_watcher; _} protocol_hash protocol =
    Protocol_store.store protocol_store protocol_hash protocol
    >>= function
    | None ->
        Lwt.return_none
    | p ->
        Lwt_watcher.notify protocol_watcher protocol_hash ;
        Lwt.return p

  let store_raw {protocol_store; protocol_watcher; _} protocol_hash
      raw_protocol =
    Protocol_store.raw_store protocol_store protocol_hash raw_protocol
    >>= function
    | None ->
        Lwt.return_none
    | p ->
        Lwt_watcher.notify protocol_watcher protocol_hash ;
        Lwt.return p

  let read {protocol_store; _} protocol_hash =
    Protocol_store.read protocol_store protocol_hash

  let mem {protocol_store; _} protocol_hash =
    Protocol_store.mem protocol_store protocol_hash

  let protocol_watcher {protocol_watcher; _} =
    Lwt_watcher.create_stream protocol_watcher
end

let create_store ~store_dir ~context_index ~chain_id ~genesis ~genesis_context
    ?(history_mode = History_mode.default) ~allow_testchains =
  let open Naming in
  Lwt_utils_unix.create_dir store_dir
  >>= fun () ->
  Protocol_store.init ~store_dir
  >>= fun protocol_store ->
  let protocol_watcher = Lwt_watcher.create_input () in
  let global_block_watcher = Lwt_watcher.create_input () in
  let chain_dir = store_dir // chain_store chain_id in
  let global_store =
    {
      store_dir;
      context_index;
      main_chain_store = None;
      protocol_store;
      allow_testchains;
      protocol_watcher;
      global_block_watcher;
    }
  in
  Chain.create_chain_store
    global_store
    ~chain_dir
    ~chain_id
    ~expiration:None
    ~genesis
    ~genesis_context
    history_mode
  >>=? fun main_chain_store ->
  global_store.main_chain_store <- Some main_chain_store ;
  return global_store

(* Computes the savepoint which is expected given a cemented store and
   an history mode's offset. None is returned it the cemented_store is
   not accorded to the given offset. *)
let expected_savepoint main_chain_store offset =
  let cemented_store =
    Block_store.cemented_block_store main_chain_store.block_store
  in
  let cemented_block_files =
    Cemented_block_store.cemented_blocks_files cemented_store
  in
  let nb_files = Array.length cemented_block_files in
  let index = nb_files - offset in
  if index < 0 then
    (* When the expected offset is below the last known block*)
    None
  else if index >= nb_files then
    (* When the offset = 0, we take the checkpoint *)
    let cycle = cemented_block_files.(nb_files - 1) in
    Some (Int32.succ cycle.end_level)
  else
    (* We take the expected level *)
    let cycle = cemented_block_files.(index) in
    Some cycle.start_level

(* Returns the savepoint which is actually reacheable given an
   expected savepoint level.*)
let available_savepoint main_chain_store expected_level =
  Chain.savepoint main_chain_store
  >>= fun current_savepoint ->
  if expected_level < snd current_savepoint then
    Lwt.return (snd current_savepoint)
  else Lwt.return expected_level

(* Returns the preserved level is the given level is higher that the
   current preserved block. The preserved block aims to be the one needed
   and maintained available to export snasphot. That is to say:
   lafl(head) - max_op_ttl(lafl).*)
let preserved_block main_chain_store expected_level =
  Chain.current_head main_chain_store
  >>= fun current_head ->
  Block.get_block_metadata main_chain_store current_head
  >>=? fun current_head_metadata ->
  let head_lafl = Block_repr.last_allowed_fork_level current_head_metadata in
  let head_max_op_ttl =
    Int32.of_int (Block_repr.max_operations_ttl current_head_metadata)
  in
  let block_to_preserve = Int32.(max 0l (sub head_lafl head_max_op_ttl)) in
  let new_block_level = min block_to_preserve expected_level in
  Block.read_block_by_level main_chain_store new_block_level
  >>=? fun block -> return (Block.descriptor block)

let infer_savepoint main_chain_store ~previous_offset ~target_offset =
  ( match previous_offset with
  | Some previous_offset -> (
    (*Full or rolling offset*)
    match expected_savepoint main_chain_store target_offset with
    | Some expected_savepoint_level ->
        (* Fit the right cycle *)
        if previous_offset <= target_offset then
          Lwt.return (Int32.succ expected_savepoint_level)
        else Lwt.return expected_savepoint_level
    | None ->
        (* The expected savepoint cannot be satisfied. Instead, we
          return the current savepoint (best effort). *)
        Chain.savepoint main_chain_store
        >>= fun current_savepoint -> Lwt.return (snd current_savepoint) )
  | None -> (
    (*Archive offset*)
    match expected_savepoint main_chain_store target_offset with
    | Some expected_savepoint_level ->
        Lwt.return expected_savepoint_level
    | None ->
        Chain.caboose main_chain_store
        >>= fun current_caboose -> Lwt.return (snd current_caboose) ) )
  >>= fun expected_savepoint_level ->
  available_savepoint main_chain_store expected_savepoint_level
  >>= fun available_savepoint ->
  preserved_block main_chain_store available_savepoint

(* Computes the caboose which is expected given a cemented store and
   an history mode's offset. None is returned it the cemented_store is
   not accorded to the given offset. *)
let expected_caboose main_chain_store offset =
  let cemented_store =
    Block_store.cemented_block_store main_chain_store.block_store
  in
  let cemented_block_files =
    Cemented_block_store.cemented_blocks_files cemented_store
  in
  let nb_files = Array.length cemented_block_files in
  let index = nb_files - offset in
  if offset <> 0 && index >= 0 then
    let cycle = cemented_block_files.(nb_files - offset) in
    Some cycle.start_level
  else (* The expected caboose cannot be satisfied *)
    None

let infer_caboose main_chain_store savepoint offset ~new_history_mode =
  function
  | History_mode.Archive -> (
    match new_history_mode with
    | History_mode.Archive ->
        assert false
    | Full _ ->
        Chain.caboose main_chain_store >>= return
    | Rolling _ ->
        return savepoint )
  | Full _ -> (
    match expected_caboose main_chain_store offset with
    | Some expected_caboose ->
        preserved_block main_chain_store expected_caboose
    | None ->
        return savepoint )
  | Rolling r ->
      if r.offset < offset then Chain.caboose main_chain_store >>= return
      else return savepoint

let switch_history_mode main_chain_store previous_history_mode new_history_mode
    =
  let open History_mode in
  let is_valid_switch =
    match (previous_history_mode, new_history_mode) with
    | (p, n) when History_mode.equal p n ->
        false
    | (Archive, Full _)
    | (Archive, Rolling _)
    | (Full _, Full _)
    | (Full _, Rolling _)
    | (Rolling _, Rolling _) ->
        true
    | _ ->
        (* The remaining combinations are inavlid. *)
        false
  in
  fail_unless
    is_valid_switch
    (Incorrect_history_mode_switch
       {previous_mode = previous_history_mode; next_mode = new_history_mode})
  >>=? fun () ->
  Chain.set_history_mode main_chain_store new_history_mode
  >>=? fun () ->
  ( match (previous_history_mode, new_history_mode) with
  | (Full n, Rolling m) | (Rolling n, Rolling m) ->
      infer_savepoint
        main_chain_store
        ~previous_offset:(Some n.offset)
        ~target_offset:m.offset
      >>=? fun new_savepoint ->
      infer_caboose
        main_chain_store
        new_savepoint
        m.offset
        ~new_history_mode
        previous_history_mode
      >>=? fun new_caboose ->
      Chain.current_head main_chain_store
      >>= fun _current_head ->
      (* Block_store.do_the_magic
       *   main_chain_store.block_store
       *   main_chain_store.chain_dir
       *   ~from_block:(Block.descriptor current_head)
       *   ~to_block:new_caboose
       * >>=? fun () -> *)
      let cemented_block_store =
        Block_store.cemented_block_store main_chain_store.block_store
      in
      (* Cemented_block_store.clean_indexes cemented_block_store (snd new_caboose) ; *)
      Cemented_block_store.trigger_gc cemented_block_store new_history_mode
      >>= fun () ->
      (* Cemented_block_store.comply_to_history_mode
       *   cemented_block_store
       *   new_history_mode
       * >>= fun () -> *)
      Chain.set_savepoint main_chain_store new_savepoint
      >>=? fun () -> Chain.set_caboose main_chain_store new_caboose
  | (Full n, Full m) ->
      infer_savepoint
        main_chain_store
        ~previous_offset:(Some n.offset)
        ~target_offset:m.offset
      >>=? fun new_savepoint ->
      Cemented_block_store.comply_to_history_mode
        (Block_store.cemented_block_store main_chain_store.block_store)
        new_history_mode
      >>= fun () -> Chain.set_savepoint main_chain_store new_savepoint
  | (Archive, Full m) | (Archive, Rolling m) ->
      infer_savepoint
        main_chain_store
        ~previous_offset:None
        ~target_offset:m.offset
      >>=? fun new_savepoint ->
      infer_caboose
        main_chain_store
        new_savepoint
        m.offset
        ~new_history_mode
        previous_history_mode
      >>=? fun new_caboose ->
      Cemented_block_store.comply_to_history_mode
        (Block_store.cemented_block_store main_chain_store.block_store)
        new_history_mode
      >>= fun () ->
      Chain.set_savepoint main_chain_store new_savepoint
      >>=? fun () -> Chain.set_caboose main_chain_store new_caboose
  | _ ->
      (* Should not happen *)
      assert false )
  >>=? fun () ->
  Event.(emit switch_history_mode (previous_history_mode, new_history_mode))
  >>= fun () -> return_unit

let load_store ?history_mode ~force_history_mode_switch ~store_dir
    ~context_index ~chain_id ~allow_testchains ~readonly () =
  (* TODO: check genesis coherence and fail if mismatch *)
  let chain_dir = Naming.(store_dir // chain_store chain_id) in
  Protocol_store.init ~store_dir
  >>= fun protocol_store ->
  let protocol_watcher = Lwt_watcher.create_input () in
  let global_block_watcher = Lwt_watcher.create_input () in
  let global_store =
    {
      store_dir;
      context_index;
      main_chain_store = None;
      protocol_store;
      allow_testchains;
      protocol_watcher;
      global_block_watcher;
    }
  in
  Chain.load_chain_store global_store ~chain_dir ~chain_id ~readonly
  >>=? fun main_chain_store ->
  ( match history_mode with
  | None ->
      return main_chain_store
  | Some history_mode ->
      let previous_history_mode = Chain.history_mode main_chain_store in
      fail_when
        (history_mode <> previous_history_mode && not force_history_mode_switch)
        (Cannot_switch_history_mode
           {previous_mode = previous_history_mode; next_mode = history_mode})
      >>=? fun () ->
      if force_history_mode_switch then
        switch_history_mode main_chain_store previous_history_mode history_mode
        >>=? fun () ->
        (* update history mode in pre-loaded chain config *)
        let chain_config = {main_chain_store.chain_config with history_mode} in
        return {main_chain_store with chain_config}
      else return main_chain_store )
  >>=? fun main_chain_store ->
  global_store.main_chain_store <- Some main_chain_store ;
  return global_store

let main_chain_store store =
  Option.unopt_assert ~loc:__POS__ store.main_chain_store

let init ?patch_context ?commit_genesis ?history_mode
    ?(force_history_mode_switch = false) ?(readonly = false) ~store_dir
    ~context_dir ~allow_testchains genesis =
  let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
  ( match commit_genesis with
  | Some commit_genesis ->
      Context.init ~readonly:true ?patch_context context_dir
      >>= fun context_index -> Lwt.return (context_index, commit_genesis)
  | None ->
      Context.init ~readonly ?patch_context context_dir
      >>= fun context_index ->
      let commit_genesis ~chain_id =
        Context.commit_genesis
          context_index
          ~chain_id
          ~time:genesis.time
          ~protocol:genesis.protocol
      in
      Lwt.return (context_index, commit_genesis) )
  >>= fun (context_index, commit_genesis) ->
  let chain_dir = Naming.(store_dir // chain_store chain_id) in
  (* FIXME: should be checked with the store's consistency check
     (along with load_chain_state checks) *)
  (* FIXME check that only one chain_dir exists *)
  if Sys.file_exists chain_dir && Sys.is_directory chain_dir then
    load_store
      ?history_mode
      ~force_history_mode_switch
      ~store_dir
      ~context_index
      ~chain_id
      ~allow_testchains
      ~readonly
      ()
  else
    (* Fresh store *)
    commit_genesis ~chain_id
    >>=? fun genesis_context ->
    create_store
      ~store_dir
      ~context_index
      ~chain_id
      ~genesis
      ~genesis_context
      ?history_mode
      ~allow_testchains
    >>=? fun store -> return store

let close_store global_store =
  Lwt_watcher.shutdown_input global_store.protocol_watcher ;
  Lwt_watcher.shutdown_input global_store.global_block_watcher ;
  let main_chain_store =
    Option.unopt_assert ~loc:__POS__ global_store.main_chain_store
  in
  Chain.close_chain_store main_chain_store
  >>= fun () -> Context.close global_store.context_index

let get_chain_store store chain_id =
  let chain_store = main_chain_store store in
  let rec loop chain_store =
    if Chain_id.equal (Chain.chain_id chain_store) chain_id then
      return chain_store
    else
      Shared.use chain_store.chain_state (fun {active_testchain; _} ->
          match active_testchain with
          | None ->
              fail (Validation_errors.Unknown_chain chain_id)
          | Some {testchain_store; _} ->
              loop testchain_store)
  in
  loop chain_store

let get_chain_store_opt store chain_id =
  get_chain_store store chain_id
  >>= function
  | Ok chain_store -> Lwt.return_some chain_store | Error _ -> Lwt.return_none

let all_chain_stores store =
  let chain_store = main_chain_store store in
  let rec loop acc chain_store =
    let acc = chain_store :: acc in
    Shared.use chain_store.chain_state (fun {active_testchain; _} ->
        match active_testchain with
        | None ->
            Lwt.return acc
        | Some {testchain_store; _} ->
            loop acc testchain_store)
  in
  loop [] chain_store

let directory store = store.store_dir

let context_index store = store.context_index

let allow_testchains {allow_testchains; _} = allow_testchains

let global_block_watcher {global_block_watcher; _} =
  Lwt_watcher.create_stream global_block_watcher

(************ For testing and internal purposes only **************)
module Unsafe = struct
  let repr_of_block b = b

  let block_of_repr b = b

  let get_block_store chain_store = chain_store.block_store

  let set_head chain_store new_head =
    Shared.update_with chain_store.chain_state (fun chain_state ->
        Stored_data.write chain_state.current_head new_head
        >>= fun () ->
        return
          (Some {chain_state with current_head = chain_state.current_head}, ()))

  let set_checkpoint chain_store new_checkpoint =
    Shared.update_with chain_store.chain_state (fun chain_state ->
        Stored_data.write chain_state.checkpoint_data new_checkpoint
        >>= fun () ->
        return (Some {chain_state with checkpoint = new_checkpoint}, ()))

  let set_history_mode = Chain.set_history_mode

  let set_savepoint = Chain.set_savepoint

  let set_caboose = Chain.set_caboose

  let load_testchain = Chain.load_testchain

  let open_for_snapshot_export ~store_dir ~context_dir genesis
      ~(locked_f : chain_store * Context.index -> 'a tzresult Lwt.t) =
    let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
    Context.init ~readonly:true context_dir
    >>= fun context_index ->
    load_store
      ~store_dir
      ~context_index
      ~chain_id
      ~allow_testchains:false
      ~readonly:true
      ~force_history_mode_switch:false
      ()
    >>=? fun store ->
    let chain_store = main_chain_store store in
    lock_for_read chain_store.lockfile
    >>= fun () ->
    Error_monad.protect (fun () ->
        Lwt.finalize
          (fun () -> locked_f (chain_store, context_index))
          (fun () -> close_store store))

  let restore_from_snapshot ?(notify = fun () -> Lwt.return_unit) ~store_dir
      ~context_index ~genesis ~genesis_context_hash ~floating_blocks_stream
      ~new_head_with_metadata ~protocol_levels ~history_mode =
    let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
    let chain_dir = Naming.(store_dir // chain_store chain_id) in
    let genesis_block =
      Chain.create_genesis_block ~genesis genesis_context_hash
    in
    let new_head_descr =
      ( Block_repr.hash new_head_with_metadata,
        Block_repr.level new_head_with_metadata )
    in
    (* Write consistent stored data *)
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.protocol_levels)
      Protocol_levels.encoding
      protocol_levels
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.current_head)
      Block_repr.encoding
      new_head_with_metadata
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.alternate_heads)
      (Block_hash.Map.encoding Data_encoding.int32)
      Block_hash.Map.empty
    >>= fun () ->
    (* Checkpoint is the new head *)
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.checkpoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      new_head_descr
    >>= fun () ->
    (* Savepoint is the head *)
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.savepoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      new_head_descr
    >>= fun () ->
    (* Depending on the history mode, set the caboose properly *)
    ( match history_mode with
    | History_mode.Archive | Full _ ->
        return (Block_repr.hash genesis_block, Block_repr.level genesis_block)
    | Rolling _ -> (
        Lwt_stream.peek floating_blocks_stream
        >>= function
        | None ->
            (* This should not happen. The floating store of a
               snapshot exported at highest_cemented_block + 1 should
               have a floating store populated with the cemented
               cycle. *)
            assert false
        | Some caboose -> (
          match Block_repr.metadata new_head_with_metadata with
          | None ->
              assert false
          | Some metadata ->
              if
                Int32.sub
                  (Block_repr.level new_head_with_metadata)
                  (Int32.of_int metadata.max_operations_ttl)
                <= 0l
              then return (genesis.block, 0l)
              else return (Block_repr.hash caboose, Block_repr.level caboose) )
        ) )
    >>=? fun caboose_descr ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.caboose)
      Data_encoding.(tup2 Block_hash.encoding int32)
      caboose_descr
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Naming.Chain_data.invalid_blocks)
      (Block_hash.Map.encoding invalid_block_encoding)
      Block_hash.Map.empty
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.forked_chains)
      (Chain_id.Map.encoding Block_hash.encoding)
      Chain_id.Map.empty
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.genesis)
      Block_repr.encoding
      genesis_block
    >>= fun () ->
    (* Load the store (containing the cemented if relevant) *)
    Block_store.load ~chain_dir ~genesis_block ~readonly:false
    >>=? fun block_store ->
    (* Store the floating (in the correct order!) *)
    Lwt_stream.iter_s
      (fun block ->
        Block_store.store_block block_store block >>= fun () -> notify ())
      floating_blocks_stream
    >>= fun () ->
    (* Store the head *)
    Block_store.store_block block_store new_head_with_metadata
    >>= fun () ->
    (* Check correctness of protocol transition blocks *)
    let open Protocol_levels in
    iter_s
      (fun (_, {block = (bh, _); protocol; commit_info = commit_info_opt}) ->
        Block_store.read_block block_store ~read_metadata:false (Block (bh, 0))
        >>=? fun block_opt ->
        match (block_opt, commit_info_opt) with
        | (None, _) -> (
          match history_mode with
          | Rolling _ ->
              (* If we are importing a rolling snapshot then allow the
               absence of block. *)
              return_unit
          | _ ->
              fail (Missing_activation_block (bh, protocol, history_mode)) )
        | (Some _block, None) ->
            (* TODO no commit info : raise a warning *)
            return_unit
        | (Some block, Some commit_info) ->
            Context.check_protocol_commit_consistency
              context_index
              ~expected_context_hash:(Block.context_hash block)
              ~given_protocol_hash:protocol
              ~author:commit_info.author
              ~message:commit_info.message
              ~timestamp:(Block.timestamp block)
              ~test_chain_status:commit_info.test_chain_status
              ~data_merkle_root:commit_info.data_merkle_root
              ~parents_contexts:commit_info.parents_contexts
            >>= fun is_consistent ->
            fail_unless
              ( is_consistent
              || Compare.Int32.(equal (Block_repr.level block) 0l) )
              (Inconsistent_protocol_commit_info (Block.hash block, protocol)))
      (Protocol_levels.bindings protocol_levels)
    >>=? fun () ->
    Block_store.close block_store
    >>= fun () ->
    let chain_config =
      {Chain_config.history_mode; genesis; expiration = None}
    in
    Chain_config.write ~chain_dir chain_config >>=? fun () -> return_unit

  let restore_from_legacy_snapshot ?(notify = fun () -> Lwt.return_unit)
      ~store_dir ~context_index ~genesis ~genesis_context_hash
      ~floating_blocks_stream ~new_head_with_metadata ~partial_protocol_levels
      ~history_mode =
    let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
    let chain_dir = Naming.(store_dir // chain_store chain_id) in
    let genesis_block =
      Chain.create_genesis_block ~genesis genesis_context_hash
    in
    let new_head_descr =
      ( Block_repr.hash new_head_with_metadata,
        Block_repr.level new_head_with_metadata )
    in
    (* Write consistent stored data *)
    (* We will write protocol levels when we have access to blocks to
       retrieve necessary infos *)
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.current_head)
      Block_repr.encoding
      new_head_with_metadata
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.alternate_heads)
      (Block_hash.Map.encoding Data_encoding.int32)
      Block_hash.Map.empty
    >>= fun () ->
    (* Checkpoint is the new head *)
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.checkpoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      new_head_descr
    >>= fun () ->
    (* Savepoint is the head *)
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.savepoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      new_head_descr
    >>= fun () ->
    (* Depending on the history mode, set the caboose properly *)
    ( match history_mode with
    | History_mode.Archive | Full _ ->
        return genesis_block
    | Rolling _ -> (
        Lwt_stream.peek floating_blocks_stream
        >>= function
        | None ->
            (* This should not happen. It is ensured, by construction
               when exporting a (valid) snapshot. *)
            assert false
        | Some caboose ->
            return caboose ) )
    >>=? fun caboose ->
    let caboose_descr = Block.descriptor caboose in
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.caboose)
      Data_encoding.(tup2 Block_hash.encoding int32)
      caboose_descr
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Naming.Chain_data.invalid_blocks)
      (Block_hash.Map.encoding invalid_block_encoding)
      Block_hash.Map.empty
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.forked_chains)
      (Chain_id.Map.encoding Block_hash.encoding)
      Chain_id.Map.empty
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.genesis)
      Block_repr.encoding
      genesis_block
    >>= fun () ->
    (* Load the store (containing the cemented if relevant) *)
    Block_store.load ~chain_dir ~genesis_block ~readonly:false
    >>=? fun block_store ->
    (* Store the floating *)
    Lwt_stream.iter_s
      (fun block ->
        Block_store.store_block block_store block >>= fun () -> notify ())
      floating_blocks_stream
    >>= fun () ->
    (* Store the head *)
    Block_store.store_block block_store new_head_with_metadata
    >>= fun () ->
    notify ()
    >>= fun () ->
    (* We also need to store the genesis' protocol transition *)
    Chain.get_commit_info context_index (Block.header genesis_block)
    >>=? fun genesis_commit_info ->
    let initial_protocol_levels =
      Protocol_levels.(
        add
          (Block.proto_level genesis_block)
          {
            block = Block.descriptor genesis_block;
            protocol = genesis.protocol;
            commit_info = Some genesis_commit_info;
          }
          empty)
    in
    Context.checkout_exn
      context_index
      (Block.context_hash new_head_with_metadata)
    >>= fun context ->
    Context.get_protocol context
    >>=? fun head_protocol ->
    (* Compute protocol levels and check their correctness *)
    fold_left_s
      (fun proto_levels (transition_level, protocol_hash, commit_info_opt) ->
        let distance =
          Int32.(
            to_int
              (sub (Block_repr.level new_head_with_metadata) transition_level))
        in
        (* FIXME? what happens when the snapshot does not contain the
           block ? (i.e. rolling) *)
        Block_store.read_block
          block_store
          ~read_metadata:false
          (Block (Block_repr.hash new_head_with_metadata, distance))
        >>=? fun block_opt ->
        match (block_opt, commit_info_opt) with
        | (None, _) -> (
          match history_mode with
          | Rolling _ ->
              (* Corner-case for when the head protocol's transition
                 block has been deleted. *)
              let block =
                (* Important: we cannot retrieve the protocol
                   associated to an arbitrary block with a legacy
                   snapshot, we only know the head's one as it's
                   written in the context. Therefore, the transition
                   block is overwritten with either the caboose if
                   both blocks have the same proto_level or the
                   current_head otherwise. In the former case, block's
                   protocol data won't be deserialisable.  *)
                if
                  Compare.Int.(
                    Block.proto_level caboose
                    = Block.proto_level new_head_with_metadata)
                then caboose_descr
                else Block.descriptor new_head_with_metadata
              in
              if Protocol_hash.equal protocol_hash head_protocol then
                return
                  Protocol_levels.(
                    add
                      (Block.proto_level new_head_with_metadata)
                      {block; protocol = protocol_hash; commit_info = None}
                      proto_levels)
              else return proto_levels
          | _ ->
              fail
                (Missing_activation_block_legacy
                   (transition_level, protocol_hash, history_mode)) )
        | (Some block, None) ->
            (* TODO no commit info: raise a warning *)
            return
              Protocol_levels.(
                add
                  (Block.proto_level block)
                  {
                    block = Block.descriptor block;
                    protocol = protocol_hash;
                    commit_info = commit_info_opt;
                  }
                  proto_levels)
        | (Some block, Some commit_info) ->
            let open Protocol_levels in
            Context.check_protocol_commit_consistency
              context_index
              ~expected_context_hash:(Block.context_hash block)
              ~given_protocol_hash:protocol_hash
              ~author:commit_info.author
              ~message:commit_info.message
              ~timestamp:(Block.timestamp block)
              ~test_chain_status:commit_info.test_chain_status
              ~data_merkle_root:commit_info.data_merkle_root
              ~parents_contexts:commit_info.parents_contexts
            >>= fun is_consistent ->
            if
              is_consistent
              || Compare.Int32.(equal (Block_repr.level block) 0l)
            then
              return
                Protocol_levels.(
                  add
                    (Block_repr.proto_level block)
                    {
                      block = Block.descriptor block;
                      protocol = protocol_hash;
                      commit_info = commit_info_opt;
                    }
                    proto_levels)
            else
              fail
                (Inconsistent_protocol_commit_info
                   (Block.hash block, protocol_hash)))
      initial_protocol_levels
      partial_protocol_levels
    >>=? fun protocol_levels ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.protocol_levels)
      Protocol_levels.encoding
      protocol_levels
    >>= fun () ->
    Block_store.close block_store
    >>= fun () ->
    let chain_config =
      {Chain_config.history_mode; genesis; expiration = None}
    in
    Chain_config.write ~chain_dir chain_config >>=? fun () -> return_unit

  let restore_from_legacy_upgrade ~store_dir ~genesis alternate_heads
      invalid_blocks forked_chains =
    let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
    let chain_dir = Naming.(store_dir // chain_store chain_id) in
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.alternate_heads)
      (Block_hash.Map.encoding Data_encoding.int32)
      alternate_heads
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Naming.Chain_data.invalid_blocks)
      (Block_hash.Map.encoding invalid_block_encoding)
      invalid_blocks
    >>= fun () ->
    Stored_data.write_file
      ~file:Naming.(chain_dir // Chain_data.forked_chains)
      (Chain_id.Map.encoding Block_hash.encoding)
      forked_chains
    >>= fun () -> return_unit
end
