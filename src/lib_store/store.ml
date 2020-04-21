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
  Block_store.get_predecessor block_store hash distance

let get_highest_cemented_level chain_store =
  Cemented_block_store.get_highest_cemented_level
    chain_store.block_store.Block_store.cemented_store

(* Will that block be compatible with the current store ? *)
let locked_is_acceptable_block chain_store chain_state (hash, level) =
  Stored_data.read chain_state.current_head
  >>= fun current_head ->
  Block_store.read_block_metadata
    chain_store.block_store
    (Hash (Block_repr.hash current_head, 0))
  >>= function
  | None ->
      assert false
  | Some metadata ->
      let level_limit = Block_repr.last_allowed_fork_level metadata in
      (* The block must be at least after the highest cemented block (or
         SP when no blocks are cemented) *)
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

  let repr b = b

  let of_repr b = b

  let repr_metadata m = m

  let of_repr_metadata m = m

  let equal b b' = Block_hash.equal (Block_repr.hash b) (Block_repr.hash b')

  let descriptor blk = (Block_repr.hash blk, Block_repr.level blk)

  (* I/O operations *)

  let is_known_valid {block_store; _} hash =
    Block_store.(is_known block_store (Hash (hash, 0)))

  let is_known_invalid {chain_state; _} hash =
    Shared.use chain_state (fun {invalid_blocks; _} ->
        Stored_data.read invalid_blocks
        >>= fun invalid_blocks ->
        Lwt.return (Block_hash.Map.mem hash invalid_blocks))

  let is_known chain_store hash =
    is_known_valid chain_store hash
    >>= fun is_known ->
    if is_known then Lwt.return_true else is_known_invalid chain_store hash

  let block_validity chain_store hash =
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
      (Hash (hash, distance))
    >>= function
    | None ->
        (* TODO lift the error to block_store *)
        fail (Store_errors.Block_not_found hash)
    | Some block ->
        return block

  let read_block_metadata ?(distance = 0) chain_store hash =
    Block_store.read_block_metadata
      chain_store.block_store
      (Hash (hash, distance))

  let get_block_metadata_opt chain_store block =
    match Block_repr.metadata block with
    | Some metadata ->
        Lwt.return_some metadata
    | None -> (
        Block_store.read_block_metadata
          chain_store.block_store
          (Hash (block.hash, 0))
        >>= function
        | Some metadata ->
            block.metadata <- Some metadata ;
            Lwt.return_some metadata
        | None ->
            Lwt.return_none )

  let get_block_metadata chain_store block =
    get_block_metadata_opt chain_store block
    >>= function
    | Some metadata ->
        block.metadata <- Some metadata ;
        return metadata
    | None ->
        fail (Store_errors.Block_metadata_not_found (Block_repr.hash block))

  (* Pas bon *)
  let store_block_metadata chain_store chunk =
    Lwt_list.map_s
      (fun ((block : t), metadata) ->
        Lwt.return {block with metadata = Some metadata})
      chunk
    >>= fun blocks ->
    Block_store.store_metadata_chunk chain_store.block_store blocks

  let read_block_opt chain_store ?(distance = 0) hash =
    (* TODO: Make sure the checkpoint is still reachable *)
    read_block chain_store ~distance hash
    >>= function
    | Ok block -> Lwt.return_some block | Error _ -> Lwt.return_none

  (* Predecessor = itself for genesis *)
  let read_predecessor chain_store block =
    read_block chain_store (Block_repr.predecessor block)

  let read_predecessor_opt chain_store block =
    read_predecessor chain_store block
    >>= function
    | Ok block -> Lwt.return_some block | Error _ -> Lwt.return_none

  let read_ancestor_hash chain_store ~distance hash =
    read_ancestor_hash chain_store ~distance hash

  let read_predecessor_of_hash_opt chain_store hash =
    read_ancestor_hash chain_store ~distance:1 hash
    >>= function
    | Some hash -> read_block_opt chain_store hash | None -> Lwt.return_none

  let read_predecessor_of_hash chain_store hash =
    read_predecessor_of_hash_opt chain_store hash
    >>= function
    | Some b -> return b | None -> fail (Store_errors.Block_not_found hash)

  let locked_read_block_by_level chain_store head level =
    let distance = Int32.(to_int (sub (Block_repr.level head) level)) in
    read_block chain_store ~distance (Block_repr.hash head)

  let locked_read_block_by_level_opt chain_store head level =
    let distance = Int32.(to_int (sub (Block_repr.level head) level)) in
    read_block_opt chain_store ~distance (Block_repr.hash head)

  let read_block_by_level chain_store level =
    current_head chain_store
    >>= fun current_head ->
    locked_read_block_by_level chain_store current_head level

  let read_block_by_level_opt chain_store level =
    current_head chain_store
    >>= fun current_head ->
    locked_read_block_by_level_opt chain_store current_head level

  (* TODO use proper errors *)
  let store_block chain_store ~block_header ~block_header_metadata ~operations
      ~operations_metadata ~context_hash ~message ~max_operations_ttl
      ~last_allowed_fork_level =
    let bytes = Block_header.to_bytes block_header in
    let hash = Block_header.hash_raw bytes in
    fail_unless
      (block_header.shell.validation_passes = List.length operations)
      (failure "Store.Block.store_block: invalid operations length")
    >>=? fun () ->
    fail_unless
      (block_header.shell.validation_passes = List.length operations_metadata)
      (failure "Store.Block.store_block: invalid operations_data length")
    >>=? fun () ->
    fail_unless
      (List.for_all2
         (fun l1 l2 -> List.length l1 = List.length l2)
         operations
         operations_metadata)
      (failure
         "Store.Block.store_block: inconsistent operations and operations_data")
    >>=? fun () ->
    (* let the validator check the consistency... of fitness, level, ... *)
    (* known returns true for invalid blocks as well *)
    is_known chain_store hash
    >>= fun is_known ->
    if is_known then return_none
    else
      (* Safety check: never ever commit a block that is not compatible
         with the current checkpoint. *)
      Shared.use chain_store.chain_state (fun chain_state ->
          locked_is_acceptable_block
            chain_store
            chain_state
            (hash, block_header.shell.level))
      >>= fun acceptable_block ->
      fail_unless
        acceptable_block
        (Validation_errors.Checkpoint_error (hash, None))
      >>=? fun () ->
      let commit = context_hash in
      fail_unless
        (Context_hash.equal block_header.shell.context commit)
        (Validation_errors.Inconsistent_hash
           (commit, block_header.shell.context))
      >>=? fun () ->
      let contents = {Block_repr.header = block_header; operations} in
      let metadata =
        Some
          {
            Block_repr.message;
            max_operations_ttl;
            last_allowed_fork_level;
            block_metadata = block_header_metadata;
            operations_metadata;
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
      return_some block

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
        failwith
          "Store.Block.context: failed to checkout context for block %a"
          Block_hash.pp
          (Block_repr.hash block)

  let context_exists chain_store block =
    let context_index = chain_store.global_store.context_index in
    Context.exists context_index (Block_repr.context block)

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
            failwith
              "Store.Block.protocol_hash: protocol with level %d not found"
              proto_level)

  let protocol_hash_exn chain_store block =
    protocol_hash chain_store block
    >>= function Ok ph -> Lwt.return ph | Error _ -> Lwt.fail Not_found

  let compute_locator chain_store ?(size = 200) head seed =
    caboose chain_store
    >>= fun (caboose, _caboose_level) ->
    Block_locator.compute
      ~get_predecessor:(fun h n ->
        read_ancestor_hash chain_store h ~distance:n)
      ~caboose
      ~size
      head.Block_repr.hash
      head.Block_repr.contents.header
      seed

  (* TODO: should we consider level as well ? Rolling could have
     difficulties boostrapping *)
  let filter_known_suffix chain_store locator =
    let history_mode = history_mode chain_store in
    Block_locator.unknown_prefix ~is_known:(block_validity chain_store) locator
    >>= function
    | (Known_valid, prefix_locator) ->
        Lwt.return_some prefix_locator
    | (Known_invalid, _) ->
        Lwt.return_none
    | (Unknown, _) -> (
      match history_mode with
      | Archive ->
          Lwt.return_none
      | Rolling _ | Full _ ->
          Lwt.return_some locator )

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
    if is_genesis chain_store hash then
      failwith "Cannot mark the genesis block as invalid"
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
  let path chain_store (b1 : Block.t) (b2 : Block.t) =
    if not Compare.Int32.(Block.level b1 <= Block.level b2) then
      invalid_arg "Chain_traversal.path" ;
    let rec loop acc current =
      if Block.equal b1 current then Lwt.return_some acc
      else
        Block.read_predecessor_opt chain_store current
        >>= function
        | Some pred -> loop (current :: acc) pred | None -> Lwt.return_none
    in
    loop [] b2

  let common_ancestor chain_store (b1 : Block.t) (b2 : Block.t) =
    let rec loop (b1 : Block.t) (b2 : Block.t) =
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
        (* TODO *)
        assert false
    | Some ancestor -> (
        path chain_store ancestor to_block
        >>= function
        | None ->
            (* TODO *)
            assert false
        | Some path ->
            Lwt.return (ancestor, path) )

  (* FIXME replace that with a proper cache *)
  let live_blocks chain_store ~block n =
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

(* TODO: voir ce qu'on va faire avec ça *)
(* let restore_store_consistency internal_state chain_data block_store =
 *   Stored_data.read internal_state
 *   >>= function
 *   | Idle ->
 *       (\* Nothing to do *\)
 *       Lwt.return_unit
 *   | Storing ->
 *       (\* Remove blocks from the file that are not present in the index
 *       *\)
 *       Block_store.restore_rw_blocks_consistency block_store
 *       >>= fun () -> Stored_data.write internal_state Idle
 *   | Set_head ->
 *       Stored_data.read chain_data.Chain_data.current_head
 *       >>= fun written_head ->
 *       (\* Hypothesis: the head is known in block_store *\)
 *       let head_is_known =
 *         Block_store.is_known block_store (Hash (written_head.hash, 0))
 *       in
 *       assert head_is_known ;
 *       (\* Removing alternate heads will make the store eventually consistent *\)
 *       Stored_data.write chain_data.alternate_heads Block_hash.Map.empty
 *   | Merging ->
 *       (\* Hypothesis: the store swapping is atomic, thus we can simply delete the temporary stores. *\)
 *       (\* The alternate heads might contain outdated branches but it
 *          will be purged at next checkpoint update. *\)
 *       Block_store.try_remove_temporary_stores block_store *)

module Chain = struct
  type nonrec chain_store = chain_store

  type t = chain_store

  let global_store {global_store; _} = global_store

  let chain_id chain_store = chain_store.chain_id

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
        return (Some {chain_state with savepoint = new_savepoint}, ()))

  let caboose chain_store = caboose chain_store

  let set_caboose chain_store new_caboose =
    Shared.update_with chain_store.chain_state (fun chain_state ->
        (* FIXME: potential data-race if called during a merge *)
        Stored_data.write chain_state.caboose_data new_caboose
        >>= fun () -> return (Some {chain_state with caboose = new_caboose}, ()))

  let current_head chain_store = current_head chain_store

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
          Chain_traversal.live_blocks chain_store ~block max_operations_ttl
          >>= fun live -> return live)

  let is_ancestor chain_store ~head:(hash, lvl) ~ancestor:(hash', lvl') =
    if Compare.Int32.(lvl' > lvl) then Lwt.return_false
    else if Compare.Int32.(lvl = lvl') then
      Lwt.return (Block_hash.equal hash hash')
    else
      Block.read_ancestor_hash
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
              (* It's an orphan new head : mark the previous head as
               alternate head *)
              Lwt.return alternate_heads
          | Some _prev_branch_level ->
              (* It's a known branch, update the alternate heads *)
              Lwt.return (Block_hash.Map.remove new_head_pred alternate_heads))

  let update_savepoint locked_chain_store chain_state history_mode
      ~min_level_to_preserve new_head =
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
            locked_chain_store.block_store.Block_store.cemented_store
        in
        if
          Compare.Int32.(
            snd chain_state.savepoint >= Block.level min_block_to_preserve)
        then return chain_state.savepoint
        else
          let table_len = Array.length table in
          (* If the offset is 0, the minimum block to preserve will be
             the savepoint. *)
          if offset <= 0 then return (Block.descriptor min_block_to_preserve)
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
                  failwith
                    "Store.trigger_merge: cannot retrieve the new savepoint \
                     hash"
              | Some savepoint ->
                  return (Block.descriptor savepoint) )

  (* Hypothesis: new_head.last_allowed_fork_level > prev_head.last_allowed_fork_level *)
  let trigger_merge chain_store chain_state ~prev_head_metadata ~new_head
      ~new_head_metadata =
    (* Lock the directories while the reordering store is happening *)
    lock_for_write chain_store.lockfile
    >>= fun () ->
    (* Prevent snapshots from accessing the store while a merge is occuring *)
    let history_mode = history_mode chain_store in
    (* Determine the interval of blocks to cement [from_block ; to_block] *)
    let prev_last_lafl = Block.last_allowed_fork_level prev_head_metadata in
    ( match get_highest_cemented_level chain_store with
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
            chain_store
            new_head
            (Int32.succ prev_last_lafl) )
        >>=? fun block -> return (Block.descriptor block)
    | Some level ->
        fail_unless
          Compare.Int32.(
            Block.last_allowed_fork_level prev_head_metadata = level)
          (Store_errors.Inconsistent_store_state
             (Format.asprintf
                "the most recent cemented block (%ld) is not the previous \
                 head's last allowed fork level (%ld)"
                level
                (Block.last_allowed_fork_level prev_head_metadata)))
        >>=? fun () ->
        (* Retrieve (pred_head.last_allowed_fork_level + 1) *)
        Block.locked_read_block_by_level
          chain_store
          new_head
          (Int32.succ level)
        >>=? fun from_block -> return (Block.descriptor from_block) )
    >>=? fun from_block ->
    (let new_head_lafl = Block.last_allowed_fork_level new_head_metadata in
     let distance =
       Int32.(to_int (sub (Block.level new_head) new_head_lafl))
     in
     Block.read_ancestor_hash chain_store ~distance (Block.hash new_head)
     >>= function
     | None ->
         fail
           (Store_errors.Inconsistent_store_state
              "cannot retrieve head's last allowed fork level hash")
     | Some hash ->
         return (hash, new_head_lafl))
    >>=? fun to_block ->
    (* Determine the updated values for: checkpoint, savepoint, caboose *)
    (* Checkpoint: *)
    let checkpoint =
      (* Only update the checkpoint when he falls behind the upper
         bound to cement *)
      if Compare.Int32.(snd chain_state.checkpoint < snd to_block) then
        to_block
      else chain_state.checkpoint
    in
    (* Try to preserve max_op_ttl predecessors of the upper-bound
       (along with their metadata) so we can build snapshot from it *)
    Block.read_block chain_store (fst to_block)
    >>=? fun to_block_b ->
    Block.get_block_metadata_opt chain_store to_block_b
    >>= (function
          | Some metadata ->
              let nb_blocks_to_preserve = Block.max_operations_ttl metadata in
              let min_level_to_preserve =
                Compare.Int32.max
                  (snd chain_state.caboose)
                  Int32.(sub (snd to_block) (of_int nb_blocks_to_preserve))
              in
              (* + 1, to_block + max_op_ttl(checkpoint) *)
              Lwt.return (succ nb_blocks_to_preserve, min_level_to_preserve)
          | None ->
              (* If no metadata is found, keep the blocks until the caboose *)
              let min_level_to_preserve = snd chain_state.caboose in
              (* + 1, it's a size *)
              let nb_blocks_to_preserve =
                Int32.(
                  to_int (succ (sub (snd to_block) min_level_to_preserve)))
              in
              Lwt.return (nb_blocks_to_preserve, min_level_to_preserve))
    (* let head_level = Block.level new_head in *)
    (* Lwt.return Int32.(to_int (sub (snd to_block) caboose_level))) *)
    >>= fun (nb_blocks_to_preserve, min_level_to_preserve) ->
    (* Savepoint: *)
    update_savepoint
      chain_store
      chain_state
      history_mode
      ~min_level_to_preserve
      new_head
    >>=? fun savepoint ->
    (* Caboose *)
    ( match history_mode with
    | Archive | Full _ ->
        (* caboose = genesis *)
        return chain_state.caboose
    | Rolling {offset} ->
        Block.locked_read_block_by_level
          chain_store
          new_head
          min_level_to_preserve
        >>=? fun min_block_to_preserve ->
        let table =
          Cemented_block_store.cemented_blocks_files
            chain_store.block_store.Block_store.cemented_store
        in
        if
          Compare.Int32.(
            snd chain_state.caboose >= Block.level min_block_to_preserve)
        then return chain_state.caboose
        else
          let table_len = Array.length table in
          if offset <= 0 then return (Block.descriptor min_block_to_preserve)
          else if table_len <= offset then
            (* When the first cycle is merged, we shift the genesis to
               its lower bound : cannot be the savepoint because it
               might not have metadata *)
            if table_len = 0 then return from_block
            else return chain_state.caboose
          else (* new lowest cemented block  *)
            return savepoint )
    >>=? fun caboose ->
    let finalizer () =
      (* Update the stored value after the merge *)
      Stored_data.write chain_state.checkpoint_data checkpoint
      >>= fun () ->
      Stored_data.write chain_state.savepoint_data savepoint
      >>= fun () ->
      Stored_data.write chain_state.caboose_data caboose
      >>= fun () -> may_unlock chain_store.lockfile
    in
    Block_store.merge_stores
      chain_store.block_store
      ~finalizer
      ~nb_blocks_to_preserve
      ~history_mode
      ~from_block
      ~to_block
      ()
    >>= fun () -> return (checkpoint, savepoint, caboose, to_block)

  let re_store (chain_store : t) ~head:new_head ~checkpoint:new_checkpoint
      ~savepoint:new_savepoint ~caboose:new_caboose =
    (* (\* If not provided, we assume that the checkpoint is the
     *  highest cemented block *\)
     * ( match checkpoint with
     * | Some c ->
     *     return c
     * | None -> (
     *   match get_highest_cemented_level chain_store with
     *   | Some l ->
     *       Block.read_block_by_level chain_store l
     *       >>=? fun b -> return (Block.hash b, l)
     *   | None ->
     *       failwith "Failed to find a suitable checkpoint" ) )
     * >>=? fun new_checkpoint ->
     * (\* If not provided, we assume that the savepoint is the
     *  successor of the new_checkpoint *\)
     * ( match savepoint with
     * | Some s ->
     *     return s
     * | None ->
     *     let l = Int32.add (snd new_checkpoint) 1l in
     *     Block.read_block_by_level chain_store l
     *     >>=? fun b -> return (Block.hash b, l) )
     * >>=? fun new_savepoint -> *)
    Shared.update_with
      chain_store.chain_state
      (fun ({current_head; _} as chain_state) ->
        Stored_data.write current_head new_head
        >>= fun () ->
        Stored_data.write chain_state.checkpoint_data new_checkpoint
        >>= fun () ->
        Stored_data.write chain_state.savepoint_data new_savepoint
        >>= fun () ->
        Stored_data.write chain_state.caboose_data new_caboose
        >>= fun () ->
        return
          ( Some
              {
                chain_state with
                checkpoint = new_checkpoint;
                savepoint = new_savepoint;
                caboose = new_caboose;
              },
            () ))
    >>=? fun () -> return_unit

  let set_head chain_store (new_head : Block.t) : Block.t tzresult Lwt.t =
    Shared.update_with
      chain_store.chain_state
      (fun ({current_head; savepoint; checkpoint; caboose; _} as chain_state)
           ->
        Stored_data.read current_head
        >>= fun prev_head ->
        Block.get_block_metadata chain_store new_head
        >>=? fun new_head_metadata ->
        Block.get_block_metadata chain_store prev_head
        >>=? fun prev_head_metadata ->
        (* If the new head is equal to the previous head *)
        if Block.equal prev_head new_head then
          (* Nothing to do *)
          return (None, prev_head)
        else
          (* First, check that we do not try to set a head below the
                 last allowed fork level *)
          fail_unless
            Compare.Int32.(
              Block.level new_head
              > Block.last_allowed_fork_level prev_head_metadata)
            (Store_errors.Invalid_head_switch
               {
                 minimum_allowed_level =
                   Int32.succ
                     (Block.last_allowed_fork_level prev_head_metadata);
                 given_head = Block.descriptor new_head;
               })
          >>=? fun () ->
          (* Acceptable head *)
          let new_head_last_allowed_fork_level =
            Block.last_allowed_fork_level new_head_metadata
          in
          let prev_head_last_allowed_fork_level =
            Block.last_allowed_fork_level prev_head_metadata
          in
          (* Should we trigger a merge ? i.e. has the last allowed
               fork level changed and do we have enough blocks ? *)
          let has_lafl_changed =
            Compare.Int32.(
              new_head_last_allowed_fork_level
              > prev_head_last_allowed_fork_level)
          in
          let has_necessary_blocks =
            Compare.Int32.(snd caboose <= prev_head_last_allowed_fork_level)
          in
          ( if has_lafl_changed && has_necessary_blocks then
            (* We must be sure that the previous merge is completed
               before starting a new merge *)
            Block_store.await_merging chain_store.block_store
            >>=? fun () ->
            let is_already_cemented =
              Option.unopt_map
                ~f:(fun highest_cemented_level ->
                  Compare.Int32.(
                    highest_cemented_level >= new_head_last_allowed_fork_level))
                ~default:false
                (get_highest_cemented_level chain_store)
            in
            if is_already_cemented then return (checkpoint, savepoint, caboose)
            else
              trigger_merge
                chain_store
                chain_state
                ~prev_head_metadata
                ~new_head
                ~new_head_metadata
              >>=? fun (checkpoint, savepoint, caboose, to_block) ->
              (* Filter alternate heads that are below the new highest cemented block *)
              update_alternate_heads
                chain_store
                ~filter_below:(snd to_block)
                chain_state.alternate_heads
                ~prev_head
                ~new_head
              >>= fun () -> return (checkpoint, savepoint, caboose)
          else
            update_alternate_heads
              chain_store
              chain_state.alternate_heads
              ~prev_head
              ~new_head
            >>= fun () -> return (checkpoint, savepoint, caboose) )
          >>=? fun (checkpoint, savepoint, caboose) ->
          (* Update current_head and live_{blocks,ops} *)
          Stored_data.write current_head new_head
          >>= fun () ->
          let new_head_max_op_ttl =
            Block.max_operations_ttl new_head_metadata
          in
          (* Updating live blocks *)
          Chain_traversal.live_blocks
            chain_store
            ~block:new_head
            new_head_max_op_ttl
          >>= fun (live_blocks, live_operations) ->
          return
            ( Some
                {
                  chain_state with
                  live_blocks;
                  live_operations;
                  checkpoint;
                  savepoint;
                  caboose;
                },
              prev_head ))

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

  let is_valid_for_checkpoint chain_store
      ~checkpoint:(current_checkpoint_hash, current_checkpoint_level)
      (given_checkpoint_hash, given_checkpoint_level) =
    if Compare.Int32.(given_checkpoint_level < current_checkpoint_level) then
      Lwt.return_false
    else
      read_ancestor_hash
        chain_store
        ~distance:
          Int32.(to_int (sub given_checkpoint_level current_checkpoint_level))
        given_checkpoint_hash
      >>= function
      | None -> (
          (* <=> checkpoint not found or no ancestor found =>
             the checkpoint must be in the future and therefore not yet known *)
          Block.is_known chain_store given_checkpoint_hash
          >>= function true -> Lwt.return false | false -> Lwt.return_true )
      | Some ancestor ->
          Lwt.return (Block_hash.equal ancestor current_checkpoint_hash)

  let set_checkpoint chain_store new_checkpoint =
    Shared.use chain_store.chain_state (fun {checkpoint; _} ->
        is_valid_for_checkpoint chain_store ~checkpoint new_checkpoint
        >>= function
        | false ->
            failwith
              "Store.set_checkpoint: cannot set a checkpoint below the \
               current one"
        | true ->
            return_unit)
    >>=? fun () ->
    Shared.update_with chain_store.chain_state (fun chain_state ->
        (* FIXME: potential data-race if called during a merge *)
        Stored_data.write chain_state.checkpoint_data new_checkpoint
        >>= fun () ->
        return (Some {chain_state with checkpoint = new_checkpoint}, ()))

  let is_acceptable_block chain_store block_descr =
    Shared.use chain_store.chain_state (fun chain_state ->
        locked_is_acceptable_block chain_store chain_state block_descr)

  let best_known_head_for_checkpoint chain_store ~checkpoint =
    let (_, checkpoint_level) = checkpoint in
    current_head chain_store
    >>= fun current_head ->
    is_valid_for_checkpoint
      chain_store
      ~checkpoint
      (Block.hash current_head, Block.level current_head)
    >>= fun valid ->
    if valid then Lwt.return current_head
    else
      let find_valid_predecessor hash =
        (* TODO : remove assert *)
        Block.read_block_opt chain_store hash
        >|= Option.unopt_assert ~loc:__POS__
        >>= fun block ->
        if Compare.Int32.(Block_repr.level block < checkpoint_level) then
          Lwt.return block
        else
          Block.read_block_opt
            chain_store
            hash
            ~distance:
              ( 1
              + ( Int32.to_int
                @@ Int32.sub (Block_repr.level block) checkpoint_level ) )
          >>= fun pred ->
          let pred = Option.unopt_assert ~loc:__POS__ pred in
          Lwt.return pred
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
          >>= fun best ->
          valid_predecessor_t
          >>= fun pred ->
          if Fitness.(Block.fitness pred > Block.fitness best) then
            Lwt.return pred
          else Lwt.return best)
        heads
        (Lwt.return best)

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
              commit_info = Some genesis_commit_info;
            }
            empty)
    >>= fun protocol_levels ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.genesis)
      Block_repr.encoding
      ~initial_data:genesis_block
    >>= fun genesis ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.current_head)
      Block_repr.encoding
      ~initial_data:genesis_block
    >>= fun current_head ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.alternate_heads)
      (Block_hash.Map.encoding Data_encoding.int32)
      ~initial_data:Block_hash.Map.empty
    >>= fun alternate_heads ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.checkpoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      ~initial_data:(genesis_block.hash, genesis_level)
    >>= fun checkpoint_data ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.savepoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
      ~initial_data:(genesis_block.hash, genesis_level)
    >>= fun savepoint_data ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.caboose)
      Data_encoding.(tup2 Block_hash.encoding int32)
      ~initial_data:(genesis_block.hash, genesis_level)
    >>= fun caboose_data ->
    Stored_data.init
      ~file:Naming.(chain_dir // Naming.Chain_data.invalid_blocks)
      (Block_hash.Map.encoding invalid_block_encoding)
      ~initial_data:Block_hash.Map.empty
    >>= fun invalid_blocks ->
    Stored_data.init
      ~file:Naming.(chain_dir // Chain_data.forked_chains)
      (Chain_id.Map.encoding Block_hash.encoding)
      ~initial_data:Chain_id.Map.empty
    >>= fun forked_chains ->
    Stored_data.read checkpoint_data
    >>= fun checkpoint ->
    Stored_data.read savepoint_data
    >>= fun savepoint ->
    Stored_data.read caboose_data
    >>= fun caboose ->
    let active_testchain = None in
    let mempool = Mempool.empty in
    let live_blocks = Block_hash.Set.empty in
    let live_operations = Operation_hash.Set.empty in
    Lwt.return
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
        active_testchain;
        mempool;
        live_blocks;
        live_operations;
      }

  (* Files are expected to be present *)
  let load_chain_state ~chain_dir =
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.protocol_levels)
      Protocol_levels.encoding
    >>= fun protocol_levels ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.genesis)
      Block_repr.encoding
    >>= fun genesis ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.current_head)
      Block_repr.encoding
    >>= fun current_head ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.alternate_heads)
      (Block_hash.Map.encoding Data_encoding.int32)
    >>= fun alternate_heads ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.checkpoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
    >>= fun checkpoint_data ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.savepoint)
      Data_encoding.(tup2 Block_hash.encoding int32)
    >>= fun savepoint_data ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.caboose)
      Data_encoding.(tup2 Block_hash.encoding int32)
    >>= fun caboose_data ->
    Stored_data.load
      ~file:Naming.(chain_dir // Chain_data.invalid_blocks)
      (Block_hash.Map.encoding invalid_block_encoding)
    >>= fun invalid_blocks ->
    Stored_data.load
      ~file:Naming.(chain_dir // Naming.Chain_data.forked_chains)
      (Chain_id.Map.encoding Block_hash.encoding)
    >>= fun forked_chains ->
    Stored_data.read checkpoint_data
    >>= fun checkpoint ->
    Stored_data.read savepoint_data
    >>= fun savepoint ->
    Stored_data.read caboose_data
    >>= fun caboose ->
    let active_testchain = None in
    let mempool = Mempool.empty in
    let live_blocks = Block_hash.Set.empty in
    let live_operations = Operation_hash.Set.empty in
    Lwt.return
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
        active_testchain;
        mempool;
        live_blocks;
        live_operations;
      }

  let get_commit_info index header =
    protect
      ~on_error:(fun _ ->
        failwith
          "get_commit_info: could not retrieve the commit info. Is the \
           context initialized?")
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
    get_commit_info global_store.context_index (Block.header genesis_block)
    >>=? fun genesis_commit_info ->
    create_chain_state
      ~chain_dir
      ~genesis_block
      ~genesis_protocol:genesis.Genesis.protocol
      ~genesis_commit_info
    >>= fun chain_state ->
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
    >>= fun chain_state ->
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
        Lwt.fail_with
          "Store.load_chain_store: could not retrieve head metadata"
    | Some metadata ->
        let max_op_ttl = Block.max_operations_ttl metadata in
        Chain_traversal.live_blocks chain_store ~block:head max_op_ttl
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
              >>=? fun () ->
              ( match active_testchain with
              | Some {testchain_store; _} ->
                  loop testchain_store
              | None ->
                  return_unit )
              >>=? fun () ->
              may_unlock chain_store.lockfile
              >>= fun () ->
              Lwt_utils_unix.safe_close lockfile >>= fun () -> return_unit)
    in
    loop chain_store

  (* Test chain *)

  let testchain_status chain_store block =
    Block.context_exn chain_store block
    >>= fun context ->
    Context.get_test_chain context
    >>= fun status ->
    match status with
    | Running {genesis; _} ->
        Lwt.return (status, Some genesis)
    | Forking _ ->
        Lwt.return (status, Some (Block.hash block))
    | Not_running ->
        Lwt.return (status, None)

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
    (* TODO replace with error *)
    assert chain_store.global_store.allow_testchains ;
    Shared.update_with
      chain_store.chain_state
      (fun ({active_testchain; _} as chain_state) ->
        match active_testchain with
        | Some ({testchain_store; forked_block} as testchain) ->
            (* Already forked and active *)
            if Chain_id.equal testchain_store.chain_id testchain_id then (
              assert (Block_hash.equal forked_block forked_block_hash) ;
              return (None, testchain) )
            else
              Lwt.fail_with "Store.fork_testchain: test chain already exists"
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
                  failwith "Store.load_testchain: cannot find test chain"
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
              return
                ( Some {chain_state with active_testchain = Some testchain},
                  testchain ))

  (* Look for chain_store's testchains - does not look recursively *)
  let load_testchain chain_store ~chain_id =
    Shared.use chain_store.chain_state (fun {active_testchain; _} ->
        locked_load_testchain chain_store ~chain_id active_testchain)

  let shutdown_testchain chain_store =
    Shared.update_with
      chain_store.chain_state
      (fun ({active_testchain; _} as chain_state) ->
        match active_testchain with
        | Some testchain ->
            close_chain_store testchain.testchain_store
            >>=? fun () ->
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

  let set_protocol_level chain_store protocol_level (block, protocol_hash) =
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

  let find_activation_block chain_store ~proto_level =
    Shared.use chain_store.chain_state (fun {protocol_levels; _} ->
        Stored_data.read protocol_levels
        >>= fun protocol_levels ->
        Lwt.return (Protocol_levels.find_opt proto_level protocol_levels))

  let find_protocol chain_store ~proto_level =
    find_activation_block chain_store ~proto_level
    >>= function
    | None ->
        Lwt.return_none
    | Some {Protocol_levels.protocol; _} ->
        Lwt.return_some protocol

  let may_update_protocol_level chain_store proto_level (block, protocol_hash)
      =
    find_activation_block chain_store ~proto_level
    >>= function
    | Some _ ->
        return_unit
    | None ->
        set_protocol_level chain_store proto_level (block, protocol_hash)

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
            ~proto_level:(Block.proto_level pred)
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
  let all_stored_protocols {protocol_store; _} =
    Protocol_store.all protocol_store

  let store_protocol {protocol_store; protocol_watcher; _} protocol_hash
      protocol =
    Protocol_store.store protocol_store protocol_hash protocol
    >>= function
    | None ->
        Lwt.return_none
    | p ->
        Lwt_watcher.notify protocol_watcher protocol_hash ;
        Lwt.return p

  let store_raw_protocol {protocol_store; protocol_watcher; _} protocol_hash
      raw_protocol =
    Protocol_store.raw_store protocol_store protocol_hash raw_protocol
    >>= function
    | None ->
        Lwt.return_none
    | p ->
        Lwt_watcher.notify protocol_watcher protocol_hash ;
        Lwt.return p

  let read_protocol {protocol_store; _} protocol_hash =
    Protocol_store.read protocol_store protocol_hash

  let is_protocol_stored {protocol_store; _} protocol_hash =
    Protocol_store.mem protocol_store protocol_hash

  let protocol_watcher {protocol_watcher; _} =
    Lwt_watcher.create_stream protocol_watcher
end

(* let get_cemented chain_store cemented_cycles genesis_block =
 *   let nb_cycles = Array.length cemented_cycles in
 *   if nb_cycles = 0 then
 *     let genesis =
 *       (Block_repr.hash genesis_block, Block_repr.level genesis_block)
 *     in
 *     Lwt.return (genesis, genesis)
 *   else (
 *     Array.sort
 *       Cemented_block_store.(
 *         fun {start_level; _} {start_level = start_level'; _} ->
 *           compare start_level start_level')
 *       cemented_cycles ;
 *     let {Cemented_block_store.end_level = highest_cemented_level; _} =
 *       cemented_cycles.(nb_cycles - 1)
 *     in
 *     Block.read_block_by_level_opt chain_store highest_cemented_level
 *     >>= (function None -> assert false | Some b -> Lwt.return b)
 *     >>= fun highest_cemented_block ->
 *     let {Cemented_block_store.start_level = lowest_cemented_level; _} =
 *       cemented_cycles.(0)
 *     in
 *     Block.read_block_by_level_opt chain_store lowest_cemented_level
 *     >>= (function None -> assert false | Some b -> Lwt.return b)
 *     >>= fun lowest_cemented_block ->
 *     Lwt.return
 *       ( (Block.hash lowest_cemented_block, lowest_cemented_level),
 *         (Block.hash highest_cemented_block, highest_cemented_level) ) ) *)

(* (\* TODO: infer protocol table ?*\)
 * let infer_chain_data ~store_dir chain_id ~chain_store genesis_block
 *     history_mode =
 *   let chain_dir = Naming.(store_dir // chain_store chain_id) in
 *   let cemented_blocks_dir =
 *     Naming.(store_dir // chain_store chain_id // cemented_blocks_directory)
 *   in
 *   let floating_store = chain_store.block_store.rw_floating_block_store in
 *   Cemented_block_store.load_table ~cemented_blocks_dir
 *   >>= fun cemented_cycles ->
 *   get_cemented chain_store cemented_cycles genesis_block
 *   >>= fun (lowest_cemented, highest_cemented) ->
 *   Cemented_block_store.load ~cemented_blocks_dir
 *   >>= fun cemented_store ->
 *   (\* Works if only one branch is available in floating_block_store*\)
 *   Floating_block_store.fold
 *     (fun block (((_, min_level) as min), ((_, max_level) as max)) ->
 *       let level = Block_repr.level block in
 *       let hash = Block_repr.hash block in
 *       let new_min = if level < min_level then (hash, level) else min in
 *       let new_max = if level > max_level then (hash, level) else max in
 *       Lwt.return (new_min, new_max))
 *     ((Block_hash.zero, Int32.max_int), (Block_hash.zero, 0l))
 *     floating_store
 *   >>= fun ( (lowest_floating_hash, lowest_floating_level),
 *             (highest_floating_hash, _highest_floating_level) ) ->
 *   let caboose =
 *     match history_mode with
 *     | History_mode.Rolling _ ->
 *         (lowest_floating_hash, lowest_floating_level)
 *     | _ ->
 *         lowest_cemented
 *   in
 *   let checkpoint =
 *     match history_mode with
 *     | History_mode.Rolling _ ->
 *         let level = Int32.sub lowest_floating_level 1l in
 *         let hash =
 *           Option.unopt_assert
 *             ~loc:__POS__
 *             (Cemented_block_store.get_cemented_block_hash cemented_store level)
 *         in
 *         (hash, level)
 *     | _ ->
 *         highest_cemented
 *   in
 *   let savepoint =
 *     if history_mode = History_mode.Archive then caboose
 *     else
 *       let (h, l) = checkpoint in
 *       (h, Int32.add 1l l)
 *   in
 *   Block.read_block_opt chain_store highest_floating_hash
 *   >>= (function None -> assert false | Some b -> Lwt.return b)
 *   >>= fun current_head ->
 *   (\* Stored_data.init
 *    *   ~file:Naming.(chain_dir // Chain_data.protocol_levels)
 *    *   Protocol_levels.encoding
 *    *   ~initial_data:
 *    *     Protocol_levels.(
 *    *       add
 *    *         genesis_header.Block_header.shell.proto_level
 *    *         ( (genesis_block.hash, genesis_header.Block_header.shell.level),
 *    *           genesis_protocol )
 *    *         empty)
 *    * >>= fun _protocol_levels -> *\)
 *   Stored_data.init
 *     ~file:Naming.(chain_dir // Chain_data.current_head)
 *     Block_repr.encoding
 *     ~initial_data:current_head
 *   >>= fun _current_head ->
 *   Stored_data.init
 *     ~file:Naming.(chain_dir // Chain_data.alternate_heads)
 *     (Block_hash.Map.encoding Data_encoding.int32)
 *     ~initial_data:Block_hash.Map.empty
 *   >>= fun _alternate_heads ->
 *   Stored_data.init
 *     ~file:Naming.(chain_dir // Chain_data.checkpoint)
 *     Data_encoding.(tup2 Block_hash.encoding int32)
 *     ~initial_data:checkpoint
 *   >>= fun _checkpoint ->
 *   Stored_data.init
 *     ~file:Naming.(chain_dir // Chain_data.savepoint)
 *     Data_encoding.(tup2 Block_hash.encoding int32)
 *     ~initial_data:savepoint
 *   >>= fun _savepoint ->
 *   Stored_data.init
 *     ~file:Naming.(chain_dir // Chain_data.caboose)
 *     Data_encoding.(tup2 Block_hash.encoding int32)
 *     ~initial_data:caboose
 *   >>= fun _caboose ->
 *   Stored_data.init
 *     ~file:Naming.(chain_dir // Naming.Chain_data.invalid_blocks)
 *     (Block_hash.Map.encoding invalid_block_encoding)
 *     ~initial_data:Block_hash.Map.empty
 *   >>= fun _invalid_blocks ->
 *   Stored_data.init
 *     ~file:Naming.(chain_dir // Chain_data.forked_chains)
 *     (Chain_id.Map.encoding Block_hash.encoding)
 *     ~initial_data:Chain_id.Map.empty
 *   >>= fun _forked_chains ->
 *   (\* Stored_data.write chain_store.chain_data.protocol_levels protocol_levels
 *    * >>= fun () -> *\)
 *   Shared.locked_use chain_store.chain_state (fun chain_state ->
 *       Stored_data.write chain_state.current_head current_head
 *       >>= fun () ->
 *       Stored_data.write chain_state.alternate_heads Block_hash.Map.empty
 *       >>= fun () ->
 *       Stored_data.write chain_state.checkpoint_data checkpoint
 *       >>= fun () ->
 *       Stored_data.write chain_state.savepoint_data savepoint
 *       >>= fun () ->
 *       Stored_data.write chain_state.caboose_data caboose
 *       >>= fun () ->
 *       Stored_data.write chain_state.invalid_blocks Block_hash.Map.empty
 *       >>= fun () ->
 *       Stored_data.write chain_state.forked_chains Chain_id.Map.empty
 *       >>= fun () -> Lwt.return_unit) *)

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

let load_store ?history_mode ~store_dir ~context_index ~chain_id
    ~allow_testchains ~readonly () =
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
      return_unit
  | Some history_mode ->
      let previous_history_mode = Chain.history_mode main_chain_store in
      fail_when
        (history_mode <> previous_history_mode)
        (Store_errors.Incorrect_history_mode_switch
           {previous_mode = previous_history_mode; next_mode = history_mode})
  )
  >>=? fun () ->
  global_store.main_chain_store <- Some main_chain_store ;
  return global_store

let main_chain_store store =
  Option.unopt_assert ~loc:__POS__ store.main_chain_store

let init ?patch_context ?commit_genesis ?history_mode ?(readonly = false)
    ~store_dir ~context_dir ~allow_testchains genesis =
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
  if Sys.file_exists chain_dir && Sys.is_directory chain_dir then
    load_store
      ?history_mode
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
  >>=? fun () ->
  Context.close global_store.context_index >>= fun () -> return_unit

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
    ()
  >>=? fun store ->
  let chain_store = main_chain_store store in
  lock_for_read chain_store.lockfile
  >>= fun () ->
  Error_monad.protect
    ~on_error:(fun err ->
      close_store store >>=? fun () -> Lwt.return (Error err))
    (fun () ->
      locked_f (chain_store, context_index)
      >>=? fun res -> close_store store >>=? fun () -> return res)

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
          (* FIXME what happens when cemented ? *)
          failwith
            "Store.restore_from_snapshot: could not find the caboose for \
             rolling"
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
            else return (Block_repr.hash caboose, Block_repr.level caboose) ) )
  )
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
  (* Check correctness of protocol transition blocks *)
  let open Protocol_levels in
  iter_s
    (fun (_, {block = (bh, _); protocol; commit_info = commit_info_opt}) ->
      Block_store.read_block block_store ~read_metadata:false (Hash (bh, 0))
      >>= fun block_opt ->
      match (block_opt, commit_info_opt) with
      | (None, _) ->
          (* Ignore unknown blocks *)
          return_unit
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
            (is_consistent || Compare.Int32.(equal (Block_repr.level block) 0l))
            (Exn
               (Failure
                  (Format.asprintf
                     "restore_from_snapshot: Inconsistent commit hash found \
                      for transition block %a activating protocol %a"
                     Block_hash.pp
                     (Block.hash block)
                     Protocol_hash.pp
                     protocol))))
    (Protocol_levels.bindings protocol_levels)
  >>=? fun () ->
  Block_store.close block_store
  >>=? fun () ->
  let chain_config = {Chain_config.history_mode; genesis; expiration = None} in
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
      return (Block_repr.hash genesis_block, Block_repr.level genesis_block)
  | Rolling _ -> (
      Lwt_stream.peek floating_blocks_stream
      >>= function
      | None ->
          (* FIXME what happens when cemented ? *)
          failwith
            "Store.restore_from_snapshot: could not find the caboose for \
             rolling"
      | Some caboose ->
          return (Block_repr.hash caboose, Block_repr.level caboose) ) )
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
  (* Compute correct protocol levels and check their correctness *)
  fold_left_s
    (fun proto_levels (transition_level, protocol_hash, commit_info_opt) ->
      let distance =
        Int32.(
          to_int
            (sub (Block_repr.level new_head_with_metadata) transition_level))
      in
      (* FIXME? what happens when the snapshot does not contain the block ? (i.e. rolling) *)
      Block_store.read_block
        block_store
        ~read_metadata:false
        (Hash (Block_repr.hash new_head_with_metadata, distance))
      >>= fun block_opt ->
      match (block_opt, commit_info_opt) with
      | (None, _) ->
          (* Ignore unknown blocks *)
          return proto_levels
      | (Some block, None) ->
          (* TODO no commit info : raise a warning *)
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
          if is_consistent || Compare.Int32.(equal (Block_repr.level block) 0l)
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
            failwith
              "restore_from_snapshot: Inconsistent commit hash found for \
               transition block %a activating protocol %a"
              Block_hash.pp
              (Block.hash block)
              Protocol_hash.pp
              protocol_hash)
    initial_protocol_levels
    partial_protocol_levels
  >>=? fun protocol_levels ->
  Stored_data.write_file
    ~file:Naming.(chain_dir // Chain_data.protocol_levels)
    Protocol_levels.encoding
    protocol_levels
  >>= fun () ->
  Block_store.close block_store
  >>=? fun () ->
  let chain_config = {Chain_config.history_mode; genesis; expiration = None} in
  Chain_config.write ~chain_dir chain_config >>=? fun () -> return_unit

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

let cement_blocks_chunk chain_store blocks ~write_metadata =
  Block_store.cement_blocks ~write_metadata chain_store.block_store blocks

(****************** For testing purposes only *****************)

let unsafe_get_block_store chain_store = chain_store.block_store
