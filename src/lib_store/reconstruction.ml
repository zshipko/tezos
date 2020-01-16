(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2019 Nomadic Labs, <contact@nomadic-labs.com>               *)
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

open Reconstruction_errors

let check_context_hash_consistency block_validation_result block_header =
  fail_unless
    (Context_hash.equal
       block_validation_result.Block_validation.context_hash
       block_header.Block_header.shell.context)
    (Reconstruction_failure "resulting context hash does not match")

(* Reconstruct the storage (without checking if the context/store is already populated) *)
let reconstruct_storage _store_root chain_store context_index chain_id
    ~user_activated_upgrades ~user_activated_protocol_overrides genesis_block
    (history_list : Block_hash.t list) =
  let history = Array.of_list history_list in
  let limit = Array.length history in
  let block_store = Store.unsafe_get_block_store chain_store in
  Cemented_block_store.load_table
    ~cemented_blocks_dir:block_store.cemented_store.cemented_blocks_dir
  >>= (function v -> Lwt.return (Array.to_list v))
  >>= fun cemented_cycles ->
  let find block =
    List.find_opt
      (fun ({end_level; _} : Cemented_block_store.cemented_blocks_file) ->
        Store.Block.level block = end_level)
      cemented_cycles
  in
  let exist block = match find block with None -> false | Some _ -> true in
  let rec reconstruct_chunks level lafl metadata_chunk =
    Tezos_stdlib_unix.Utils.display_progress
      "Reconstructing contexts: %i/%i"
      level
      limit ;
    if level = limit then return_unit
    else
      let block_hash = history.(level) in
      Store.Block.read_block chain_store block_hash
      >>=? fun block ->
      let block_header = Store.Block.header block in
      let operations = Store.Block.operations block in
      Store.Block.read_predecessor chain_store block
      >>=? fun pred_block ->
      let predecessor_block_header = Store.Block.header pred_block in
      Context.checkout_exn context_index predecessor_block_header.shell.context
      >>= fun predecessor_context ->
      Block_validation.apply
        chain_id
        ~user_activated_upgrades
        ~user_activated_protocol_overrides
        ~max_operations_ttl:(Int32.to_int (Store.Block.level pred_block))
        ~predecessor_block_header
        ~predecessor_context
        ~block_header
        operations
      >>=? fun block_validation_result ->
      check_context_hash_consistency
        block_validation_result.validation_store
        block_header
      >>=? fun () ->
      let ({validation_store; block_metadata; ops_metadata; _}
            : Block_validation.result) =
        block_validation_result
      in
      let metadata =
        {
          Store.Block.message = validation_store.message;
          max_operations_ttl = validation_store.max_operations_ttl;
          last_allowed_fork_level = validation_store.last_allowed_fork_level;
          block_metadata;
          operations_metadata = ops_metadata;
        }
      in
      let new_metadata_chunk = (block, metadata) :: metadata_chunk in
      ( if lafl < validation_store.last_allowed_fork_level then
        (* New cycle : store the chunck and continue with an empty one*)
        Store.Block.store_block_metadata
          chain_store
          (List.rev new_metadata_chunk)
        >>= fun () ->
        reconstruct_chunks
          (level + 1)
          validation_store.last_allowed_fork_level
          []
      else if
      (* For the first cycles, the last allowed fork level is not updated.
         We must rely on the previously stored cycles.*)
      lafl = 0l && exist block
    then
        Store.Block.store_block_metadata
          chain_store
          (List.rev new_metadata_chunk)
        >>= fun () ->
        reconstruct_chunks
          (level + 1)
          validation_store.last_allowed_fork_level
          []
      else
        reconstruct_chunks
          (level + 1)
          validation_store.last_allowed_fork_level
          new_metadata_chunk )
      >>=? fun () -> return_unit
  in
  Store.Chain.genesis_block chain_store
  >|= Store.Block.hash
  >>= fun genesis_hash ->
  Store.Block.read_block chain_store genesis_hash
  >>=? fun genesis_b ->
  Store.Block.get_block_metadata chain_store genesis_b
  >>=? fun genesis_metadata ->
  reconstruct_chunks 0 0l [(genesis_b, genesis_metadata)]
  >>=? fun () ->
  Store.Chain.set_history_mode chain_store History_mode.Archive
  >>= fun () ->
  Tezos_stdlib_unix.Utils.display_progress_end () ;
  Store.Chain.current_head chain_store
  >>= fun new_head ->
  Store.Chain.checkpoint chain_store
  >>= fun checkpoint ->
  Store.Chain.re_store
    chain_store
    ~head:new_head
    ~checkpoint
    ~savepoint:(genesis_block, 0l)
    ~caboose:(genesis_block, 0l)
  >>=? fun () -> return_unit

let reconstruct ?patch_context ~store_root ~context_root ~(genesis : Genesis.t)
    ~user_activated_upgrades ~user_activated_protocol_overrides =
  let chain_id = Chain_id.of_block_hash genesis.block in
  Store.init
    ?patch_context
    ~store_dir:store_root
    ~context_dir:context_root
    ~allow_testchains:false
    ~history_mode:History_mode.(Full {offset = default_offset})
    genesis
  >>=? fun store ->
  let context_index = Store.context_index store in
  let chain_store = Store.main_chain_store store in
  let history_mode = Store.Chain.history_mode chain_store in
  fail_unless
    (history_mode = History_mode.(Full {offset = default_offset}))
    (Cannot_reconstruct history_mode)
  >>=? fun () ->
  let low_limit = 1l in
  lwt_emit Reconstruct_start_default
  >>= fun () ->
  Store.Chain.savepoint chain_store
  >>= fun (savepoint_hash, savepoint_level) ->
  fail_when
    (savepoint_level = 0l)
    (Reconstruction_failure "nothing to reconstruct")
  >>=? fun () ->
  lwt_emit (Reconstruct_end_default savepoint_hash)
  >>= fun () ->
  Store.Block.read_block chain_store savepoint_hash
  >>=? fun savepoint_block ->
  let high_limit = Store.Block.predecessor savepoint_block in
  lwt_emit Reconstruct_enum
  >>= fun () ->
  let rec gather_history low_limit block_hash acc =
    Store.Block.read_block_opt chain_store block_hash
    >>= function
    | Some block ->
        if Store.Block.level block = low_limit then return (block_hash :: acc)
        else
          gather_history
            low_limit
            (Store.Block.predecessor block)
            (block_hash :: acc)
    | None ->
        failwith
          "Unexpected missing block in store: %a"
          Block_hash.pp
          block_hash
  in
  gather_history low_limit high_limit []
  >>=? fun hash_history ->
  reconstruct_storage
    store_root
    chain_store
    context_index
    chain_id
    ~user_activated_upgrades
    ~user_activated_protocol_overrides
    genesis.block
    hash_history
  >>=? fun () -> lwt_emit Reconstruct_success >>= fun () -> return_unit

(* TODO: Solve these corner cases !
   Import a snapshot and ask for a reconstruct :
          - no blocks cemented
          - some blocks cemented
 *)
