(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
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

open Test_utils

let test_export_full (store_dir, context_dir) store =
  let chain_store = Store.main_chain_store store in
  Store.Chain.genesis_block chain_store
  >>= fun genesis_block ->
  Alpha_utils.bake_n chain_store 777 genesis_block
  >>=? fun (blocks, last) ->
  let snapshot_dir = store_dir // "snapshot.full" in
  (* No lockfile on the same process, we must enforce waiting the
     merging thread to finish *)
  let block_store = Store.unsafe_get_block_store chain_store in
  Block_store.await_merging block_store
  >>= fun () ->
  Snapshots.export
    ~rolling:false
    ~store_dir
    ~context_dir
    ~chain_name:(Distributed_db_version.Name.of_string "test")
    ~block:(Some (Block_hash.to_b58check (Store.Block.hash last)))
    ~snapshot_dir
    genesis
  >>=? fun () ->
  let dir = store_dir // "imported_store" in
  let dst_store_dir = dir // "store" in
  let dst_context_dir = dir // "context" in
  Snapshots.import
    ~block:(Block_hash.to_b58check (Store.Block.hash last))
    ~snapshot_dir
    ~dst_store_dir
    ~dst_context_dir
    ~user_activated_upgrades:[]
    ~user_activated_protocol_overrides:[]
    genesis
  >>=? fun () ->
  Store.init
    ~store_dir:dst_store_dir
    ~context_dir:dst_context_dir
    ~allow_testchains:true
    genesis
  >>=? fun store ->
  let main_chain_store = Store.main_chain_store store in
  check_invariants main_chain_store
  >>= fun () ->
  assert_presence_in_store
    ~with_metadata:false
    main_chain_store
    (List.rev (List.tl (List.rev blocks)))
  >>=? fun () ->
  Store.Chain.current_head main_chain_store
  >>= fun head ->
  (* Make sure the new head is the chosen exported block *)
  Assert.equal
    ~prn:(Format.asprintf "%a" Test_utils.pp_block)
    ~eq:Store.Block.equal
    last
    head ;
  Alpha_utils.bake_until_n_cycle_end main_chain_store 10 head
  >>=? fun (blocks, _) ->
  assert_presence_in_store ~with_metadata:false main_chain_store blocks
  >>=? fun () ->
  check_invariants main_chain_store
  >>= fun () -> Store.close_store store >>= fun () -> return_unit

let test_export_rolling (store_dir, context_dir) store =
  let chain_store = Store.main_chain_store store in
  Store.Chain.genesis_block chain_store
  >>= fun genesis_block ->
  Alpha_utils.bake_n chain_store 777 genesis_block
  >>=? fun (blocks, last) ->
  let snapshot_dir = store_dir // "snapshot.rolling" in
  (* No lockfile on the same process, we must enforce waiting the
     merging thread to finish *)
  let block_store = Store.unsafe_get_block_store chain_store in
  Block_store.await_merging block_store
  >>= fun () ->
  Snapshots.export
    ~rolling:true
    ~store_dir
    ~context_dir
    ~chain_name:(Distributed_db_version.Name.of_string "test")
    ~block:(Some (Block_hash.to_b58check (Store.Block.hash last)))
    ~snapshot_dir
    genesis
  >>=? fun () ->
  let dir = store_dir // "imported_store" in
  let dst_store_dir = dir // "store" in
  let dst_context_dir = dir // "context" in
  Snapshots.import
    ~block:(Block_hash.to_b58check (Store.Block.hash last))
    ~snapshot_dir
    ~dst_store_dir
    ~dst_context_dir
    ~user_activated_upgrades:[]
    ~user_activated_protocol_overrides:[]
    genesis
  >>=? fun () ->
  Store.init
    ~store_dir:dst_store_dir
    ~context_dir:dst_context_dir
    ~allow_testchains:true
    genesis
  >>=? fun store ->
  let main_chain_store = Store.main_chain_store store in
  check_invariants main_chain_store
  >>= fun () ->
  let history_mode = Store.Chain.history_mode main_chain_store in
  assert (match history_mode with Rolling _ -> true | _ -> false) ;
  Store.Chain.current_head main_chain_store
  >>= fun head ->
  Store.Block.get_block_metadata main_chain_store head
  >>=? fun metadata ->
  let max_op_ttl = Store.Block.max_operations_ttl metadata in
  let expected_caboose_level =
    Int32.(sub (Store.Block.level head) (of_int max_op_ttl))
  in
  Store.Chain.caboose main_chain_store
  >>= fun (_, caboose_level) ->
  Assert.equal
    ~eq:Int32.equal
    ~prn:Int32.to_string
    expected_caboose_level
    caboose_level ;
  let blocks =
    List.filter
      (fun b -> Compare.Int32.(Store.Block.level b >= caboose_level))
      blocks
  in
  assert_presence_in_store
    ~with_metadata:false
    main_chain_store
    (List.rev (List.tl (List.rev blocks)))
  >>=? fun () ->
  (* Make sure the new head is the chosen exported block *)
  Assert.equal
    ~prn:(Format.asprintf "%a" Test_utils.pp_block)
    ~eq:Store.Block.equal
    last
    head ;
  Alpha_utils.bake_until_n_cycle_end main_chain_store 10 head
  >>=? fun (blocks, _) ->
  Store.Chain.caboose main_chain_store
  >>= fun (_, caboose_level) ->
  let present_blocks =
    List.filter
      (fun b -> Compare.Int32.(Store.Block.level b >= caboose_level))
      blocks
  in
  assert_presence_in_store ~with_metadata:false main_chain_store present_blocks
  >>=? fun () ->
  check_invariants main_chain_store
  >>= fun () -> Store.close_store store >>= fun () -> return_unit

(* TODO:

   - While continuing baking
   - Rolling (+ while continuing baking => hard(?), lock file is useless)

*)

let tests =
  let test_cases =
    List.map
      (wrap_test ~patch_context:(fun ctxt -> Alpha_utils.patch_context ctxt))
      [ ("export full", test_export_full);
        ("export rolling", test_export_rolling)
        (* ("export_checkpoint", test_export_checkpoint);
         * ("export_from_cemented", test_export_from_cemented) *)
       ]
  in
  ("snapshots", test_cases)
