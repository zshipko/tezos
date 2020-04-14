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

let check_import_invariants ~caller_loc ~rolling
    (previously_baked_blocks, exported_block) (imported_chain_store, head) =
  protect
    ~on_error:(fun err ->
      Format.eprintf "Error while checking invariants at: %s" caller_loc ;
      Lwt.return (Error err))
    (fun () ->
      (* Check that the head exists with metadata and corresponds to
           the exported block *)
      assert_presence_in_store ~with_metadata:true imported_chain_store [head]
      >>=? fun () ->
      assert_presence_in_store
        ~with_metadata:true
        imported_chain_store
        [exported_block]
      >>=? fun () ->
      Assert.equal_block
        ~msg:("imported head consistency: " ^ caller_loc)
        (Store.Block.header exported_block)
        (Store.Block.header head) ;
      (* Check that we possess all the blocks wrt our descriptors *)
      Store.Chain.savepoint imported_chain_store
      >>= fun savepoint ->
      Store.Chain.checkpoint imported_chain_store
      >>= fun checkpoint ->
      Store.Chain.caboose imported_chain_store
      >>= fun caboose ->
      let (expected_present, expected_absent) =
        List.partition
          (fun b ->
            Compare.Int32.(Store.Block.level b <= snd checkpoint)
            && Compare.Int32.(Store.Block.level b >= snd caboose))
          previously_baked_blocks
      in
      assert_presence_in_store
        ~with_metadata:false
        imported_chain_store
        expected_present
      >>=? fun () ->
      assert_absence_in_store imported_chain_store expected_absent
      >>=? fun () ->
      (* Check that the descriptors are consistent *)
      ( if rolling then
        (* In rolling: we expected to have at least the max_op_ttl
              blocks from the head *)
        Store.Block.get_block_metadata imported_chain_store head
        >>=? fun metadata ->
        let max_op_ttl = Store.Block.max_operations_ttl metadata in
        return Int32.(sub (Store.Block.level head) (of_int max_op_ttl))
      else return 0l )
      >>=? fun expected_caboose_level ->
      Assert.equal
        ~msg:("savepoint consistency: " ^ caller_loc)
        (Store.Block.level exported_block)
        (snd savepoint) ;
      Assert.equal
        ~msg:("checkpoint consistency: " ^ caller_loc)
        (snd savepoint)
        (snd checkpoint) ;
      Assert.equal
        ~msg:("caboose consistency: " ^ __LOC__)
        ~eq:Int32.equal
        ~prn:Int32.to_string
        expected_caboose_level
        (snd caboose) ;
      return_unit)

let export_import ~caller_loc (store_dir, context_dir) chain_store
    ~previously_baked_blocks ?exported_block_hash ~rolling =
  check_invariants chain_store
  >>= fun () ->
  let snapshot_dir = store_dir // "snapshot.full" in
  (* No lockfile on the same process, we must enforce waiting the
     merging thread to finish *)
  let block_store = Store.unsafe_get_block_store chain_store in
  Block_store.await_merging block_store
  >>=? fun () ->
  let block = Option.map ~f:Block_hash.to_b58check exported_block_hash in
  Snapshots.export
    ~rolling
    ?block
    ~store_dir
    ~context_dir
    ~chain_name:(Distributed_db_version.Name.of_string "test")
    ~snapshot_dir
    genesis
  >>=? fun () ->
  let dir = store_dir // "imported_store" in
  let dst_store_dir = dir // "store" in
  let dst_context_dir = dir // "context" in
  Snapshots.import
    ?block
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
  >>=? fun store' ->
  protect
    ~on_error:(fun err ->
      Store.close_store store' >>=? fun () -> Lwt.return (Error err))
    (fun () ->
      let chain_store' = Store.main_chain_store store' in
      Store.Chain.current_head chain_store'
      >>= fun head' ->
      ( match exported_block_hash with
      | Some hash ->
          Assert.equal
            ~msg:("export with given hash: " ^ caller_loc)
            ~eq:Block_hash.equal
            (Store.Block.hash head')
            hash ;
          Lwt.return head'
      | None ->
          Store.Chain.checkpoint chain_store
          >>= fun checkpoint ->
          Assert.equal
            ~msg:("export checkpoint: " ^ caller_loc)
            ~eq:Block_hash.equal
            (Store.Block.hash head')
            (fst checkpoint) ;
          Lwt.return head' )
      >>= fun exported_block ->
      let history_mode = Store.Chain.history_mode chain_store' in
      assert (
        match history_mode with
        | Rolling _ when rolling ->
            true
        | Full _ when not rolling ->
            true
        | _ ->
            false ) ;
      check_import_invariants
        ~caller_loc
        ~rolling
        (previously_baked_blocks, exported_block)
        (chain_store', head')
      >>=? fun () -> return (store', chain_store', head'))

let check_baking_continuity ~caller_loc ~exported_chain_store
    ~imported_chain_store =
  let open Tezos_protocol_alpha.Protocol.Alpha_context in
  Store.Chain.current_head imported_chain_store
  >>= fun imported_head ->
  Alpha_utils.get_constants imported_chain_store imported_head
  >>=? fun {Constants.parametric = {blocks_per_cycle; preserved_cycles; _}; _} ->
  let imported_history_mode = Store.Chain.history_mode imported_chain_store in
  let imported_offset =
    match imported_history_mode with
    | History_mode.Rolling {offset} | Full {offset} ->
        offset
    | Archive ->
        assert false
  in
  (* Bake until we have enough cycles to reach our offset (and a bit more) *)
  let nb_blocks_to_bake =
    Int32.to_int blocks_per_cycle * (preserved_cycles + imported_offset + 2)
  in
  (* Check invariants after every baking *)
  let rec loop head = function
    | 0 ->
        return head
    | n ->
        Alpha_utils.bake imported_chain_store head
        >>=? fun new_head ->
        check_invariants imported_chain_store
        >>= fun () -> loop new_head (n - 1)
  in
  loop imported_head nb_blocks_to_bake
  >>=? fun last' ->
  (* Also bake with the exported store so we make sure we bake the same blocks *)
  let level_to_reach = Store.Block.level last' in
  Store.Chain.current_head exported_chain_store
  >>= fun exported_head ->
  let nb_blocks_to_bake =
    Int32.(to_int (sub level_to_reach (Store.Block.level exported_head)))
  in
  Alpha_utils.bake_n exported_chain_store nb_blocks_to_bake exported_head
  >>=? fun (_blocks, last) ->
  Assert.equal_block
    ~msg:("check both head after baking: " ^ caller_loc)
    (Store.Block.header last)
    (Store.Block.header last') ;
  (* Check that the checkpoint are the same *)
  Store.Chain.checkpoint exported_chain_store
  >>= fun checkpoint ->
  Store.Chain.checkpoint imported_chain_store
  >>= fun checkpoint' ->
  Assert.equal
    ~msg:("checkpoint equality: " ^ caller_loc)
    ~prn:(fun (hash, level) ->
      Format.asprintf "%a (%ld)" Block_hash.pp hash level)
    checkpoint
    checkpoint' ;
  return_unit

let test store_path store ~caller_loc ?exported_block_level
    ~nb_blocks_to_bake_before_export ~rolling =
  Format.printf "start@." ;
  let chain_store = Store.main_chain_store store in
  Store.Chain.genesis_block chain_store
  >>= fun genesis_block ->
  Store.Chain.checkpoint chain_store
  >>= fun checkpoint ->
  match exported_block_level with
  | Some level when Compare.Int32.(level <= 0l) ->
      return_unit
  | None when Compare.Int32.(equal (snd checkpoint) 0l) ->
      return_unit
  | exported_block_level ->
      Format.printf "baking@." ;
      Alpha_utils.bake_n
        chain_store
        nb_blocks_to_bake_before_export
        genesis_block
      >>=? fun (previously_baked_blocks, _last) ->
      ( match exported_block_level with
      | Some lvl ->
          Store.Block.read_block_by_level chain_store lvl
          >>=? fun block -> return_some (Store.Block.hash block)
      | None ->
          return_none )
      >>=? fun exported_block_hash ->
      Format.printf "export import@." ;
      export_import
        ~caller_loc
        store_path
        chain_store
        ~rolling
        ?exported_block_hash
        ~previously_baked_blocks
      >>=? fun (store', chain_store', _head) ->
      Lwt.finalize
        (fun () ->
          Format.printf "baking continuity import@." ;
          check_baking_continuity
            ~caller_loc
            ~exported_chain_store:chain_store
            ~imported_chain_store:chain_store')
        (fun () ->
          (* only close store' - store will be closed by the test
             wrapper *)
          Store.close_store store' >>= fun _ -> Lwt.return_unit)

let make_tests genesis_parameters =
  let open Tezos_protocol_alpha.Protocol in
  let { Parameters_repr.constants =
          {Constants_repr.blocks_per_cycle; preserved_cycles; _};
        _ } =
    genesis_parameters
  in
  let blocks_per_cycle = Int32.to_int blocks_per_cycle in
  let nb_initial_blocks_list =
    (* the ladle *)
    [ preserved_cycles * blocks_per_cycle;
      ((2 * preserved_cycles) + 1) * blocks_per_cycle;
      65;
      77;
      89 ]
  in
  let exporter_history_modes =
    History_mode.
      [ Archive;
        Full {offset = 0};
        Full {offset = default_offset};
        Rolling {offset = 0};
        Rolling {offset = default_offset} ]
  in
  let export_blocks_levels =
    List.flatten
      (List.map
         (fun nb_initial_blocks ->
           [ None;
             Some Int32.(of_int (nb_initial_blocks - blocks_per_cycle));
             Some (Int32.of_int nb_initial_blocks) ])
         nb_initial_blocks_list)
  in
  let permutations =
    List.(
      product
        nb_initial_blocks_list
        (product
           exporter_history_modes
           (product export_blocks_levels [false; true])))
    |> List.map (fun (a, (b, (c, d))) -> (a, b, c, d))
  in
  List.filter_map
    (fun (nb_initial_blocks, history_mode, exported_block_level, rolling) ->
      let description =
        Format.asprintf
          "export => import with %d initial blocks from %a to %s (exported \
           block at %s)"
          nb_initial_blocks
          History_mode.pp
          history_mode
          (if rolling then "rolling" else "full")
          ( match exported_block_level with
          | None ->
              "checkpoint"
          | Some i ->
              Format.sprintf "level %ld" i )
      in
      match history_mode with
      | Rolling _ when rolling = false ->
          None
      | _ -> (
        match exported_block_level with
        | Some level
          when Compare.Int32.(Int32.of_int nb_initial_blocks <= level) ->
            None
        | None | Some _ ->
            Some
              (wrap_test
                 ~keep_dir:true
                 ~history_mode
                 ~patch_context:(fun ctxt ->
                   Alpha_utils.patch_context ~genesis_parameters ctxt)
                 ( description,
                   fun store_path store ->
                     test
                       ?exported_block_level
                       ~nb_blocks_to_bake_before_export:nb_initial_blocks
                       ~rolling
                       ~caller_loc:description (* to remove *)
                       store_path
                       store )) ))
    permutations

(* (\* Test below the checkpoint *\)
 *
 * let test_export_full store_path store =
 *   let chain_store = Store.main_chain_store store in
 *   Store.Chain.genesis_block chain_store
 *   >>= fun genesis_block ->
 *   Alpha_utils.bake_n chain_store 49 genesis_block
 *   >>=? fun (blocks, last) ->
 *   export_import
 *     store_path
 *     chain_store
 *     (Store.Block.descriptor last)
 *     ~rolling:false
 *   >>=? fun (store', chain_store', head) ->
 *   assert_presence_in_store
 *     ~with_metadata:false
 *     chain_store'
 *     (List.rev (List.tl (List.rev blocks)))
 *   >>=? fun () ->
 *   Alpha_utils.bake_until_n_cycle_end chain_store' 10 head
 *   >>=? fun (blocks, _) ->
 *   assert_presence_in_store ~with_metadata:false chain_store' blocks
 *   >>=? fun () ->
 *   check_invariants chain_store'
 *   >>= fun () -> Store.close_store store' >>= fun () -> return_unit
 *
 * let test_export_rolling store_path store =
 *   let chain_store = Store.main_chain_store store in
 *   Store.Chain.genesis_block chain_store
 *   >>= fun genesis_block ->
 *   Alpha_utils.bake_n chain_store 49 genesis_block
 *   >>=? fun (blocks, last) ->
 *   Store.Chain.savepoint chain_store
 *   >>= fun savepoint ->
 *   Store.Chain.checkpoint chain_store
 *   >>= fun checkpoint ->
 *   Store.Chain.caboose chain_store
 *   >>= fun caboose ->
 *   Format.printf
 *     "savepoint %ld - checkpoint %ld - caboose %ld@."
 *     (snd savepoint)
 *     (snd checkpoint)
 *     (snd caboose) ;
 *   export_import
 *     store_path
 *     chain_store
 *     (Store.Block.descriptor last)
 *     ~rolling:true
 *   >>=? fun (store', chain_store', head) ->
 *   Store.Chain.savepoint chain_store'
 *   >>= fun savepoint ->
 *   Store.Chain.checkpoint chain_store'
 *   >>= fun checkpoint ->
 *   Store.Chain.caboose chain_store'
 *   >>= fun caboose ->
 *   Format.printf
 *     "savepoint %ld - checkpoint %ld - caboose %ld@."
 *     (snd savepoint)
 *     (snd checkpoint)
 *     (snd caboose) ;
 *   Store.Block.get_block_metadata chain_store' head
 *   >>=? fun metadata ->
 *   let max_op_ttl = Store.Block.max_operations_ttl metadata in
 *   let expected_caboose_level =
 *     Int32.(sub (Store.Block.level head) (of_int max_op_ttl))
 *   in
 *   Store.Chain.caboose chain_store'
 *   >>= fun (_, caboose_level) ->
 *   Assert.equal
 *     ~msg:__LOC__
 *     ~eq:Int32.equal
 *     ~prn:Int32.to_string
 *     expected_caboose_level
 *     caboose_level ;
 *   let blocks =
 *     List.filter
 *       (fun b -> Compare.Int32.(Store.Block.level b >= caboose_level))
 *       blocks
 *   in
 *   assert_presence_in_store
 *     ~with_metadata:false
 *     chain_store'
 *     (List.rev (List.tl (List.rev blocks)))
 *   >>=? fun () ->
 *   (\* Make sure the new head is the chosen exported block *\)
 *   Assert.equal
 *     ~prn:(Format.asprintf "%a" Test_utils.pp_block)
 *     ~eq:Store.Block.equal
 *     last
 *     head ;
 *   Alpha_utils.bake_until_n_cycle_end chain_store' 10 head
 *   >>=? fun (blocks, _) ->
 *   Store.Chain.caboose chain_store'
 *   >>= fun (_, caboose_level) ->
 *   let present_blocks =
 *     List.filter
 *       (fun b -> Compare.Int32.(Store.Block.level b >= caboose_level))
 *       blocks
 *   in
 *   assert_presence_in_store ~with_metadata:false chain_store' present_blocks
 *   >>=? fun () ->
 *   check_invariants chain_store'
 *   >>= fun () -> Store.close_store store' >>= fun () -> return_unit
 *
 * let test_export_checkpoint_full store_path store =
 *   let chain_store = Store.main_chain_store store in
 *   Store.Chain.genesis_block chain_store
 *   >>= fun genesis_block ->
 *   Alpha_utils.bake_n chain_store 49 genesis_block
 *   >>=? fun (blocks, _last) ->
 *   Store.Chain.checkpoint chain_store
 *   >>= fun checkpoint ->
 *   export_import store_path chain_store checkpoint ~rolling:false
 *   >>=? fun (store', chain_store', head) ->
 *   let (expected_present, expected_absent) =
 *     List.partition
 *       (fun b -> Compare.Int32.(Store.Block.level b <= snd checkpoint))
 *       blocks
 *   in
 *   assert_presence_in_store ~with_metadata:false chain_store' expected_present
 *   >>=? fun () ->
 *   assert_absence_in_store chain_store' expected_absent
 *   >>=? fun () ->
 *   Alpha_utils.bake_until_n_cycle_end chain_store' 10 head
 *   >>=? fun (blocks, _) ->
 *   assert_presence_in_store ~with_metadata:false chain_store' blocks
 *   >>=? fun () ->
 *   check_invariants chain_store'
 *   >>= fun () -> Store.close_store store' >>= fun () -> return_unit
 *
 * let test_export_checkpoint_rolling store_path store =
 *   let chain_store = Store.main_chain_store store in
 *   Store.Chain.genesis_block chain_store
 *   >>= fun genesis_block ->
 *   Alpha_utils.bake_n chain_store 49 genesis_block
 *   >>=? fun (blocks, _last) ->
 *   Store.Chain.checkpoint chain_store
 *   >>= fun checkpoint ->
 *   export_import store_path chain_store checkpoint ~rolling:true
 *   >>=? fun (store', chain_store', head) ->
 *   Store.Chain.caboose chain_store'
 *   >>= fun (_, caboose_level) ->
 *   Store.Block.get_block_metadata chain_store' head
 *   >>=? fun metadata ->
 *   let max_op_ttl = Store.Block.max_operations_ttl metadata in
 *   let expected_caboose_level =
 *     Int32.(sub (Store.Block.level head) (of_int max_op_ttl))
 *   in
 *   Assert.equal
 *     ~msg:__LOC__
 *     ~eq:Int32.equal
 *     ~prn:Int32.to_string
 *     expected_caboose_level
 *     caboose_level ;
 *   let (expected_present, expected_absent) =
 *     List.partition
 *       (fun b ->
 *         Compare.Int32.(Store.Block.level b <= snd checkpoint)
 *         && Compare.Int32.(Store.Block.level b >= caboose_level))
 *       blocks
 *   in
 *   assert_presence_in_store ~with_metadata:false chain_store' expected_present
 *   >>=? fun () ->
 *   assert_absence_in_store chain_store' expected_absent
 *   >>=? fun () ->
 *   Alpha_utils.bake_until_n_cycle_end chain_store' 10 head
 *   >>=? fun (blocks, _) ->
 *   let (expected_present, expected_absent) =
 *     List.partition
 *       (fun b -> Compare.Int32.(Store.Block.level b >= caboose_level))
 *       blocks
 *   in
 *   assert_presence_in_store ~with_metadata:false chain_store' expected_present
 *   >>=? fun () ->
 *   assert_absence_in_store chain_store' expected_absent
 *   >>=? fun () ->
 *   assert_presence_in_store ~with_metadata:false chain_store' blocks
 *   >>=? fun () ->
 *   check_invariants chain_store'
 *   >>= fun () -> Store.close_store store' >>= fun () -> return_unit
 *
 * let test_export_from_cemented store_path store =
 *   Format.printf "store path : %s@." (fst store_path) ;
 *   test_export_checkpoint_full store_path store *)

(* TODO:
   export => import => export => import from full & rolling
   export equivalence
*)

let tests =
  let test_cases =
    make_tests
      Tezos_protocol_alpha_parameters.Default_parameters.(
        parameters_of_constants constants_sandbox)
    (* List.map
     *   (wrap_test ~patch_context:(fun ctxt -> Alpha_utils.patch_context ctxt))
     *   [ ("export full", test_export_full);
     *     ("export rolling", test_export_rolling);
     *     ("export checkpoint full", test_export_checkpoint_full);
     *     ("export checkpoint rolling", test_export_checkpoint_rolling);
     *     ("export from cemented", test_export_from_cemented) ] *)
  in
  ("snapshots", test_cases)
