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

(* From "legacy chain_validator"*)
let may_update_checkpoint chain_state new_head =
  Legacy_state.Chain.checkpoint chain_state
  >>= fun checkpoint ->
  Legacy_state.Block.last_allowed_fork_level new_head
  >>=? fun new_level ->
  if new_level <= checkpoint.shell.level then return_unit
  else
    let state = Legacy_state.Chain.global_state chain_state in
    Legacy_state.history_mode state
    >>= fun history_mode ->
    let head_level = Legacy_state.Block.level new_head in
    Legacy_state.Block.predecessor_n
      new_head
      (Int32.to_int (Int32.sub head_level new_level))
    >>= function
    | None ->
        assert false (* should not happen *)
    | Some new_checkpoint -> (
        Legacy_state.Block.read_opt chain_state new_checkpoint
        >>= function
        | None ->
            assert false (* should not happen *)
        | Some new_checkpoint -> (
            let new_checkpoint = Legacy_state.Block.header new_checkpoint in
            match history_mode with
            | History_mode.Legacy.Archive ->
                Legacy_state.Chain.set_checkpoint chain_state new_checkpoint
                >>= fun () -> return_unit
            | Full ->
                Legacy_state.Chain.set_checkpoint_then_purge_full
                  chain_state
                  new_checkpoint
            | Rolling ->
                Legacy_state.Chain.set_checkpoint_then_purge_rolling
                  chain_state
                  new_checkpoint ) )

let copy_to_legacy previously_baked_blocks old_state current_store =
  Error_monad.iter_s
    (fun b ->
      let header = Store.Block.header b in
      let legacy_block =
        ( {chain_state = old_state; hash = Store.Block.hash b; header}
          : Legacy_state.Block.t )
      in
      Store.Block.get_block_metadata current_store b
      >>=? fun m ->
      let block_header_metadata = Store.Block.block_metadata m in
      let operations = Store.Block.operations b in
      let operations_metadata = Store.Block.operations_metadata m in
      let validation_store =
        ( Store.Block.
            {
              context_hash = context_hash b;
              message = message m;
              max_operations_ttl = max_operations_ttl m;
              last_allowed_fork_level = last_allowed_fork_level m;
            }
          : Tezos_validation.Block_validation.validation_store )
      in
      (* Context checks are ignored *)
      Legacy_state.Block.store
        old_state
        header
        block_header_metadata
        operations
        operations_metadata
        validation_store
        ~forking_testchain:false
      >>=? fun _block ->
      Legacy_chain.set_head old_state legacy_block
      >>=? fun _prev_head ->
      may_update_checkpoint old_state legacy_block >>=? fun () -> return_unit)
    previously_baked_blocks

let assert_presence new_chain_store previously_baked_blocks ?savepoint ?caboose
    = function
  | History_mode.Archive ->
      assert_presence_in_store
        ~with_metadata:true
        new_chain_store
        previously_baked_blocks
  | Full _ ->
      let expected_savepoint = Option.unopt_assert ~loc:__POS__ savepoint in
      let (pruned, complete) =
        List.split_n (Int32.to_int expected_savepoint) previously_baked_blocks
      in
      assert_presence_in_store ~with_metadata:false new_chain_store pruned
      >>=? fun () ->
      assert_presence_in_store ~with_metadata:true new_chain_store complete
  | Rolling _ ->
      let expected_caboose = Option.unopt_assert ~loc:__POS__ caboose in
      let expected_savepoint = Option.unopt_assert ~loc:__POS__ savepoint in
      let (pruned, complete) =
        let rolling_window =
          List.filter
            (fun b -> Store.Block.level b >= expected_caboose)
            previously_baked_blocks
        in
        List.split_n (Int32.to_int expected_savepoint) rolling_window
      in
      assert_presence_in_store ~with_metadata:false new_chain_store pruned
      >>=? fun () ->
      assert_presence_in_store ~with_metadata:true new_chain_store complete

let check_flags new_chain_store descr previously_baked_blocks last history_mode
    =
  match history_mode with
  | History_mode.Archive ->
      Assert.equal_history_mode
        ~msg:("history mode consistency: " ^ descr)
        history_mode
        (Store.Chain.history_mode new_chain_store) ;
      Store.Chain.checkpoint new_chain_store
      >>= fun checkpoint ->
      Store.Block.get_block_metadata new_chain_store last
      >>=? fun metadata ->
      let expected_checkpoint = Store.Block.last_allowed_fork_level metadata in
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("checkpoint consistency: " ^ descr)
        expected_checkpoint
        (snd checkpoint) ;
      Store.Chain.savepoint new_chain_store
      >>= fun savepoint ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("savepoint consistency: " ^ descr)
        0l
        (snd savepoint) ;
      Store.Chain.caboose new_chain_store
      >>= fun caboose ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("caboose consistency: " ^ descr)
        (snd savepoint)
        (snd caboose) ;
      assert_presence new_chain_store previously_baked_blocks history_mode
  | Full _ ->
      Assert.equal_history_mode
        ~msg:("history mode consistency: " ^ descr)
        history_mode
        (Store.Chain.history_mode new_chain_store) ;
      Store.Chain.checkpoint new_chain_store
      >>= fun checkpoint ->
      Store.Block.get_block_metadata new_chain_store last
      >>=? fun metadata ->
      let expected_checkpoint = Store.Block.last_allowed_fork_level metadata in
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("checkpoint consistency: " ^ descr)
        expected_checkpoint
        (snd checkpoint) ;
      Store.Chain.savepoint new_chain_store
      >>= fun savepoint ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("savepoint consistency: " ^ descr)
        expected_checkpoint
        (snd savepoint) ;
      Store.Chain.caboose new_chain_store
      >>= fun caboose ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("caboose consistency: " ^ descr)
        0l
        (snd caboose) ;
      assert_presence
        new_chain_store
        previously_baked_blocks
        ~savepoint:(snd savepoint)
        history_mode
  | Rolling _ ->
      Assert.equal_history_mode
        ~msg:("history mode consistency: " ^ descr)
        history_mode
        (Store.Chain.history_mode new_chain_store) ;
      Store.Chain.checkpoint new_chain_store
      >>= fun checkpoint ->
      Store.Block.get_block_metadata new_chain_store last
      >>=? fun metadata ->
      let expected_checkpoint = Store.Block.last_allowed_fork_level metadata in
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("checkpoint consistency: " ^ descr)
        expected_checkpoint
        (snd checkpoint) ;
      Store.Chain.savepoint new_chain_store
      >>= fun savepoint ->
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("savepoint consistency: " ^ descr)
        expected_checkpoint
        (snd savepoint) ;
      Store.Chain.caboose new_chain_store
      >>= fun caboose ->
      Store.Block.get_block_metadata new_chain_store last
      >>=? fun metadata ->
      let max_op_ttl = Store.Block.max_operations_ttl metadata in
      let min_caboose = if snd checkpoint = 0l then 0l else 1l in
      let expected_caboose =
        max
          min_caboose
          Int32.(add (sub expected_checkpoint (of_int max_op_ttl)) 1l)
      in
      Assert.equal
        ~prn:(Format.sprintf "%ld")
        ~msg:("caboose consistency: " ^ descr)
        expected_caboose
        (snd caboose) ;
      assert_presence
        new_chain_store
        previously_baked_blocks
        ~caboose:expected_caboose
        ~savepoint:expected_checkpoint
        history_mode

let test descr base_dir baking_store legacy_state ~nb_blocks_to_bake
    patch_context history_mode =
  let current_chain_store = Store.main_chain_store baking_store in
  Store.Chain.genesis_block current_chain_store
  >>= fun genesis_block ->
  Alpha_utils.bake_n current_chain_store nb_blocks_to_bake genesis_block
  >>=? fun (previously_baked_blocks, last) ->
  let chain_id = Store.Chain.chain_id (Store.main_chain_store baking_store) in
  Legacy_state.Chain.get legacy_state chain_id
  >>=? fun global_state ->
  Legacy_state.Chain.store global_state
  >>= fun old_store ->
  copy_to_legacy previously_baked_blocks global_state current_chain_store
  >>=? fun () ->
  Legacy_chain.head global_state
  >>= fun head ->
  let lmdb_chain_store = Legacy_store.Chain.get old_store chain_id in
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  Legacy_store.Chain_data.Current_head.store lmdb_chain_data head.hash
  >>= fun () ->
  let chain_name = Distributed_db_version.Name.of_string "TEZOS" in
  let store_dir = base_dir // "upgraded_store" in
  let context_dir = base_dir // "context" in
  Store.init
    ~patch_context
    ~history_mode
    ~store_dir
    ~context_dir
    ~allow_testchains:false
    genesis
  >>=? fun new_store ->
  Lwt.finalize
    (fun () ->
      Error_monad.protect
        (fun () ->
          Legacy.raw_upgrade
            chain_name
            ~new_store
            ~old_store
            history_mode
            genesis
          >>=? fun () -> return_false)
        ~on_error:(function
          | [Legacy.Failed_to_upgrade s] when s = "Nothing to do" ->
              return_true
          | err ->
              Format.printf
                "@\nTest failed:@\n%a@."
                Error_monad.pp_print_error
                err ;
              Alcotest.fail "Should not fail")
      >>=? fun expected_to_fail ->
      if expected_to_fail then return_unit
      else
        Store.get_chain_store new_store chain_id
        >>=? fun new_chain_store ->
        check_flags
          new_chain_store
          descr
          previously_baked_blocks
          last
          history_mode
        >>=? fun () -> return_unit)
    (fun () -> Store.close_store new_store)

let make_tests speed genesis_parameters =
  let accounts = Alpha_utils.Account.generate_accounts 5 in
  let history_modes =
    match speed with
    | `Slow ->
        History_mode.
          [ (* (Target, source) *)
            (Archive, Legacy.Archive);
            (Full {offset = 0}, Legacy.Full);
            (Full {offset = default_offset}, Legacy.Full);
            (Rolling {offset = 0}, Legacy.Rolling);
            (Rolling {offset = default_offset}, Legacy.Rolling) ]
    | `Quick ->
        History_mode.
          [ (* (Target, source) *)
            (Archive, Legacy.Archive);
            (Full {offset = 0}, Legacy.Full);
            (Rolling {offset = 0}, Legacy.Rolling) ]
  in
  let nb_blocks_to_bake =
    match speed with
    | `Slow ->
        0 -- 100
    | `Quick ->
        [0; 3; 8; 21; 42; 57; 89; 92; 101]
  in
  let permutations = List.(product nb_blocks_to_bake history_modes) in
  let patch_context ctxt =
    Alpha_utils.patch_context ~genesis_parameters ~accounts ctxt
  in
  List.map
    (fun (nb_blocks_to_bake, (history_mode, legacy_history_mode)) ->
      let descr =
        Format.asprintf
          "Legacy upgrade on a bootstrapped %a node with %d blocks."
          History_mode.pp
          history_mode
          nb_blocks_to_bake
      in
      wrap_test_legacy
        ~keep_dir:true
        ~history_mode
        ~legacy_history_mode
        ~patch_context
        ( descr,
          fun base_dir current_store legacy_store ->
            test
              descr
              base_dir
              current_store
              legacy_store
              ~nb_blocks_to_bake
              patch_context
              history_mode ))
    permutations

let tests speed =
  let test_cases =
    make_tests
      speed
      Tezos_protocol_alpha_parameters.Default_parameters.(
        parameters_of_constants constants_sandbox)
  in
  ("legacy", test_cases)
