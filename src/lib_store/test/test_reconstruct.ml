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

let test_from_bootstrapped ~descr (store_root, context_root) store
    ~nb_blocks_to_bake ~patch_context =
  let chain_store = Store.main_chain_store store in
  let genesis = Store.Chain.genesis chain_store in
  Store.Chain.genesis_block chain_store
  >>= fun genesis_block ->
  Alpha_utils.bake_n chain_store nb_blocks_to_bake genesis_block
  >>=? fun (baked_blocks, last) ->
  Store.close_store store
  >>= fun () ->
  Error_monad.protect
    (fun () ->
      Reconstruction.reconstruct
        ~patch_context
        ~store_root
        ~context_root
        ~genesis
        ~user_activated_upgrades:[]
        ~user_activated_protocol_overrides:[]
      >>=? fun () -> return_false)
    ~on_error:(function
      | [Reconstruction_errors.(Reconstruction_failure Nothing_to_reconstruct)]
        as e ->
          if Store.Block.level last <= Int32.of_int ((8 * 5) + (2 * 8)) then
            (* It is expected as nothing was reconstructed *)
            return_true
          else (
            Format.printf "@\nTest failed:@\n%a@." Error_monad.pp_print_error e ;
            Alcotest.fail
              "Should not fail to reconstruct (nothing_to_reconstruct raised)."
            )
      | [Reconstruction_errors.(Cannot_reconstruct History_mode.Archive)]
      | [Reconstruction_errors.(Cannot_reconstruct (History_mode.Rolling _))]
        ->
          (* In both Archive and Rolling _ modes, the reconstruction should fail *)
          return_true
      | err ->
          Format.printf "@\nTest failed:@\n%a@." Error_monad.pp_print_error err ;
          Alcotest.fail "Should not fail")
  >>=? fun expected_to_fail ->
  if expected_to_fail then return_unit
  else
    Store.init
      ~patch_context
      ~store_dir:store_root
      ~context_dir:context_root
      ~allow_testchains:false
      genesis
    >>=? fun store ->
    let chain_store = Store.main_chain_store store in
    let history_mode = Store.Chain.history_mode chain_store in
    Assert.equal_history_mode
      ~msg:("history mode consistency: " ^ descr)
      history_mode
      History_mode.Archive ;
    Store.Chain.checkpoint chain_store
    >>= fun checkpoint ->
    Store.Block.get_block_metadata chain_store last
    >>=? fun metadata ->
    let expected_checkpoint = Store.Block.last_allowed_fork_level metadata in
    Assert.equal
      ~prn:(Format.sprintf "%ld")
      ~msg:("checkpoint consistency: " ^ descr)
      expected_checkpoint
      (snd checkpoint) ;
    Store.Chain.savepoint chain_store
    >>= fun savepoint ->
    Assert.equal ~msg:("savepoint consistency: " ^ descr) 0l (snd savepoint) ;
    Store.Chain.caboose chain_store
    >>= fun caboose ->
    Assert.equal
      ~msg:("caboose consistency: " ^ descr)
      (snd savepoint)
      (snd caboose) ;
    assert_presence_in_store ~with_metadata:true chain_store baked_blocks
    >>=? fun () -> Store.close_store store >>= fun () -> return_unit

let make_tests genesis_parameters =
  let history_modes =
    History_mode.
      [ Full {offset = 0};
        Full {offset = 3};
        Full {offset = default_offset}
        (* Archive;
         * Rolling {offset = default_offset} *) ]
  in
  let nb_blocks_to_bake = 1 -- 100 in
  let permutations = List.(product nb_blocks_to_bake history_modes) in
  List.map
    (fun (nb_blocks_to_bake, history_mode) ->
      let descr =
        Format.asprintf
          "Reconstructing on a bootstrapped %a node with %d blocks."
          History_mode.pp
          history_mode
          nb_blocks_to_bake
      in
      let patch_context ctxt =
        Alpha_utils.patch_context ~genesis_parameters ctxt
      in
      wrap_simple_store_init_test
        ~patch_context
        ~history_mode
        ~keep_dir:false
        ( descr,
          fun data_dir store ->
            test_from_bootstrapped
              ~descr
              data_dir
              store
              ~nb_blocks_to_bake
              ~patch_context ))
    permutations

let tests =
  let test_cases =
    make_tests
      Tezos_protocol_alpha_parameters.Default_parameters.(
        parameters_of_constants constants_sandbox)
  in
  ("reconstruct", test_cases)
