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

let test_from_bootstrapped (store_root, context_root) _store ~nb_blocks_to_bake
    ~patch_context =
  Store.init
    ~store_dir:store_root
    ~context_dir:context_root
    ~allow_testchains:false
    genesis
  >>=? fun store ->
  let chain_store = Store.main_chain_store store in
  let genesis = Store.Chain.genesis chain_store in
  Store.Chain.genesis_block chain_store
  >>= fun genesis_block ->
  Format.printf "here@." ;
  Alpha_utils.bake_n chain_store nb_blocks_to_bake genesis_block
  >>=? fun (_previously_baked_blocks, _last) ->
  Format.printf "here@." ;
  Store.close_store store
  >>=? fun () ->
  Reconstruction.reconstruct
    ~patch_context
    ~store_root
    ~context_root
    ~genesis
    ~user_activated_upgrades:[]
    ~user_activated_protocol_overrides:[]
  >>=? fun () -> return_unit

let make_tests genesis_parameters =
  let history_modes =
    History_mode.
      [ (* Archive; *)
        Full {offset = 0}
        (* Full {offset = default_offset};
         * Rolling {offset = 0};
         * Rolling {offset = default_offset} *) ]
  in
  let nb_blocks_to_bake = [96] in
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
      wrap_test
        ~keep_dir:true
        ~history_mode
        ~patch_context
        ( descr,
          fun data_dir store ->
            test_from_bootstrapped
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
