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

open Test_utils

let assert_presence_in_cemented_store ?(with_metadata = true) cemented_store
    blocks =
  iter_s
    (fun b ->
      let hash = Block_repr.hash b in
      Cemented_block_store.get_cemented_block_by_hash
        ~read_metadata:with_metadata
        cemented_store
        hash
      >>= function
      | None ->
          Alcotest.failf
            "assert_presence_in_cemented_store: cannot find block %a"
            pp_raw_block
            b
      | Some b' ->
          if with_metadata then (
            Assert.equal ~msg:"block equality with metadata" b b' ;
            return_unit )
          else (
            Assert.equal_block
              ~msg:"block equality without metadata"
              (Block_repr.header b)
              (Block_repr.header b') ;
            return_unit ))
    blocks

let test_cement_pruned_blocks cemented_store =
  make_raw_block_list ~kind:`Pruned (genesis_hash, -1l) 4095
  >>= fun (blocks, _head) ->
  Cemented_block_store.cement_blocks
    cemented_store
    ~write_metadata:false
    blocks
  >>=? fun () ->
  assert_presence_in_cemented_store ~with_metadata:true cemented_store blocks

let test_cement_full_blocks cemented_store =
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) 4095
  >>= fun (blocks, _head) ->
  Cemented_block_store.cement_blocks
    cemented_store
    ~write_metadata:false
    blocks
  >>=? fun () ->
  assert_presence_in_cemented_store ~with_metadata:false cemented_store blocks

let test_metadata_retrieval cemented_store =
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) 100
  >>= fun (blocks, _head) ->
  Cemented_block_store.cement_blocks cemented_store ~write_metadata:true blocks
  >>=? fun () ->
  assert_presence_in_cemented_store ~with_metadata:true cemented_store blocks

let wrap_cemented_store_test (name, f) =
  let cemented_store_init f _ () =
    let prefix_dir = "tezos_indexed_store_test_" in
    let run f =
      let base_dir = Filename.temp_file prefix_dir "" in
      Format.printf "temp dir: %s@." base_dir ;
      Lwt_unix.unlink base_dir
      >>= fun () -> Lwt_unix.mkdir base_dir 0o700 >>= fun () -> f base_dir
    in
    run (fun base_dir ->
        let dir = Filename.concat base_dir "cemented_store" in
        Cemented_block_store.init ~cemented_blocks_dir:dir
        >>=? fun cemented_store ->
        Error_monad.protect (fun () ->
            f cemented_store
            >>=? fun () ->
            Cemented_block_store.close cemented_store ;
            return_unit))
    >>= function
    | Error err ->
        Format.printf "@\nTest failed:@\n%a@." Error_monad.pp_print_error err ;
        Lwt.fail Alcotest.Test_error
    | Ok () ->
        Lwt.return_unit
  in
  Alcotest_lwt.test_case name `Quick (cemented_store_init f)

(* let test_cemented_predecessors store =
 *   let chain_store = Store.main_chain_store store in
 *   let block_store = Store.unsafe_get_block_store chain_store in
 *   make_block_list chain_store ~kind:`Full 100
 *   >>= fun (blocks, _head) ->
 *   Block_store.cement_blocks ~write_metadata:false block_store blocks
 *   >>= fun () ->
 *   Lwt_list.iter_s
 *     (fun b ->
 *       let level = Block_repr.level b in
 *       Lwt_list.iter_s
 *         (fun distance ->
 *           Block_store.get_predecessor block_store b.hash distance
 *           >>= function
 *           | None ->
 *               Alcotest.fail "expected predecessor but none found"
 *           | Some h ->
 *               Alcotest.check
 *                 (module Block_hash)
 *                 "get predecessor"
 *                 h
 *                 (List.nth blocks (Int32.to_int level - distance - 1)).hash ;
 *               Lwt.return_unit)
 *         (0 -- (Int32.to_int level - 1)))
 *     blocks
 *   >>= fun () -> return_unit *)

let tests =
  let test_cases =
    List.map
      wrap_cemented_store_test
      [ ("cementing pruned blocks", test_cement_pruned_blocks);
        ("cementing full blocks", test_cement_full_blocks);
        ("retrieve cemented metadata", test_metadata_retrieval) ]
  in
  ("cemented store", test_cases)
