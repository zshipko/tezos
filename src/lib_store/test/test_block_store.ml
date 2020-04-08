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
open Block_store

let descr b = Block_repr.(hash b, level b)

let assert_presence_in_block_store ?(with_metadata = false) block_store blocks
    =
  iter_s
    (fun b ->
      let hash = Block_repr.hash b in
      Block_store.is_known block_store (Hash (hash, 0))
      >>= fun is_known ->
      if not is_known then
        Alcotest.failf
          "assert_presence_in_block_store: block %a in not known"
          pp_raw_block
          b ;
      Block_store.read_block
        ~read_metadata:with_metadata
        block_store
        (Hash (hash, 0))
      >>= function
      | None ->
          Alcotest.failf
            "assert_presence_in_block_store: cannot find block %a"
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

let assert_absence_in_block_store block_store blocks =
  iter_s
    (fun b ->
      let hash = Block_repr.hash b in
      Block_store.is_known block_store (Hash (hash, 0))
      >>= function
      | true ->
          Alcotest.failf
            "assert_absence_in_block_store: found unexpected block %a"
            pp_raw_block
            b
      | false ->
          return_unit)
    blocks

let test_storing_and_access_predecessors block_store =
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) 200
  >>= fun (blocks, _head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  assert_presence_in_block_store block_store blocks
  >>=? fun () ->
  Lwt_list.iter_s
    (fun b ->
      let hash = Block_repr.hash b in
      let level = Block_repr.level b in
      Lwt_list.iter_s
        (fun distance ->
          Block_store.get_predecessor block_store hash distance
          >>= function
          | None ->
              Alcotest.fail "expected predecessor but none found"
          | Some h ->
              Alcotest.check
                (module Block_hash)
                (Format.asprintf
                   "get %d-th predecessor of %a (%ld)"
                   distance
                   Block_hash.pp
                   hash
                   level)
                h
                (List.nth blocks (Int32.to_int level - distance)).hash ;
              Lwt.return_unit)
        (0 -- (Int32.to_int level - 1)))
    blocks
  >>= fun () -> return_unit

let test_consecutive_concurrent_merges block_store =
  let nb_blocks = 400 in
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) nb_blocks
  >>= fun (blocks, _head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  let ((chunk1, chunk2), (chunk3, chunk4)) =
    let (a, b) = List.split_n (nb_blocks / 2) blocks in
    (List.split_n (nb_blocks / 4) a, List.split_n (nb_blocks / 4) b)
  in
  let merge_chunk chunk =
    Block_store.merge_stores
      block_store
      ~history_mode:Archive
      ~from_block:(descr (List.hd chunk))
      ~to_block:(descr (List.hd (List.rev chunk)))
      ~nb_blocks_to_preserve:0
      ~finalizer:(fun () ->
        assert_presence_in_block_store block_store chunk
        >>= function Ok () -> Lwt.return_unit | _ -> assert false)
      ()
  in
  let threads = List.map merge_chunk [chunk1; chunk2; chunk3; chunk4] in
  Lwt.join threads
  >>= fun () ->
  Block_store.await_merging block_store
  >>= fun () ->
  assert_presence_in_block_store ~with_metadata:false block_store blocks
  >>=? fun () -> return_unit

let test_big_merge block_store =
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) 20_000
  >>= fun (blocks, head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  Block_store.merge_stores
    block_store
    ~history_mode:Archive
    ~from_block:(descr (List.hd blocks))
    ~to_block:(descr head)
    ~nb_blocks_to_preserve:0
    ~finalizer:(fun () ->
      assert_presence_in_block_store block_store blocks
      >>= function Ok () -> Lwt.return_unit | _ -> assert false)
    ()
  >>= fun () ->
  assert_presence_in_block_store ~with_metadata:false block_store blocks
  >>=? fun () ->
  Block_store.await_merging block_store >>= fun () -> return_unit

(** Makes several branches at different level and make sure those
    created before the checkpoint are correctly GCed *)
let test_merge_with_branches block_store =
  (* make an initial chain of nb_blocks+1 blocks *)
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) 1000
  >>= fun (blocks, _head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  let branches_fork_points_to_gc = [100; 200; 300; 400; 448] in
  (* 448 => we also keep the checkpoint *)
  map_s
    (fun level ->
      let fork_root = List.nth blocks (level - 1) in
      make_raw_block_list ~kind:`Full (descr fork_root) 50
      >>= fun (blocks, _head) ->
      Lwt_list.iter_s (Block_store.store_block block_store) blocks
      >>= fun () -> return blocks)
    branches_fork_points_to_gc
  >>=? fun blocks_to_gc ->
  let branches_fork_points_to_keep = [450; 500; 600; 700; 800] in
  map_s
    (fun level ->
      let fork_root = List.nth blocks (level - 1) in
      make_raw_block_list ~kind:`Full (descr fork_root) 50
      >>= fun (blocks, _head) ->
      Lwt_list.iter_s (Block_store.store_block block_store) blocks
      >>= fun () -> return blocks)
    branches_fork_points_to_keep
  >>=? fun blocks_to_keep ->
  (* merge 0 to 450 *)
  let from_block = descr @@ List.hd blocks in
  let to_block = descr @@ List.nth blocks 449 in
  Block_store.merge_stores
    block_store
    ~history_mode:Archive
    ~nb_blocks_to_preserve:0
    ~from_block
    ~to_block
    ()
  >>= fun () ->
  Block_store.await_merging block_store
  >>= fun () ->
  assert_presence_in_block_store
    block_store
    (blocks @ List.flatten blocks_to_keep)
  >>=? fun () ->
  assert_absence_in_block_store block_store (List.flatten blocks_to_gc)

let test_archive_merge block_store =
  let nb_blocks = 400 in
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) nb_blocks
  >>= fun (blocks, _head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  let ((chunk1, chunk2), (chunk3, chunk4)) =
    let (a, b) = List.split_n (nb_blocks / 2) blocks in
    (List.split_n (nb_blocks / 4) a, List.split_n (nb_blocks / 4) b)
  in
  let merge_chunk chunk =
    Block_store.merge_stores
      block_store
      ~history_mode:Archive
      ~nb_blocks_to_preserve:0
      ~from_block:(descr (List.hd chunk))
      ~to_block:(descr (List.hd (List.rev chunk)))
      ~finalizer:(fun () ->
        assert_presence_in_block_store block_store chunk
        >>= function Ok () -> Lwt.return_unit | _ -> assert false)
      ()
  in
  let threads = List.map merge_chunk [chunk1; chunk2; chunk3; chunk4] in
  Lwt.join threads
  >>= fun () ->
  assert_presence_in_block_store ~with_metadata:true block_store blocks
  >>=? fun () ->
  Block_store.await_merging block_store >>= fun () -> return_unit

let test_full_2_merge block_store =
  let nb_blocks = 400 in
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) nb_blocks
  >>= fun (blocks, _head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  let ((chunk1, chunk2), (chunk3, chunk4)) =
    let (a, b) = List.split_n (nb_blocks / 2) blocks in
    (List.split_n (nb_blocks / 4) a, List.split_n (nb_blocks / 4) b)
  in
  let merge_chunk chunk =
    Block_store.merge_stores
      block_store
      ~history_mode:(Full {offset = 2})
      ~from_block:(descr (List.hd chunk))
      ~to_block:(descr (List.hd (List.rev chunk)))
      ~nb_blocks_to_preserve:0
      ~finalizer:(fun () ->
        assert_presence_in_block_store block_store chunk
        >>= function Ok () -> Lwt.return_unit | _ -> assert false)
      ()
  in
  let threads = List.map merge_chunk [chunk1; chunk2; chunk3; chunk4] in
  Lwt.join threads
  >>= fun () ->
  Block_store.await_merging block_store
  >>= fun () ->
  assert_presence_in_block_store
    ~with_metadata:true
    block_store
    (chunk3 @ chunk4)
  >>=? fun () ->
  assert_presence_in_block_store
    block_store
    ~with_metadata:false
    (chunk1 @ chunk2)
  >>=? fun () -> return_unit

let test_rolling_0_merge block_store =
  let nb_blocks = 400 in
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) nb_blocks
  >>= fun (blocks, _head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  let ((chunk1, chunk2), (chunk3, chunk4)) =
    let (a, b) = List.split_n (nb_blocks / 2) blocks in
    (List.split_n (nb_blocks / 4) a, List.split_n (nb_blocks / 4) b)
  in
  let merge_chunk chunk =
    Block_store.merge_stores
      block_store
      ~history_mode:(Rolling {offset = 0})
      ~nb_blocks_to_preserve:(nb_blocks / 8)
      ~from_block:(descr (List.hd chunk))
      ~to_block:(descr (List.hd (List.rev chunk)))
      ()
  in
  let threads = List.map merge_chunk [chunk1; chunk2; chunk3; chunk4] in
  Lwt.join threads
  >>= fun () ->
  Block_store.await_merging block_store
  >>= fun () ->
  let (purged_blocks, preserved_blocks) =
    List.split_n (nb_blocks - (nb_blocks / 8) - 1) blocks
  in
  (* -1 => because we keep the checkpoint in floating *)
  assert_presence_in_block_store
    ~with_metadata:true
    block_store
    preserved_blocks
  >>=? fun () ->
  assert_absence_in_block_store block_store purged_blocks
  >>=? fun () -> return_unit

let test_rolling_2_merge block_store =
  let nb_blocks = 400 in
  make_raw_block_list ~kind:`Full (genesis_hash, -1l) nb_blocks
  >>= fun (blocks, _head) ->
  Lwt_list.iter_s (Block_store.store_block block_store) blocks
  >>= fun () ->
  let ((chunk1, chunk2), (chunk3, chunk4)) =
    let (a, b) = List.split_n (nb_blocks / 2) blocks in
    (List.split_n (nb_blocks / 4) a, List.split_n (nb_blocks / 4) b)
  in
  let merge_chunk chunk =
    Block_store.merge_stores
      block_store
      ~nb_blocks_to_preserve:0
      ~history_mode:(Rolling {offset = 2})
      ~from_block:(descr (List.hd chunk))
      ~to_block:(descr (List.hd (List.rev chunk)))
      ()
  in
  let threads = List.map merge_chunk [chunk1; chunk2; chunk3; chunk4] in
  Lwt.join threads
  >>= fun () ->
  Block_store.await_merging block_store
  >>= fun () ->
  assert_presence_in_block_store
    ~with_metadata:true
    block_store
    (chunk3 @ chunk4)
  >>=? fun () ->
  assert_absence_in_block_store block_store (chunk1 @ chunk2)
  >>=? fun () -> return_unit

let wrap_test ?keep_dir (name, g) =
  let f _ store =
    let chain_store = Store.main_chain_store store in
    let block_store = Store.unsafe_get_block_store chain_store in
    g block_store
  in
  wrap_test ?keep_dir (name, f)

let tests =
  let test_cases =
    List.map
      wrap_test
      [ ( "block storing and access predecessors (hash only)",
          test_storing_and_access_predecessors );
        ("consecutive & concurrent merge", test_consecutive_concurrent_merges);
        ("merge with branches", test_merge_with_branches);
        ("consecutive merge (Archive)", test_archive_merge);
        ("consecutive merge (Full + 2 cycles)", test_full_2_merge);
        ( "consecutive merge (Rolling + 0 cycles + preserve blocks)",
          test_rolling_0_merge );
        ("consecutive merge (Rolling + 2 cycles)", test_rolling_2_merge);
        ("20k blocks merge", test_big_merge) ]
  in
  ("block store", test_cases)
