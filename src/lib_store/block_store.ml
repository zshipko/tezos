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

module Block_cache = Lru_cache.Make (Block_hash.Table)
open Block_repr

type block_store = {
  chain_dir : string;
  genesis_block : Block_repr.t;
  cemented_store : Cemented_block_store.t;
  mutable ro_floating_block_stores : Floating_block_store.t list;
  mutable rw_floating_block_store : Floating_block_store.t;
  block_cache : Block_repr.t Block_cache.t;
  merge_mutex : Lwt_mutex.t;
  merge_scheduler : Lwt_idle_waiter.t;
  (* Target level x Merging thread *)
  mutable merging_thread : (int32 * unit Lwt.t) option;
}

type t = block_store

type key = Hash of (Block_hash.t * int)

(** [global_predecessor_lookup chain_block_store hash nth] retrieves the
    2^[nth] predecessor's hash from the block with corresponding [hash]
    by checking all stores iteratively.
    Returns none if the predecessor is not found or if it is below genesis.
*)
let global_predecessor_lookup block_store hash pow_nth =
  (* pow_nth = 0 => direct predecessor *)
  (* Look in the RW block_store then RO stores then cemented *)
  let hash_opt =
    List.find_map
      (function
        | {Floating_block_store.floating_block_index; _} -> (
          try
            let predecessors =
              (Floating_block_index.find floating_block_index hash)
                .predecessors
            in
            List.nth_opt predecessors pow_nth
          with Not_found -> None ))
      ( block_store.rw_floating_block_store
      :: block_store.ro_floating_block_stores )
  in
  match hash_opt with
  | Some hash ->
      Some hash
  | None -> (
    (* It must be cemented *)
    match
      Cemented_block_store.get_cemented_block_level
        block_store.cemented_store
        hash
    with
    | None ->
        None
    | Some level ->
        (* level - 2^n *)
        let pred_level =
          max
            (Block_repr.level block_store.genesis_block)
            Int32.(sub level (shift_left 1l pow_nth))
        in
        Cemented_block_store.get_cemented_block_hash
          block_store.cemented_store
          pred_level )

(**
   Takes a block and populates its predecessors store, under the
   assumption that all its predecessors have their store already
   populated. The precedecessors are distributed along the chain, up
   to the genesis, at a distance from [b] that grows exponentially.
   The store tabulates a function [p] from distances to block_ids such
   that if [p(b,d)=b'] then [b'] is at distance 2^d from [b].
   Example of how previous predecessors are used:
   p(n,0) = n-1
   p(n,1) = n-2  = p(n-1,0)
   p(n,2) = n-4  = p(n-2,1)
   p(n,3) = n-8  = p(n-4,2)
   p(n,4) = n-16 = p(n-8,3)
   ...
*)
let compute_predecessors block_store block =
  let rec loop predecessors_acc pred dist =
    if dist = Floating_block_index.nb_predecessors then predecessors_acc
    else
      match global_predecessor_lookup block_store pred (dist - 1) with
      | None ->
          predecessors_acc
      | Some pred' ->
          loop (pred' :: predecessors_acc) pred' (dist + 1)
  in
  let predecessor = predecessor block in
  if Block_hash.equal block.hash predecessor then
    (* genesis *)
    List.init Floating_block_index.nb_predecessors (fun _ -> block.hash)
  else
    let rev_preds = loop [predecessor] predecessor 1 in
    List.rev rev_preds

(** [get_predecessor chain_store hash distance] retrieves the
    block which is at [distance] from the block with corresponding [hash]
    by every store iteratively *)
let get_predecessor block_store block_hash distance =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      let closest_power_two_and_rest n =
        if n < 0 then invalid_arg "negative argument"
        else
          let rec loop cnt n rest =
            if n <= 1 then (cnt, rest)
            else loop (cnt + 1) (n / 2) (rest + ((1 lsl cnt) * (n mod 2)))
          in
          loop 0 n 0
      in
      (* actual predecessor function *)
      if distance = 0 then Lwt.return_some block_hash
      else if distance < 0 then
        Format.kasprintf
          Stdlib.failwith
          "nth_predecessor: distance = %d"
          distance
      else
        let rec loop block_hash distance =
          if distance = 1 then
            Lwt.return (global_predecessor_lookup block_store block_hash 0)
          else
            let (power, rest) = closest_power_two_and_rest distance in
            let (power, rest) =
              if power < Floating_block_index.nb_predecessors then (power, rest)
              else
                let power = Floating_block_index.nb_predecessors - 1 in
                let rest = distance - (1 lsl power) in
                (power, rest)
            in
            match global_predecessor_lookup block_store block_hash power with
            | None ->
                Lwt.return_none (* reached genesis *)
            | Some pred ->
                if rest = 0 then Lwt.return_some pred
                  (* landed on the requested predecessor *)
                else loop pred rest
          (* need to jump further back *)
        in
        loop block_hash distance)

let is_known block_store key_kind =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      match key_kind with
      | Hash (hash, offset) -> (
          get_predecessor block_store hash offset
          >>= function
          | None ->
              Lwt.return_false
          | Some predecessor_hash ->
              let is_known =
                List.exists
                  (fun store ->
                    Floating_block_index.mem
                      store.Floating_block_store.floating_block_index
                      predecessor_hash)
                  ( block_store.rw_floating_block_store
                  :: block_store.ro_floating_block_stores )
              in
              Lwt.return
                ( is_known
                || Cemented_block_store.is_cemented
                     block_store.cemented_store
                     predecessor_hash ) ))

let read_block ~read_metadata block_store key_kind =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      match key_kind with
      | Hash (hash, offset) -> (
          (* Resolve the hash *)
          get_predecessor block_store hash offset
          >>= function
          | None ->
              Lwt.return_none
          | Some adjusted_hash ->
              if Block_hash.equal block_store.genesis_block.hash adjusted_hash
              then Lwt.return_some block_store.genesis_block
              else
                let fetch_block adjusted_hash =
                  (* First look in the floating stores *)
                  Lwt_utils.find_map_s
                    (fun store ->
                      Floating_block_store.get_block store adjusted_hash)
                    ( block_store.rw_floating_block_store
                    :: block_store.ro_floating_block_stores )
                  >>= function
                  | Some block ->
                      Lwt.return_some block
                  | None ->
                      (* Lastly, look in the cemented blocks *)
                      Cemented_block_store.get_cemented_block_by_hash
                        ~read_metadata
                        block_store.cemented_store
                        adjusted_hash
                in
                Block_cache.get_opt_lwt
                  block_store.block_cache
                  fetch_block
                  adjusted_hash ))

let read_block_metadata block_store key_kind =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      match key_kind with
      | Hash (hash, offset) -> (
          (* Resolve the hash *)
          get_predecessor block_store hash offset
          >>= function
          | None ->
              Lwt.return_none
          | Some adjusted_hash -> (
              if Block_hash.equal block_store.genesis_block.hash adjusted_hash
              then Lwt.return (Block_repr.metadata block_store.genesis_block)
              else
                (* First look in the floating stores *)
                Lwt_utils.find_map_s
                  (fun store ->
                    Floating_block_store.get_block store adjusted_hash)
                  ( block_store.rw_floating_block_store
                  :: block_store.ro_floating_block_stores )
                >>= function
                | Some block ->
                    Lwt.return block.metadata
                | None -> (
                  match
                    Cemented_block_store.get_cemented_block_level
                      block_store.cemented_store
                      adjusted_hash
                  with
                  | None ->
                      Lwt.return_none
                  | Some level ->
                      (* Lastly, look in the cemented blocks *)
                      Lwt.return
                        (Cemented_block_store.read_block_metadata
                           block_store.cemented_store
                           level) ) ) ))

let store_block block_store block =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      let predecessors = compute_predecessors block_store block in
      Block_cache.push block_store.block_cache block.hash block ;
      Floating_block_store.append_block
        block_store.rw_floating_block_store
        predecessors
        block)

let check_blocks_consistency blocks =
  let rec loop = function
    | [] | [_] ->
        true
    | pred :: (curr :: _ as r) ->
        let is_consistent =
          Block_hash.equal (predecessor curr) pred.hash
          && Compare.Int32.(level curr = Int32.succ (level pred))
        in
        is_consistent && loop r
  in
  loop blocks

(* TODO: remove that ? *)
(* Copy the rw floating block files, remove blocks that are not
   present in the index, then swap files. *)
(* Hypothesis: every block in the index is also in stored in the file *)
let restore_rw_blocks_consistency block_store =
  let tmp_blocks_filename =
    Format.asprintf
      "%s.part.%d"
      Naming.(Filename.get_temp_dir_name () // consistent_rw_blocks)
      Random.(int (bits ()))
  in
  (* Create fresh file *)
  Lwt_unix.openfile tmp_blocks_filename Unix.[O_CREAT; O_TRUNC; O_EXCL] 0o644
  >>= fun dst_fd ->
  let src_fd = block_store.rw_floating_block_store.Floating_block_store.fd in
  Lwt_unix.lseek src_fd 0 Unix.SEEK_END
  >>= fun total_length ->
  Lwt_unix.lseek src_fd 0 Unix.SEEK_SET
  >>= fun _ ->
  let length_size = 4 in
  let length_buffer = Bytes.create length_size in
  let rec loop remaining_bytes =
    Lwt.catch
      (fun () ->
        if remaining_bytes < length_size then
          (* Not enough data left to read *)
          Lwt.return_unit
        else
          Lwt_utils_unix.read_bytes
            ~pos:0
            ~len:length_size
            src_fd
            length_buffer
          >>= fun () ->
          let remaining_bytes = remaining_bytes - length_size in
          let length =
            Data_encoding.(Binary.of_bytes_exn int31 length_buffer)
          in
          if remaining_bytes < length then
            (* Not enough data left to read *)
            Lwt.return_unit
          else
            let block_buffer = Bytes.create length in
            Lwt_utils_unix.read_bytes ~pos:0 ~len:length src_fd block_buffer
            >>= fun () ->
            let remaining_bytes = remaining_bytes - length in
            match
              Data_encoding.Binary.of_bytes_opt
                Block_repr.encoding
                block_buffer
            with
            | None ->
                (* Bad block, discard *)
                Lwt.return_unit
            | Some block ->
                (* If the block is not known in the index, discard it *)
                if
                  Floating_block_index.mem
                    block_store.rw_floating_block_store.floating_block_index
                    block.hash
                then Lwt.return_unit
                else
                  (* The block is consistent, copy the length and the block in the new file *)
                  Lwt_utils_unix.write_bytes
                    ~pos:0
                    ~len:length_size
                    dst_fd
                    length_buffer
                  >>= fun () ->
                  Lwt_utils_unix.write_bytes
                    ~pos:0
                    ~len:length
                    dst_fd
                    block_buffer
                  >>= fun () -> loop remaining_bytes)
      (fun _ -> Lwt.return_unit)
  in
  loop total_length
  >>= fun () ->
  (* Close all fds *)
  Lwt_unix.close src_fd
  >>= fun () ->
  Lwt_unix.close dst_fd
  >>= fun () ->
  let rw_floating_blocks_name =
    Naming.(block_store.chain_dir // floating_blocks RW)
  in
  (* Swap files *)
  Lwt_unix.rename tmp_blocks_filename rw_floating_blocks_name
  >>= fun () ->
  (* Re-open the fd *)
  Lwt_unix.openfile rw_floating_blocks_name Unix.[O_RDWR] 0o644
  >>= fun new_fd ->
  block_store.rw_floating_block_store <-
    {block_store.rw_floating_block_store with fd = new_fd} ;
  Lwt.return_unit

(* Partition the blocks per cycle : each list in the  *)
let split_cycles blocks =
  let rec partition_elements (l, acc) p = function
    | [] ->
        List.rev acc
    | [h] ->
        List.rev (List.rev (h :: l) :: acc)
    | h :: (h' :: t as r) ->
        if p h h' then partition_elements (h :: l, acc) p r
        else partition_elements ([h'], List.rev (h :: l) :: acc) p t
  in
  partition_elements
    ([], [])
    (fun b b' ->
      let b_metadata =
        Option.unopt_assert ~loc:__POS__ (Block_repr.metadata b)
      in
      let b'_metadata =
        Option.unopt_assert ~loc:__POS__ (Block_repr.metadata b')
      in
      Compare.Int32.(
        b_metadata.last_allowed_fork_level
        = b'_metadata.last_allowed_fork_level))
    blocks

let cement_blocks ?ensure_level ~write_metadata block_store blocks =
  (* No need to lock *)
  let {cemented_store; _} = block_store in
  let are_blocks_consistent = check_blocks_consistency blocks in
  ( if not are_blocks_consistent then
    Lwt.fail_invalid_arg
      "cement_blocks: invalid block list to cement, the block list must be \
       correctly chained and their levels growing strictly by one beween each \
       block."
  else Lwt.return_unit )
  >>= fun () ->
  Cemented_block_store.cement_blocks
    cemented_store
    ?ensure_level
    ~write_metadata
    blocks

let store_metadata_chunk block_store blocks =
  let nb_blocks = List.length blocks in
  let first_block = List.hd blocks in
  let first_block_level = Block_repr.level first_block in
  let last_block_level =
    Int32.(add first_block_level (of_int (nb_blocks - 1)))
  in
  let filename = Format.sprintf "%ld_%ld" first_block_level last_block_level in
  Cemented_block_store.cement_blocks_metadata
    block_store.cemented_store
    ~filename
    blocks

(* [retrieve_n_predecessors stores block n] retrieves, at most, the
   [n] [block]'s predecessors (including [block]) from the floating
   stores. The resulting block list may be smaller than [n]. *)
let retrieve_n_predecessors floating_stores block n =
  let rec loop acc predecessor_hash n =
    if n = 0 then (* The list is built in the right order *)
      Lwt.return acc
    else
      Lwt_utils.find_map_s
        (fun floating_store ->
          Floating_block_store.get_block floating_store predecessor_hash)
        floating_stores
      >>= function
      | None ->
          (* The remaining blocks are not present, skip them. *)
          Lwt.return acc
      | Some block ->
          let predecessor_hash = Block_repr.predecessor block in
          loop (block :: acc) predecessor_hash (pred n)
  in
  loop [] block n

(* TODO Document invariants *)
let update_floating_stores ~ro_store ~rw_store ~new_store ~from_block ~to_block
    ~nb_blocks_to_preserve =
  let (_initial_hash, initial_level) = from_block in
  let (final_hash, final_level) = to_block in
  let nb_blocks_to_cement =
    (* (final - initial) + 1 *)
    Int32.(to_int (succ (sub final_level initial_level)))
  in
  let nb_blocks_to_retrieve = max nb_blocks_to_preserve nb_blocks_to_cement in
  retrieve_n_predecessors
    (* Reverse the stores so that the oldest RO is first in the lookup. *)
    [ro_store; rw_store]
    final_hash
    nb_blocks_to_retrieve
  >>= fun (predecessors : Block_repr.t list) ->
  let len = List.length predecessors in
  ( if len < nb_blocks_to_cement then
    Lwt.fail_with
      "block_store.update_floating_stores: could not retrieve enough blocks \
       to cement"
  else
    let blocks_to_cement =
      if len = nb_blocks_to_cement then predecessors
      else List.remove (len - nb_blocks_to_cement) predecessors
    in
    let first_block_to_preserve =
      if len <= nb_blocks_to_preserve then
        (* If not enough blocks are present to preserve, we maintain what we can *)
        List.hd predecessors
      else
        (* len > nb_blocks_to_preserve => len = nb_blocks_to_cement *)
        List.nth predecessors (len - nb_blocks_to_preserve - 1)
    in
    Lwt.return (first_block_to_preserve, blocks_to_cement) )
  >>= fun (first_block_to_preserve, blocks_to_cement) ->
  (* We write back to the new store all the blocks from
     [first_block_to_preserve] to the end of the file *)
  let visited =
    ref
      (Block_hash.Set.singleton
         (Block_repr.predecessor first_block_to_preserve))
  in
  Lwt_list.iter_s
    (fun store ->
      Floating_block_store.iter
        (fun block ->
          if Block_hash.Set.mem (Block_repr.predecessor block) !visited then (
            let hash = Block_repr.hash block in
            let {Floating_block_index.Block_info.predecessors; _} =
              Floating_block_index.find
                store.Floating_block_store.floating_block_index
                hash
            in
            visited := Block_hash.Set.add hash !visited ;
            Floating_block_store.append_block
              ~should_flush:false
              new_store
              predecessors
              block )
          else Lwt.return_unit)
        store)
    [ro_store; rw_store]
  >>= fun () ->
  Floating_block_index.flush
    new_store.Floating_block_store.floating_block_index ;
  Lwt.return blocks_to_cement

let swap_floating_stores block_store ~new_ro_store =
  let chain_dir = block_store.chain_dir in
  let swap floating_store new_kind =
    let open Naming in
    let src_floating_block_index =
      chain_dir
      // floating_block_index floating_store.Floating_block_store.kind
    in
    let src_floating_blocks =
      chain_dir // floating_blocks floating_store.Floating_block_store.kind
    in
    let dest_floating_block_index =
      chain_dir // floating_block_index new_kind
    in
    let dest_floating_blocks = chain_dir // floating_blocks new_kind in
    (* Replaces index *)
    ( if Sys.is_directory dest_floating_block_index then
      Lwt_utils_unix.remove_dir dest_floating_block_index
    else Lwt.return_unit )
    >>= fun () ->
    Lwt_unix.rename src_floating_block_index dest_floating_block_index
    >>= fun () ->
    (* Replace blocks file *)
    Lwt_unix.rename src_floating_blocks dest_floating_blocks
  in
  (* Prepare the swap: close all floating stores. *)
  Lwt_list.iter_s
    Floating_block_store.close
    ( new_ro_store :: block_store.rw_floating_block_store
    :: block_store.ro_floating_block_stores )
  >>= fun () ->
  (* (atomically?) Promote [new_ro] to [ro] *)
  swap new_ro_store Naming.RO
  >>= fun () ->
  (* ...and [new_rw] to [rw]  *)
  swap block_store.rw_floating_block_store Naming.RW
  >>= fun () ->
  (* load the swapped stores *)
  Floating_block_store.init ~chain_dir ~readonly:false RO
  >>= fun ro ->
  block_store.ro_floating_block_stores <- [ro] ;
  Floating_block_store.init ~chain_dir ~readonly:false RW
  >>= fun rw ->
  block_store.rw_floating_block_store <- rw ;
  Lwt.return_unit

let try_remove_temporary_stores block_store =
  let chain_dir = block_store.chain_dir in
  let ro_tmp = Naming.(chain_dir // floating_blocks RO_TMP) in
  let ro_index_tmp = Naming.(chain_dir // floating_block_index RO_TMP) in
  let rw_tmp = Naming.(chain_dir // floating_blocks RW_TMP) in
  let rw_index_tmp = Naming.(chain_dir // floating_block_index RW_TMP) in
  Lwt.catch (fun () -> Lwt_unix.unlink ro_tmp) (fun _ -> Lwt.return_unit)
  >>= fun () ->
  Lwt.catch (fun () -> Lwt_unix.unlink rw_tmp) (fun _ -> Lwt.return_unit)
  >>= fun () ->
  Lwt.catch
    (fun () -> Lwt_utils_unix.remove_dir ro_index_tmp)
    (fun _ -> Lwt.return_unit)
  >>= fun () ->
  Lwt.catch
    (fun () -> Lwt_utils_unix.remove_dir rw_index_tmp)
    (fun _ -> Lwt.return_unit)

let await_merging block_store =
  match block_store.merging_thread with
  | None ->
      Lwt.return_unit
  | Some (_, th) ->
      th

let merge_stores block_store ?(finalizer = fun () -> Lwt.return_unit)
    ~nb_blocks_to_preserve ~history_mode ~from_block ~to_block () =
  (* Do not allow multiple merges *)
  Lwt_mutex.lock block_store.merge_mutex
  >>= fun () ->
  (* Force waiting for a potential previous merging operation *)
  let chain_dir = block_store.chain_dir in
  Lwt_idle_waiter.force_idle block_store.merge_scheduler (fun () ->
      assert (block_store.merging_thread = None) ;
      let (_, final_level) = to_block in
      let (_, initial_level) = from_block in
      assert (Compare.Int32.(final_level >= initial_level)) ;
      (* Move the rw in the ro stores *)
      assert (List.length block_store.ro_floating_block_stores = 1) ;
      let ro_store = List.hd block_store.ro_floating_block_stores in
      let rw_store = block_store.rw_floating_block_store in
      block_store.ro_floating_block_stores <-
        block_store.rw_floating_block_store
        :: block_store.ro_floating_block_stores ;
      Floating_block_store.init ~chain_dir ~readonly:false RW_TMP
      >>= fun new_rw_store ->
      block_store.rw_floating_block_store <- new_rw_store ;
      (* Create the merging thread that we want to run in background *)
      let create_merging_thread () : unit Lwt.t =
        Floating_block_store.init ~chain_dir ~readonly:false RO_TMP
        >>= fun new_ro_store ->
        update_floating_stores
          ~ro_store
          ~rw_store
          ~new_store:new_ro_store
          ~from_block
          ~to_block
          ~nb_blocks_to_preserve
        >>= fun blocks_to_cement ->
        ( match history_mode with
        | History_mode.Archive ->
            (* In archive, we store the metadatas *)
            cement_blocks ~write_metadata:true block_store blocks_to_cement
        | (Full {offset} | Rolling {offset}) when offset > 0 ->
            cement_blocks ~write_metadata:true block_store blocks_to_cement
            >>= fun () ->
            (* Clean-up the files that are below the offset *)
            Cemented_block_store.trigger_gc
              block_store.cemented_store
              history_mode
        | Full {offset} ->
            assert (offset = 0) ;
            (* In full, we do not store the metadata *)
            cement_blocks ~write_metadata:false block_store blocks_to_cement
        | Rolling {offset} ->
            assert (offset = 0) ;
            (* Drop the blocks *)
            Lwt.return_unit )
        >>= fun () ->
        (* Swapping stores: hard-lock *)
        Lwt_idle_waiter.force_idle block_store.merge_scheduler (fun () ->
            swap_floating_stores block_store ~new_ro_store
            >>= fun () ->
            block_store.merging_thread <- None ;
            Lwt.return_unit)
      in
      (* Clean-up on cancel/exn *)
      let merging_thread =
        let cleanup () =
          block_store.merging_thread <- None ;
          try_remove_temporary_stores block_store
          >>= fun () ->
          Lwt_mutex.unlock block_store.merge_mutex ;
          Lwt.return_unit
        in
        Lwt.finalize
          (fun () -> create_merging_thread () >>= fun () -> finalizer ())
          (fun () -> cleanup ())
      in
      block_store.merging_thread <- Some (final_level, merging_thread) ;
      (* Temporary stores in place and the merging thread was started:
         we can now release the hard-lock *)
      Lwt.return_unit)

let create ~chain_dir ~genesis_block =
  let cemented_blocks_dir = Naming.(chain_dir // cemented_blocks_directory) in
  Cemented_block_store.create ~cemented_blocks_dir
  >>= fun cemented_store ->
  Floating_block_store.init ~chain_dir ~readonly:false RO
  >>= fun ro_floating_block_stores ->
  let ro_floating_block_stores = [ro_floating_block_stores] in
  Floating_block_store.init ~chain_dir ~readonly:false RW
  >>= fun rw_floating_block_store ->
  let block_cache = Block_cache.create ~capacity:100 in
  let merge_scheduler = Lwt_idle_waiter.create () in
  let merge_mutex = Lwt_mutex.create () in
  let block_store =
    {
      chain_dir;
      genesis_block;
      cemented_store;
      ro_floating_block_stores;
      rw_floating_block_store;
      block_cache;
      merge_mutex;
      merge_scheduler;
      merging_thread = None;
    }
  in
  store_block block_store genesis_block >>= fun () -> Lwt.return block_store

let load ~chain_dir ~genesis_block ~readonly =
  let cemented_blocks_dir = Naming.(chain_dir // cemented_blocks_directory) in
  Cemented_block_store.load ~cemented_blocks_dir ~readonly
  >>= fun cemented_store ->
  Floating_block_store.init ~chain_dir ~readonly RO
  >>= fun ro_floating_block_store ->
  let ro_floating_block_stores = [ro_floating_block_store] in
  Floating_block_store.init ~chain_dir ~readonly RW
  >>= fun rw_floating_block_store ->
  let block_cache = Block_cache.create ~capacity:100 in
  let merge_scheduler = Lwt_idle_waiter.create () in
  let merge_mutex = Lwt_mutex.create () in
  let block_store =
    {
      chain_dir;
      genesis_block;
      cemented_store;
      ro_floating_block_stores;
      rw_floating_block_store;
      block_cache;
      merge_mutex;
      merge_scheduler;
      merging_thread = None;
    }
  in
  (* Try cleaning up previous artifacts when not in readonly *)
  ( if not readonly then try_remove_temporary_stores block_store
  else Lwt.return_unit )
  >>= fun () -> Lwt.return block_store

let merging_state block_store =
  match block_store.merging_thread with
  | Some (lvl, th) -> (
    match Lwt.state th with
    | Return () ->
        `Done
    | Sleep ->
        `Ongoing lvl
    | Fail _ ->
        assert false )
  | None ->
      `Done

let close block_store =
  (* Wait a bit for the merging to end but cancel it if takes too
     long. *)
  Lwt_unix.with_timeout 5. (fun () -> await_merging block_store)
  >>= fun () ->
  Cemented_block_store.close block_store.cemented_store ;
  Lwt_list.iter_s
    Floating_block_store.close
    ( block_store.rw_floating_block_store
    :: block_store.ro_floating_block_stores )
