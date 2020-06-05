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
open Store_errors

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
  mutable merging_thread : (int32 * unit tzresult Lwt.t) option;
}

type t = block_store

type key = Block of (Block_hash.t * int)

let cemented_block_store {cemented_store; _} = cemented_store

let floating_block_stores {ro_floating_block_stores; rw_floating_block_store; _}
    =
  List.rev (rw_floating_block_store :: ro_floating_block_stores)

(** [global_predecessor_lookup chain_block_store hash nth] retrieves
    the 2^[nth] predecessor's hash from the block with corresponding
    [hash] by checking all stores iteratively. Returns [None] if the
    predecessor is not found or if it is below genesis. *)
let global_predecessor_lookup block_store hash pow_nth =
  (* pow_nth = 0 => direct predecessor *)
  (* Look in the RW block_store, then RO stores and finally in the
     cemented store *)
  Lwt_utils.find_map_s
    (fun floating_store ->
      Floating_block_store.find_predecessors floating_store hash
      >>= function
      | None ->
          Lwt.return_none
      | Some predecessors ->
          Lwt.return (List.nth_opt predecessors pow_nth))
    ( block_store.rw_floating_block_store
    :: block_store.ro_floating_block_stores )
  >>= function
  | Some hash ->
      Lwt.return_some hash
  | None -> (
    (* It must be cemented *)
    match
      Cemented_block_store.get_cemented_block_level
        block_store.cemented_store
        hash
    with
    | None ->
        Lwt.return_none
    | Some level ->
        (* level - 2^n *)
        let pred_level =
          max
            (Block_repr.level block_store.genesis_block)
            Int32.(sub level (shift_left 1l pow_nth))
        in
        Lwt.return
          (Cemented_block_store.get_cemented_block_hash
             block_store.cemented_store
             pred_level) )

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
    if dist = Floating_block_index.Block_info.max_predecessors then
      Lwt.return predecessors_acc
    else
      global_predecessor_lookup block_store pred (dist - 1)
      >>= function
      | None ->
          Lwt.return predecessors_acc
      | Some pred' ->
          loop (pred' :: predecessors_acc) pred' (dist + 1)
  in
  let predecessor = predecessor block in
  if Block_hash.equal block.hash predecessor then
    (* genesis *)
    Lwt.return [block.hash]
  else
    loop [predecessor] predecessor 1
    >>= fun rev_preds -> Lwt.return (List.rev rev_preds)

(** [get_hash chain_store key] retrieves the block which is at
    [distance] from the block with corresponding [hash] by every store
    iteratively. *)
let get_hash block_store (Block (block_hash, offset)) =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      let closest_power_two_and_rest n =
        if n < 0 then assert false
        else
          let rec loop cnt n rest =
            if n <= 1 then (cnt, rest)
            else loop (cnt + 1) (n / 2) (rest + ((1 lsl cnt) * (n mod 2)))
          in
          loop 0 n 0
      in
      (* actual predecessor function *)
      if offset = 0 then return_some block_hash
      else if offset < 0 then fail (Wrong_predecessor (block_hash, offset))
      else
        let rec loop block_hash offset =
          if offset = 1 then
            global_predecessor_lookup block_store block_hash 0
            >>= fun pred -> return pred
          else
            let (power, rest) = closest_power_two_and_rest offset in
            let (power, rest) =
              if power < Floating_block_index.Block_info.max_predecessors then
                (power, rest)
              else
                let power =
                  Floating_block_index.Block_info.max_predecessors - 1
                in
                let rest = offset - (1 lsl power) in
                (power, rest)
            in
            global_predecessor_lookup block_store block_hash power
            >>= function
            | None ->
                return_none
            | Some pred ->
                if rest = 0 then return_some pred
                  (* landed on the requested predecessor *)
                else loop pred rest
          (* need to jump further back *)
        in
        loop block_hash offset)

let mem block_store key =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      get_hash block_store key
      >>=? function
      | None ->
          return_false
      | Some predecessor_hash
        when Block_hash.equal block_store.genesis_block.hash predecessor_hash
        ->
          return_true
      | Some predecessor_hash ->
          Lwt_list.exists_s
            (fun store -> Floating_block_store.mem store predecessor_hash)
            ( block_store.rw_floating_block_store
            :: block_store.ro_floating_block_stores )
          >>= fun is_known_in_floating ->
          return
            ( is_known_in_floating
            || Cemented_block_store.is_cemented
                 block_store.cemented_store
                 predecessor_hash ))

let read_block ~read_metadata block_store key_kind =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      (* Resolve the hash *)
      get_hash block_store key_kind
      >>=? function
      | None ->
          return_none
      | Some adjusted_hash ->
          if Block_hash.equal block_store.genesis_block.hash adjusted_hash then
            return_some block_store.genesis_block
          else
            let fetch_block adjusted_hash =
              (* First look in the floating stores *)
              Lwt_utils.find_map_s
                (fun store ->
                  Floating_block_store.read_block store adjusted_hash)
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
              adjusted_hash
            >>= fun block -> return block)

let read_block_metadata block_store key_kind =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      (* Resolve the hash *)
      get_hash block_store key_kind
      >>=? function
      | None ->
          return_none
      | Some adjusted_hash -> (
          if Block_hash.equal block_store.genesis_block.hash adjusted_hash then
            return (Block_repr.metadata block_store.genesis_block)
          else
            (* First look in the floating stores *)
            Lwt_utils.find_map_s
              (fun store ->
                Floating_block_store.read_block store adjusted_hash)
              ( block_store.rw_floating_block_store
              :: block_store.ro_floating_block_stores )
            >>= function
            | Some block ->
                return block.metadata
            | None -> (
              (* Lastly, look in the cemented blocks *)
              match
                Cemented_block_store.get_cemented_block_level
                  block_store.cemented_store
                  adjusted_hash
              with
              | None ->
                  return_none
              | Some level ->
                  return
                    (Cemented_block_store.read_block_metadata
                       block_store.cemented_store
                       level) ) ))

let store_block block_store block =
  Lwt_idle_waiter.task block_store.merge_scheduler (fun () ->
      compute_predecessors block_store block
      >>= fun predecessors ->
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

let cement_blocks ?(check_consistency = true) ~write_metadata block_store
    blocks =
  (* No need to lock *)
  let {cemented_store; _} = block_store in
  let are_blocks_consistent = check_blocks_consistency blocks in
  fail_unless are_blocks_consistent Invalid_blocks_to_cement
  >>=? fun () ->
  Cemented_block_store.cement_blocks
    ~check_consistency
    cemented_store
    ~write_metadata
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
          Floating_block_store.read_block floating_store predecessor_hash)
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

let await_merging block_store =
  match block_store.merging_thread with
  | None ->
      return_unit
  | Some (_, th) ->
      th

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
  ( if len < nb_blocks_to_cement then fail Cannot_update_floating_store
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
    return (first_block_to_preserve, blocks_to_cement) )
  >>=? fun (first_block_to_preserve, blocks_to_cement) ->
  (* We write back to the new store all the blocks from
     [first_block_to_preserve] to the end of the file *)
  let visited =
    ref
      (Block_hash.Set.singleton
         (Block_repr.predecessor first_block_to_preserve))
  in
  iter_s
    (fun store ->
      Floating_block_store.iter_seq
        (fun (block, predecessors) ->
          if Block_hash.Set.mem (Block_repr.predecessor block) !visited then (
            let hash = Block_repr.hash block in
            visited := Block_hash.Set.add hash !visited ;
            Floating_block_store.append_block new_store predecessors block
            >>= return )
          else return_unit)
        store)
    [ro_store; rw_store]
  >>=? fun () -> return blocks_to_cement

let find_floating_store_by_kind block_store kind =
  List.find_opt
    (fun floating_store -> kind = Floating_block_store.kind floating_store)
    ( block_store.rw_floating_block_store
    :: block_store.ro_floating_block_stores )

let swap_floating_store block_store ~src:floating_store ~dst_kind =
  let src_kind = Floating_block_store.kind floating_store in
  fail_when (src_kind = dst_kind) Wrong_floating_kind_swap
  >>=? fun () ->
  (* If the destination floating store exists, try closing it. *)
  ( match find_floating_store_by_kind block_store dst_kind with
  | Some floating_store ->
      Floating_block_store.close floating_store
  | None ->
      Lwt.return_unit )
  >>= fun () ->
  (* ... also close the src floating store *)
  Floating_block_store.close floating_store
  >>= fun () ->
  let open Naming in
  (* Replace index *)
  let dst_floating_block_index_dir =
    block_store.chain_dir // floating_block_index dst_kind
  in
  let dst_floating_blocks =
    block_store.chain_dir // floating_blocks dst_kind
  in
  (* Remove old index *)
  ( if Sys.is_directory dst_floating_block_index_dir then
    Lwt_utils_unix.remove_dir dst_floating_block_index_dir
  else Lwt.return_unit )
  >>= fun () ->
  let src_floating_block_index_dir =
    block_store.chain_dir // floating_block_index src_kind
  in
  let src_floating_blocks =
    block_store.chain_dir // floating_blocks src_kind
  in
  ( if Sys.file_exists src_floating_block_index_dir then
    Lwt_unix.rename src_floating_block_index_dir dst_floating_block_index_dir
  else Lwt.return_unit )
  >>= fun () ->
  (* Replace blocks file *)
  Lwt_unix.rename src_floating_blocks dst_floating_blocks
  >>= fun () -> return_unit

let swap_all_floating_stores block_store ~new_ro_store =
  (* (atomically?) Promote [new_ro] to [ro] *)
  swap_floating_store block_store ~src:new_ro_store ~dst_kind:Naming.RO
  >>=? fun () ->
  (* ...and [new_rw] to [rw]  *)
  swap_floating_store
    block_store
    ~src:block_store.rw_floating_block_store
    ~dst_kind:Naming.RW
  >>=? fun () ->
  (* Load the swapped stores *)
  let chain_dir = block_store.chain_dir in
  Floating_block_store.init ~chain_dir ~readonly:false RO
  >>= fun ro ->
  block_store.ro_floating_block_stores <- [ro] ;
  Floating_block_store.init ~chain_dir ~readonly:false RW
  >>= fun rw ->
  block_store.rw_floating_block_store <- rw ;
  return_unit

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

let merge_stores block_store ?(finalizer = fun () -> Lwt.return_unit)
    ~nb_blocks_to_preserve ~history_mode ~from_block ~to_block () =
  (* Do not allow multiple merges *)
  Lwt_mutex.lock block_store.merge_mutex
  >>= fun () ->
  (* Force waiting for a potential previous merging operation *)
  let chain_dir = block_store.chain_dir in
  (* TODO improvement for non-blocking cases: replace force_idle by
     when_idle_force (somehow) *)
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
      let create_merging_thread () : unit tzresult Lwt.t =
        Floating_block_store.init ~chain_dir ~readonly:false RO_TMP
        >>= fun new_ro_store ->
        Lwt.catch
          (fun () ->
            update_floating_stores
              ~ro_store
              ~rw_store
              ~new_store:new_ro_store
              ~from_block
              ~to_block
              ~nb_blocks_to_preserve
            >>=? fun blocks_to_cement ->
            match history_mode with
            | History_mode.Archive ->
                (* In archive, we store the metadatas *)
                cement_blocks ~write_metadata:true block_store blocks_to_cement
            | (Full {offset} | Rolling {offset}) when offset > 0 ->
                cement_blocks ~write_metadata:true block_store blocks_to_cement
                >>=? fun () ->
                (* Clean-up the files that are below the offset *)
                Cemented_block_store.trigger_gc
                  block_store.cemented_store
                  history_mode
                >>= return
            | Full {offset} ->
                assert (offset = 0) ;
                (* In full, we do not store the metadata *)
                cement_blocks
                  ~write_metadata:false
                  block_store
                  blocks_to_cement
            | Rolling {offset} ->
                assert (offset = 0) ;
                (* Drop the blocks *)
                return_unit)
          (fun exn ->
            Floating_block_store.close new_ro_store >>= fun () -> Lwt.fail exn)
        >>=? fun () ->
        (* Swapping stores: hard-lock *)
        Lwt_idle_waiter.force_idle block_store.merge_scheduler (fun () ->
            swap_all_floating_stores block_store ~new_ro_store
            >>=? fun () ->
            (* The clean-up will unset the [merging_thread] *)
            return_unit)
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
        Lwt.catch
          (fun () ->
            Lwt.finalize
              (fun () ->
                create_merging_thread ()
                >>=? fun () -> finalizer () >>= fun () -> return_unit)
              (fun () -> cleanup ()))
          (fun exn ->
            Format.kasprintf
              (fun t -> fail (Merge_error t))
              "%s"
              (Printexc.to_string exn))
      in
      Lwt.async (fun () ->
          merging_thread
          >>= function
          | Ok () ->
              Lwt.return_unit
          | Error ([Merge_error _; _] as merge_error) ->
              Format.printf "%a" pp_print_error merge_error ;
              Lwt.return_unit
          | _ ->
              (* FIXME: what to do? *)
              Lwt.return_unit) ;
      block_store.merging_thread <- Some (final_level, merging_thread) ;
      (* Temporary stores in place and the merging thread was
         started: we can now release the hard-lock. *)
      Lwt.return_unit)

let create ~chain_dir ~genesis_block =
  let cemented_blocks_dir = Naming.(chain_dir // cemented_blocks_directory) in
  Cemented_block_store.init ~readonly:false ~cemented_blocks_dir
  >>=? fun cemented_store ->
  Floating_block_store.init ~chain_dir ~readonly:false RO
  >>= fun ro_floating_block_stores ->
  let ro_floating_block_stores = [ro_floating_block_stores] in
  Floating_block_store.init ~chain_dir ~readonly:false RW
  >>= fun rw_floating_block_store ->
  (* TODO: parametrize the block cache *)
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
  store_block block_store genesis_block >>= fun () -> return block_store

let load ~chain_dir ~genesis_block ~readonly =
  (* TODO fail if dir does not exists *)
  let cemented_blocks_dir = Naming.(chain_dir // cemented_blocks_directory) in
  Cemented_block_store.init ~cemented_blocks_dir ~readonly
  >>=? fun cemented_store ->
  Floating_block_store.init ~chain_dir ~readonly RO
  >>= fun ro_floating_block_store ->
  let ro_floating_block_stores = [ro_floating_block_store] in
  Floating_block_store.init ~chain_dir ~readonly RW
  >>= fun rw_floating_block_store ->
  (* TODO: parametrize the block cache *)
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
  >>= fun () -> return block_store

(* TODO? Register the progress of the merging thread. *)
let close block_store =
  (* Wait a bit for the merging to end but hard-stop it if it takes
     too long. *)
  Lwt_unix.with_timeout 5. (fun () ->
      await_merging block_store >>= fun _ -> Lwt.return_unit)
  >>= fun () ->
  Cemented_block_store.close block_store.cemented_store ;
  Lwt_list.iter_s
    Floating_block_store.close
    ( block_store.rw_floating_block_store
    :: block_store.ro_floating_block_stores )
