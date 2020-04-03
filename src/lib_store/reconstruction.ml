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

type status =
  | Reconstruct_start_default of Block_hash.t * Int32.t
  | Reconstruct_enum
  | Reconstruct_success

let status_pp ppf = function
  | Reconstruct_start_default (h, l) ->
      Format.fprintf
        ppf
        "Starting reconstruct from genesis toward block %a (level %ld)"
        Block_hash.pp
        h
        l
  | Reconstruct_enum ->
      Format.fprintf ppf "Enumerating all blocks to reconstruct"
  | Reconstruct_success ->
      Format.fprintf ppf "The storage was successfully reconstructed."

module Definition = struct
  let name = "reconstruction"

  type t = status Time.System.stamped

  let encoding =
    let open Data_encoding in
    Time.System.stamped_encoding
    @@ union
         [ case
             (Tag 0)
             ~title:"Reconstruct_start_default"
             (obj2
                (req "block_heash" Block_hash.encoding)
                (req "block_level" int32))
             (function
               | Reconstruct_start_default (h, l) -> Some (h, l) | _ -> None)
             (fun (h, l) -> Reconstruct_start_default (h, l));
           case
             (Tag 1)
             ~title:"Reconstruct_enum"
             empty
             (function Reconstruct_enum -> Some () | _ -> None)
             (fun () -> Reconstruct_enum);
           case
             (Tag 2)
             ~title:"Reconstruct_success"
             empty
             (function Reconstruct_success -> Some () | _ -> None)
             (fun () -> Reconstruct_success) ]

  let pp ppf (status : t) = Format.fprintf ppf "%a" status_pp status.data

  let doc = "Reconstruction status."

  let level (status : t) =
    match status.data with
    | Reconstruct_start_default _ | Reconstruct_enum | Reconstruct_success ->
        Internal_event.Notice
end

module Event_reconstruction = Internal_event.Make (Definition)

let lwt_emit (status : status) =
  let time = Systime_os.now () in
  Event_reconstruction.emit
    ~section:(Internal_event.Section.make_sanitized [Definition.name])
    (fun () -> Time.System.stamp ~time status)
  >>= function
  | Ok () ->
      Lwt.return_unit
  | Error el ->
      Format.kasprintf
        Lwt.fail_with
        "Reconstruction_event.emit: %a"
        pp_print_error
        el

open Reconstruction_errors

let check_context_hash_consistency block_validation_result block_header =
  let expected = block_header.Block_header.shell.context in
  let got = block_validation_result.Block_validation.context_hash in
  fail_unless
    (Context_hash.equal expected got)
    (Reconstruction_failure
       (Context_hash_mismatch (block_header, expected, got)))

let apply_context chain_store context_index chain_id ~user_activated_upgrades
    ~user_activated_protocol_overrides block =
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
  >>=? fun ({validation_store; block_metadata; ops_metadata; _} :
             Block_validation.result) ->
  check_context_hash_consistency validation_store block_header
  >>=? fun () ->
  return
    {
      Store.Block.message = validation_store.message;
      max_operations_ttl = validation_store.max_operations_ttl;
      last_allowed_fork_level = validation_store.last_allowed_fork_level;
      block_metadata;
      operations_metadata = ops_metadata;
    }

let reconstruct_chunks ~notify chain_store context_index
    ~user_activated_upgrades ~user_activated_protocol_overrides limit history
    cemented_cycles =
  let chain_id = Store.Chain.chain_id chain_store in
  Store.Chain.genesis_block chain_store
  >|= Store.Block.hash
  >>= fun genesis_hash ->
  Store.Block.read_block chain_store genesis_hash
  >>=? fun genesis_b ->
  Store.Block.get_block_metadata chain_store genesis_b
  >>=? fun genesis_metadata ->
  let find block =
    List.find_opt
      (fun ({end_level; _} : Cemented_block_store.cemented_blocks_file) ->
        Store.Block.level block = end_level)
      cemented_cycles
  in
  let exist block = match find block with None -> false | Some _ -> true in
  let rec reconstruct_chunks level lafl metadata_chunk =
    if level = limit then return_unit
    else
      let block_hash = history.(level) in
      Store.Block.read_block chain_store block_hash
      >>=? fun block ->
      apply_context
        chain_store
        context_index
        chain_id
        ~user_activated_upgrades
        ~user_activated_protocol_overrides
        block
      >>=? fun metadata ->
      notify ()
      >>= fun () ->
      let new_metadata_chunk = (block, metadata) :: metadata_chunk in
      ( if lafl < metadata.last_allowed_fork_level then
        (* New cycle : store the chunck and continue with an empty one*)
        Store.Block.store_block_metadata
          chain_store
          (List.rev new_metadata_chunk)
        >>= fun () ->
        reconstruct_chunks (level + 1) metadata.last_allowed_fork_level []
      else if
      (* For the first cycles, the last allowed fork level is not updated.
         We must rely on the previously stored cycles.*)
      lafl = 0l && exist block
    then
        Store.Block.store_block_metadata
          chain_store
          (List.rev new_metadata_chunk)
        >>= fun () ->
        reconstruct_chunks (level + 1) metadata.last_allowed_fork_level []
      else
        reconstruct_chunks
          (level + 1)
          metadata.last_allowed_fork_level
          new_metadata_chunk )
      >>=? fun () -> return_unit
  in
  reconstruct_chunks 0 0l [(genesis_b, genesis_metadata)]

(* Reconstruct the storage (without checking if the context/store is already populated) *)
let reconstruct_cemented chain_store context_index ~user_activated_upgrades
    ~user_activated_protocol_overrides (history_list : Block_hash.t list) =
  let history = Array.of_list history_list in
  let limit = Array.length history in
  let block_store = Store.unsafe_get_block_store chain_store in
  Cemented_block_store.load_table
    ~cemented_blocks_dir:block_store.cemented_store.cemented_blocks_dir
  >|= Array.to_list
  >>= fun cemented_cycles ->
  Lwt_utils_unix.display_progress
    ~pp_print_step:(fun ppf i ->
      Format.fprintf ppf "Reconstructing cemented blocks: %i/%i..." i limit)
    (fun notify ->
      reconstruct_chunks
        ~notify
        chain_store
        context_index
        ~user_activated_upgrades
        ~user_activated_protocol_overrides
        limit
        history
        cemented_cycles)
  >>=? fun () -> return_unit

let reconstruct_floating chain_store context_index ~user_activated_upgrades
    ~user_activated_protocol_overrides =
  let chain_id = Store.Chain.chain_id chain_store in
  let block_store = Store.unsafe_get_block_store chain_store in
  let chain_dir = block_store.chain_dir in
  Floating_block_store.init ~chain_dir ~readonly:false RO_TMP
  >>= fun new_ro_store ->
  let floating_stores =
    [ List.hd block_store.ro_floating_block_stores;
      block_store.rw_floating_block_store ]
  in
  Lwt_utils_unix.display_progress
    ~pp_print_step:(fun ppf i ->
      Format.fprintf ppf "Reconstructing floating blocks: %i..." i)
    (fun notify ->
      Error_monad.iter_s
        (fun fs ->
          Floating_block_store.iter
            (fun block ->
              let level = Block_repr.level block in
              (* It is needed to read the metadata using the cemented_block_store to
           avoid the cache mechanism which stores blocks without metadata *)
              let metadata_opt =
                Cemented_block_store.read_block_metadata
                  block_store.cemented_store
                  level
              in
              ( match metadata_opt with
              | None ->
                  (* When the metadata is not available in the cemented_block_store,
              it means that the block (in floating) was not cemented yet. It is
              thus needed to recompute its metadata + context *)
                  apply_context
                    chain_store
                    context_index
                    chain_id
                    ~user_activated_upgrades
                    ~user_activated_protocol_overrides
                    (Store.Block.of_repr block)
                  >>=? fun metadata -> return metadata
              | Some m ->
                  return (Store.Block.of_repr_metadata m) )
              >>=? fun metadata ->
              (* We should use find_predecessors but it will deadlock *)
              let predecessors =
                Block_store.compute_predecessors block_store block
              in
              Floating_block_store.append_block
                ~should_flush:false
                new_ro_store
                predecessors
                {
                  block with
                  metadata = Some (Store.Block.repr_metadata metadata);
                }
              >>= fun () -> notify () >>= fun () -> return_unit)
            fs
          >>= fun () -> return_unit)
        floating_stores)
  >>=? fun () ->
  Floating_block_store.init ~chain_dir ~readonly:false RW_TMP
  >>= fun new_rw_store ->
  block_store.rw_floating_block_store <- new_rw_store ;
  Floating_block_index.flush
    new_ro_store.Floating_block_store.floating_block_index ;
  (* Swapping stores: hard-lock *)
  Lwt_idle_waiter.force_idle block_store.merge_scheduler (fun () ->
      Block_store.swap_floating_stores block_store ~new_ro_store
      >>= fun () ->
      block_store.merging_thread <- None ;
      Lwt.return_unit)
  >>= fun () -> return_unit

(* Loads the list of hashes to reconstruct *)
let gather_history chain_store low_limit block_hash acc =
  let rec aux block_hash acc =
    Store.Block.read_block_opt chain_store block_hash
    >>= function
    | Some block ->
        if Store.Block.level block = low_limit then return (block_hash :: acc)
        else aux (Store.Block.predecessor block) (block_hash :: acc)
    | None ->
        fail (Reconstruction_failure (Missing_block block_hash))
  in
  aux block_hash acc

(* Only Full modes with any offset can be reconstructed *)
let check_history_mode_compatibility chain_store =
  match Store.Chain.history_mode chain_store with
  | History_mode.(Full {offset}) ->
      return offset
  | _ as history_mode ->
      fail (Cannot_reconstruct history_mode)

(* TODO: should we expose in in the Cemented_block_store ? *)
let get_lowest_cemented_block_with_metadata chain_store offset =
  let block_store = Store.unsafe_get_block_store chain_store in
  Cemented_block_store.load_table
    ~cemented_blocks_dir:block_store.cemented_store.cemented_blocks_dir
  >|= Array.to_list >|= List.rev
  >>= fun cemented_cycles ->
  let lcl =
    try
      let ({start_level; _} : Cemented_block_store.cemented_blocks_file) =
        List.nth
          cemented_cycles
          (min (offset - 1) (List.length cemented_cycles - 1))
      in
      start_level
    with _ -> 0l
  in
  Store.Block.read_block_by_level chain_store lcl

let default_starting_block chain_store offset =
  get_lowest_cemented_block_with_metadata chain_store offset
  >>=? fun lcb ->
  let lcb_level = Store.Block.level lcb in
  Store.Chain.current_head chain_store
  >>= fun current_head ->
  Store.Block.get_block_metadata chain_store current_head
  >>=? fun head_metadata ->
  let head_lafl = Store.Block.last_allowed_fork_level head_metadata in
  Store.Chain.savepoint chain_store
  >>= fun (_, savepoint_level) ->
  let starting_level =
    if head_lafl < savepoint_level then
      (* Happens when the storage was imported from a snapshot recently and the
         savepoint was not updated yet *)
      head_lafl
    else lcb_level
  in
  Store.Block.read_block_by_level chain_store starting_level

let restore_constants chain_store genesis_block =
  Store.Chain.set_history_mode chain_store History_mode.Archive
  >>= fun () ->
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

let reconstruct ?patch_context ~store_root ~context_root ~(genesis : Genesis.t)
    ~user_activated_upgrades ~user_activated_protocol_overrides =
  Store.init
    ?patch_context
    ~store_dir:store_root
    ~context_dir:context_root
    ~allow_testchains:false
    genesis
  >>=? fun store ->
  let context_index = Store.context_index store in
  let chain_store = Store.main_chain_store store in
  check_history_mode_compatibility chain_store
  >>=? fun full_offset ->
  let low_limit = 1l in
  default_starting_block chain_store full_offset
  >>=? fun starting_block ->
  fail_when
    (Store.Block.level starting_block = 0l)
    (Reconstruction_failure Nothing_to_reconstruct)
  >>=? fun () ->
  lwt_emit
    (Reconstruct_start_default
       (Store.Block.hash starting_block, Store.Block.level starting_block))
  >>= fun () ->
  lwt_emit Reconstruct_enum
  >>= fun () ->
  gather_history chain_store low_limit (Store.Block.hash starting_block) []
  >>=? fun hash_history ->
  reconstruct_cemented
    chain_store
    context_index
    ~user_activated_upgrades
    ~user_activated_protocol_overrides
    hash_history
  >>=? fun () ->
  let chain_store = Store.main_chain_store store in
  reconstruct_floating
    chain_store
    context_index
    ~user_activated_upgrades
    ~user_activated_protocol_overrides
  >>=? fun () ->
  restore_constants chain_store genesis.block
  >>=? fun () -> lwt_emit Reconstruct_success >>= fun () -> return_unit
