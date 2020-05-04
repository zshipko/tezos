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

open Snapshots_events
open Store_types

type error +=
  | Incompatible_history_mode of {
      requested : History_mode.t;
      stored : History_mode.t;
    }
  | Invalid_export_block of {
      block : Block_hash.t option;
      reason :
        [ `Pruned
        | `Pruned_pred
        | `Unknown
        | `Caboose
        | `Genesis
        | `Not_enough_pred ];
    }
  | (* TODO *) Snapshot_import_failure of string
  | Wrong_protocol_hash of Protocol_hash.t
  | Inconsistent_operation_hashes of
      (Operation_list_list_hash.t * Operation_list_list_hash.t)
  | Invalid_block_specification of string
  | Cannot_find_protocol_sources of Protocol_hash.t
  | Protocol_hash_and_protocol_sources_mismatch of {
      provided_protocol_hash : Protocol_hash.t;
      computed_protocol_hash : Protocol_hash.t;
    }
  | Provided_protocol_sources_and_embedded_protocol_sources_mismatch of {
      protocol_hash : Protocol_hash.t;
      computed_protocol_hash : Protocol_hash.t;
      computed_protocol_hash_from_embedded_sources : Protocol_hash.t;
    }

let () =
  let open Data_encoding in
  register_error_kind
    `Permanent
    ~id:"snapshots.incompatible_export"
    ~title:"Incompatible snapshot export"
    ~description:
      "The requested history mode for the snapshot is not compatible with the \
       given storage."
    ~pp:(fun ppf (requested, stored) ->
      Format.fprintf
        ppf
        "The requested history mode (%a) for the snapshot export is not \
         compatible with the given storage, running with history mode (%a)."
        History_mode.pp_short
        requested
        History_mode.pp_short
        stored)
    (obj2
       (req "stored" History_mode.encoding)
       (req "requested" History_mode.encoding))
    (function
      | Incompatible_history_mode {requested; stored} ->
          Some (requested, stored)
      | _ ->
          None)
    (fun (requested, stored) -> Incompatible_history_mode {requested; stored}) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.invalid_export_block"
    ~title:"Invalid export block"
    ~description:"Invalid block provided for snapshot export."
    ~pp:(fun ppf (hash, reason) ->
      Format.fprintf
        ppf
        "The selected block %a is invalid: %s."
        (Option.pp ~default:"(n/a)" Block_hash.pp)
        hash
        ( match reason with
        | `Pruned ->
            "the block is too old and has been pruned"
        | `Pruned_pred ->
            "its predecessor has been pruned"
        | `Unknown ->
            "the block is unknown"
        | `Genesis ->
            "the genesis block is not a valid export point"
        | `Caboose ->
            "the caboose block is not a valid export point"
        | `Not_enough_pred ->
            "not enough of the block's predecessors are known" ))
    (obj2
       (opt "block" Block_hash.encoding)
       (req
          "reason"
          (string_enum
             [ ("pruned", `Pruned);
               ("pruned_pred", `Pruned_pred);
               ("unknown", `Unknown);
               ("genesis", `Genesis);
               ("caboose", `Genesis);
               ("not_enough_pred", `Not_enough_pred) ])))
    (function
      | Invalid_export_block {block; reason} ->
          Some (block, reason)
      | _ ->
          None)
    (fun (block, reason) -> Invalid_export_block {block; reason}) ;
  register_error_kind
    `Permanent
    ~id:"SnapshotImportFailure"
    ~title:"Snapshot import failure"
    ~description:"The imported snapshot is malformed."
    ~pp:(fun ppf msg ->
      Format.fprintf
        ppf
        "The data contained in the snapshot is not valid. The import \
         mechanism failed to validate the file: %s."
        msg)
    (obj1 (req "message" string))
    (function Snapshot_import_failure str -> Some str | _ -> None)
    (fun str -> Snapshot_import_failure str) ;
  register_error_kind
    `Permanent
    ~id:"WrongProtocolHash"
    ~title:"Wrong protocol hash"
    ~description:"Wrong protocol hash"
    ~pp:(fun ppf p ->
      Format.fprintf
        ppf
        "Wrong protocol hash (%a) found in snapshot. Snapshot is corrupted."
        Protocol_hash.pp
        p)
    (obj1 (req "protocol_hash" Protocol_hash.encoding))
    (function Wrong_protocol_hash p -> Some p | _ -> None)
    (fun p -> Wrong_protocol_hash p) ;
  register_error_kind
    `Permanent
    ~id:"InconsistentOperationHashes"
    ~title:"Inconsistent operation hashes"
    ~description:"The operations given do not match their hashes."
    ~pp:(fun ppf (oph, oph') ->
      Format.fprintf
        ppf
        "Inconsistent operation hashes. Expected: %a, got %a."
        Operation_list_list_hash.pp
        oph
        Operation_list_list_hash.pp
        oph')
    (obj2
       (req "expected_operation_hashes" Operation_list_list_hash.encoding)
       (req "received_operation_hashes" Operation_list_list_hash.encoding))
    (function
      | Inconsistent_operation_hashes (oph, oph') ->
          Some (oph, oph')
      | _ ->
          None)
    (fun (oph, oph') -> Inconsistent_operation_hashes (oph, oph')) ;
  register_error_kind
    `Permanent
    ~id:"InvalidBlockSpecification"
    ~title:"Invalid block specification"
    ~description:"Invalid specification of block to import"
    ~pp:(fun ppf str ->
      Format.fprintf
        ppf
        "Cannot check the given block to import based on %s. You must specify \
         a valid block hash."
        str)
    (obj1 (req "str" string))
    (function Invalid_block_specification s -> Some s | _ -> None)
    (fun s -> Invalid_block_specification s) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.cannot_find_protocol_sources"
    ~title:"Cannot find protocol sources"
    ~description:"Cannot find protocol sources when exporting snapshot."
    ~pp:(fun ppf protocol_hash ->
      Format.fprintf
        ppf
        "Cannot find protocol sources while exporting snapshot when looking \
         for protocol hash %a."
        Protocol_hash.pp
        protocol_hash)
    (obj1 (req "protocol_hash" Protocol_hash.encoding))
    (function
      | Cannot_find_protocol_sources protocol_hash ->
          Some protocol_hash
      | _ ->
          None)
    (fun protocol_hash -> Cannot_find_protocol_sources protocol_hash) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.protocol_hash_and_protocol_sources_mismatch"
    ~title:"Protocol hash and protocol sources mismatch"
    ~description:
      "The protocol hash computed from protocol sources does not match the \
       one provided."
    ~pp:(fun ppf (protocol_hash, computed_protocol_hash) ->
      Format.fprintf
        ppf
        "The protocol hash provided in a snapshot protocol data does not \
         match the provided sources: computed %a but found %a."
        Protocol_hash.pp
        protocol_hash
        Protocol_hash.pp
        computed_protocol_hash)
    (obj2
       (req "protocol_hash" Protocol_hash.encoding)
       (req "computed_protocol_hash" Protocol_hash.encoding))
    (function
      | Protocol_hash_and_protocol_sources_mismatch
          {provided_protocol_hash; computed_protocol_hash} ->
          Some (provided_protocol_hash, computed_protocol_hash)
      | _ ->
          None)
    (fun (provided_protocol_hash, computed_protocol_hash) ->
      Protocol_hash_and_protocol_sources_mismatch
        {provided_protocol_hash; computed_protocol_hash}) ;
  register_error_kind
    `Permanent
    ~id:
      "snapshot.provided_protocol_sources_and_embedded_protocol_sources_mismatch"
    ~title:"Provided protocol sources and embedded protocol sources mismatch"
    ~description:
      "Provided protocol sources and embedded protocol sources mismatch."
    ~pp:
      (fun ppf
           ( protocol_hash,
             computed_protocol_hash,
             computed_protocol_hash_from_embedded_sources ) ->
      Format.fprintf
        ppf
        "@[The protocol sources provided in a snapshot protocol data do not \
         match the corresponding embedded sources in the node, according to \
         the provided protocol hash.@.@.Provided protocol hash: \
         %a@.@.Computed protocol hash from snapshot sources: %a@.@.Computed \
         protocol hash from embedded sources: %a.@]"
        Protocol_hash.pp
        protocol_hash
        Protocol_hash.pp
        computed_protocol_hash
        Protocol_hash.pp
        computed_protocol_hash_from_embedded_sources)
    (obj3
       (req "protocol_hash" Protocol_hash.encoding)
       (req "computed_protocol_hash" Protocol_hash.encoding)
       (req
          "computed_protocol_hash_from_embedded_sources"
          Protocol_hash.encoding))
    (function
      | Provided_protocol_sources_and_embedded_protocol_sources_mismatch
          { protocol_hash;
            computed_protocol_hash;
            computed_protocol_hash_from_embedded_sources } ->
          Some
            ( protocol_hash,
              computed_protocol_hash,
              computed_protocol_hash_from_embedded_sources )
      | _ ->
          None)
    (fun ( protocol_hash,
           computed_protocol_hash,
           computed_protocol_hash_from_embedded_sources ) ->
      Provided_protocol_sources_and_embedded_protocol_sources_mismatch
        {
          protocol_hash;
          computed_protocol_hash;
          computed_protocol_hash_from_embedded_sources;
        })

let write_snapshot_metadata metadata file =
  let metadata_json =
    Data_encoding.Json.construct Snapshot_version.metadata_encoding metadata
  in
  Lwt_utils_unix.Json.write_file file metadata_json
  >>= function
  | Error err ->
      Format.kasprintf Lwt.fail_with "%a" Error_monad.pp_print_error err
  | Ok () ->
      Lwt.return_unit

let read_snapshot_metadata file =
  Lwt_utils_unix.Json.read_file file
  >>= function
  | Error err ->
      Format.kasprintf Lwt.fail_with "%a" Error_monad.pp_print_error err
  | Ok json ->
      Lwt.return
        (Data_encoding.Json.destruct Snapshot_version.metadata_encoding json)

let copy_cemented_blocks ~src_cemented_dir ~dst_cemented_dir
    (files : Cemented_block_store.cemented_blocks_file list) =
  let open Cemented_block_store in
  let nb_cycles = List.length files in
  protect (fun () ->
      Lwt_utils_unix.display_progress
        ~pp_print_step:(fun fmt i ->
          Format.fprintf
            fmt
            "Copying cemented blocks: %d/%d cycles"
            i
            nb_cycles)
        (fun notify ->
          Lwt_list.iter_s
            (fun {filename; _} ->
              let cycle = Naming.(src_cemented_dir // filename) in
              Lwt_utils_unix.copy_file
                ~src:cycle
                ~dst:Naming.(dst_cemented_dir // filename)
              >>= fun () -> notify ())
            files)
      >>= fun () -> return_unit)

let write_floating_block fd (block : Block_repr.t) =
  let bytes = Data_encoding.Binary.to_bytes_exn Block_repr.encoding block in
  let len = Bytes.length bytes in
  Lwt_utils_unix.write_bytes ~pos:0 ~len fd bytes

let export_floating_blocks ~floating_ro_fd ~floating_rw_fd ~export_block =
  let (limit_hash, limit_level) = Store.Block.descriptor export_block in
  let (stream, bpush) = Lwt_stream.create_bounded 1000 in
  (* Retrieve first floating block *)
  Block_repr.read_next_block_opt floating_ro_fd
  >>= (function
        | Some (block, _length) ->
            return block
        | None -> (
            Block_repr.read_next_block_opt floating_rw_fd
            >>= function
            | Some (block, _length) ->
                return block
            | None ->
                (* No block to read *)
                failwith
                  " export_floating_blocks: broken invariant, no blocks to \
                   read in floating stores" ))
  >>=? fun first_block ->
  if Compare.Int32.(limit_level < Block_repr.level first_block) then
    failwith
      " export_floating_blocks: broken invariant, the first floating block is \
       above the target block"
  else
    let exception Done in
    let f block =
      (* FIXME: we also write potential branches, it will eventually be GCed *)
      if Compare.Int32.(Block_repr.level block >= limit_level) then
        if Block_hash.equal limit_hash (Block_repr.hash block) then raise Done
        else return_unit
      else
        let block = (* Prune everything  *) {block with metadata = None} in
        bpush#push block >>= return
    in
    let reading_thread =
      Lwt.finalize
        (fun () ->
          Lwt.catch
            (fun () ->
              Lwt_unix.lseek floating_ro_fd 0 Unix.SEEK_SET
              >>= fun _ ->
              Floating_block_store.iter_raw_fd f floating_ro_fd
              >>=? fun () ->
              Lwt_unix.lseek floating_rw_fd 0 Unix.SEEK_SET
              >>= fun _ ->
              Floating_block_store.iter_raw_fd f floating_rw_fd
              >>=? fun () ->
              failwith "floating_export: could not retrieve the target block")
            (function
              | Done ->
                  return_unit
              | exn ->
                  failwith
                    "floating_export: error while reading the floating stores \
                     floating: %s"
                    (Printexc.to_string exn)))
        (fun () -> bpush#close ; Lwt.return_unit)
    in
    return (reading_thread, stream)

(* Export the protocol table (info regarding the protocol transitions) as well
   as all the stored protocols *)

let export_protocols protocol_levels ~src_dir ~dst_dir =
  let protocol_tbl_filename = Naming.(dst_dir // Snapshot.protocols_table) in
  Lwt_unix.openfile
    protocol_tbl_filename
    Unix.[O_CREAT; O_TRUNC; O_WRONLY]
    0o444
  >>= fun fd ->
  (* Export protocols table *)
  let bytes =
    Data_encoding.Binary.to_bytes_exn Protocol_levels.encoding protocol_levels
  in
  Lwt_utils_unix.write_bytes ~pos:0 fd bytes
  >>= fun () ->
  Lwt_utils_unix.safe_close fd
  >>= fun () ->
  Lwt_unix.opendir src_dir
  >>= fun dir_handle ->
  (* Only export the protocols relative to the targeted network *)
  let proto_to_export =
    List.map
      (fun (_, {Protocol_levels.protocol; _}) -> protocol)
      (Protocol_levels.bindings protocol_levels)
  in
  let nb_proto_to_export = List.length proto_to_export in
  Lwt_utils_unix.display_progress
    ~pp_print_step:(fun fmt i ->
      Format.fprintf fmt "Copying protocols: %d/%d" i nb_proto_to_export)
    (fun notify ->
      let rec copy_protocols () =
        Lwt.catch
          (fun () ->
            Lwt_unix.readdir dir_handle
            >>= function
            | "." | ".." ->
                copy_protocols ()
            | filename -> (
              match Protocol_hash.of_b58check_opt filename with
              | None ->
                  return_unit
              | Some ph ->
                  ( if List.mem ph proto_to_export then
                    Lwt_utils_unix.copy_file
                      ~src:Naming.(src_dir // filename)
                      ~dst:Naming.(dst_dir // filename)
                    >>= fun () -> notify ()
                  else Lwt.return_unit )
                  >>= fun () -> copy_protocols () ))
          (function
            | End_of_file -> return_unit | exn -> Lwt.return (error_exn exn))
      in
      Lwt.finalize
        (fun () -> copy_protocols ())
        (fun () -> Lwt_unix.closedir dir_handle))

(* Creates the requested export folder and its hierarchy *)

let create_snapshot_dir ~snapshot_dir =
  Lwt_unix.mkdir snapshot_dir 0o755
  >>= fun () ->
  let dst_cemented_dir = Naming.(snapshot_dir // Snapshot.cemented_blocks) in
  Lwt_unix.mkdir dst_cemented_dir 0o755
  >>= fun () ->
  let dst_protocol_dir = Naming.(snapshot_dir // Snapshot.protocols) in
  Lwt_unix.mkdir dst_protocol_dir 0o755
  >>= fun () -> return (dst_cemented_dir, dst_protocol_dir)

(*
   How to choose default block:
   - Archive => checkpoint (if not in the future)
                last_allow_fork_level(head) otherwise
   - Full n | Rolling n when n > 0 => checkpoint (if not in the future)
   else
     savepoint + 1 + max_op_ttl blocks prunés dispo (en relation avec caboose)

 To be a valid export block, the block b and it's pred bp must:
    - not be genesis
    - be both known
    - not be pruned, having their context present
    - have at least max_op_ttl(b) blocks (pruned or not) available
*)

let check_export_block_validity chain_store block =
  let (block_hash, block_level) = Store.Block.descriptor block in
  Store.Block.is_known_valid chain_store block_hash
  >>= fun is_known ->
  fail_unless
    is_known
    (Invalid_export_block {block = Some block_hash; reason = `Unknown})
  >>=? fun () ->
  fail_when
    (Store.Block.is_genesis chain_store block_hash)
    (Invalid_export_block {block = Some block_hash; reason = `Genesis})
  >>=? fun () ->
  Store.Chain.savepoint chain_store
  >>= fun (_, savepoint_level) ->
  fail_when
    Compare.Int32.(savepoint_level > block_level)
    (Invalid_export_block {block = Some block_hash; reason = `Pruned})
  >>=? fun () ->
  Store.Block.read_block chain_store block_hash
  >>=? fun block ->
  Store.Block.read_predecessor_opt chain_store block
  >>= (function
        | None ->
            fail
              (Invalid_export_block
                 {block = Some block_hash; reason = `Not_enough_pred})
        | Some pred_block ->
            return pred_block)
  >>=? fun pred_block ->
  (* Make sure that the predecessor's context is known *)
  Store.Block.context_exists chain_store pred_block
  >>= fun pred_context_exists ->
  (* We also need the predecessor not to be pruned *)
  fail_when
    Compare.Int32.(
      savepoint_level > Int32.pred block_level && not pred_context_exists)
    (Invalid_export_block {block = Some block_hash; reason = `Pruned_pred})
  >>=? fun () ->
  Store.Block.get_block_metadata_opt chain_store block
  >>= (function
        | None ->
            fail
              (Invalid_export_block {block = Some block_hash; reason = `Pruned})
        | Some block_metadata ->
            return block_metadata)
  >>=? fun block_metadata ->
  Store.Chain.caboose chain_store
  >>= fun (_, caboose_level) ->
  (* We will need the following blocks
     [ (target_block - max_op_ttl(target_block)) ; ... ; target_block ] *)
  let block_max_op_ttl = Store.Block.max_operations_ttl block_metadata in
  Store.Chain.genesis_block chain_store
  >>= fun genesis_block ->
  let genesis_level = Store.Block.level genesis_block in
  let minimum_level_needed =
    Compare.Int32.(
      max genesis_level Int32.(sub block_level (of_int block_max_op_ttl)))
  in
  fail_when
    Compare.Int32.(minimum_level_needed < caboose_level)
    (Invalid_export_block {block = Some block_hash; reason = `Not_enough_pred})
  >>=? fun () -> return (pred_block, minimum_level_needed)

let retrieve_export_block chain_store block =
  ( match block with
  | `Hash (h, distance) -> (
      Store.Block.read_block_opt chain_store ~distance h
      >>= function
      | None ->
          fail (Invalid_export_block {block = Some h; reason = `Unknown})
      | Some block ->
          return_some block )
  | `Head distance ->
      Store.Chain.current_head chain_store
      >>= fun current_head ->
      Store.Block.read_block_opt
        chain_store
        ~distance
        (Store.Block.hash current_head)
      >>= return
  | `Level i ->
      Store.Block.read_block_by_level_opt chain_store i >>= return
  | `Genesis ->
      fail
        (Invalid_export_block
           {
             block = Some (Store.Chain.genesis chain_store).Genesis.block;
             reason = `Genesis;
           })
  | `Alias (`Caboose, _) ->
      Store.Chain.caboose chain_store
      >>= fun (caboose_hash, _) ->
      fail
        (Invalid_export_block {block = Some caboose_hash; reason = `Caboose})
  | `Alias (`Checkpoint, distance) -> (
      Store.Chain.checkpoint chain_store
      >>= fun (checkpoint_hash, _) ->
      Store.Block.read_block_opt chain_store checkpoint_hash
      >>= function
      | Some checkpoint_block ->
          (* The checkpoint is known: we should have its context and caboose
             should be low enough to retrieve enough blocks. *)
          if distance = 0 then return_some checkpoint_block
          else
            Store.Block.read_block_opt
              chain_store
              (Store.Block.hash checkpoint_block)
              ~distance
            >>= return
      | None when distance = 0 ->
          Store.Chain.current_head chain_store
          >>= fun current_head ->
          (* Checkpoint might be in the future: take the head's last allowed
             fork level which we should know. *)
          Store.Block.get_block_metadata chain_store current_head
          >>=? fun head_metadata ->
          let head_lafl = Store.Block.last_allowed_fork_level head_metadata in
          Store.Block.read_block_by_level chain_store head_lafl
          >>=? return_some
      | None ->
          return_none )
  | `Alias (`Savepoint, distance) ->
      Store.Chain.savepoint chain_store
      >>= fun (_, savepoint_level) ->
      Store.Block.read_block_by_level_opt
        chain_store
        Int32.(sub savepoint_level (of_int distance))
      >>= return )
  >>=? function
  | None ->
      fail (Invalid_export_block {block = None; reason = `Unknown})
  | Some export_block ->
      check_export_block_validity chain_store export_block
      >>=? fun (pred_block, minimum_level_needed) ->
      return (export_block, pred_block, minimum_level_needed)

let compute_cemented_table_and_extra_cycle chain_store ~src_cemented_dir
    ~export_block =
  Cemented_block_store.load_table ~cemented_blocks_dir:src_cemented_dir
  >>=? fun table_arr ->
  let table_len = Array.length table_arr in
  let table = Array.to_list table_arr in
  (* Check whether the export_block is in the cemented blocks *)
  let export_block_level = Store.Block.level export_block in
  let is_cemented =
    table_len > 0
    && Compare.Int32.(
         export_block_level
         <= table_arr.(table_len - 1).Cemented_block_store.end_level)
  in
  if not is_cemented then
    (* Return either an empty list or the list of all cemented files *)
    return (table, None)
  else
    let is_last_cemented_block =
      Compare.Int32.(
        export_block_level
        = table_arr.(table_len - 1).Cemented_block_store.end_level)
    in
    if is_last_cemented_block then return (table, Some [])
    else
      (* If the export block is cemented, cut the cycle containing the
         export block accordingly and retrieve the extra blocks *)
      let (filtered_table, extra_cycles) =
        List.partition
          (fun {Cemented_block_store.end_level; _} ->
            let b = Compare.Int32.(export_block_level > end_level) in
            b)
          table
      in
      assert (extra_cycles <> []) ;
      let extra_cycle = List.hd extra_cycles in
      (* If the export block is the last block in cycle, append the cycle *)
      if Compare.Int32.(export_block_level = extra_cycle.end_level) then
        return (filtered_table @ [extra_cycle], Some [])
      else
        Store.Block.read_block_by_level chain_store extra_cycle.start_level
        >>=? (fun first_block_in_cycle ->
               if
                 Compare.Int32.(
                   Store.Block.level first_block_in_cycle > export_block_level)
               then
                 (* When the cycles are short, we may keep more blocks in the
                    floating store than in cemented *)
                 Store.Chain.caboose chain_store
                 >>= fun (_, caboose_level) ->
                 Store.Block.read_block_by_level chain_store caboose_level
               else return first_block_in_cycle)
        >>=? fun first_block ->
        Store.Chain_traversal.path chain_store first_block export_block
        >>= function
        | None ->
            failwith
              "compute_cemented_table_and_extra_cycle: cannot retrieve \
               the                 beggining of cycle."
        | Some floating_blocks ->
            (* Don't forget to add the first block as [Chain_traversal.path] does
               not include the lower-bound block *)
            let floating_blocks = first_block :: floating_blocks in
            return (filtered_table, Some floating_blocks)

let dump_context context_index ~snapshot_dir ~pred_block ~export_block =
  let block_data =
    {
      Context.Block_data.block_header = Store.Block.header export_block;
      operations = Store.Block.operations export_block;
      predecessor_header = Store.Block.header pred_block;
    }
  in
  Context.dump_context
    context_index
    block_data
    ~context_file_path:Naming.(snapshot_dir // Snapshot.context)

let check_history_mode chain_store ~rolling =
  let open History_mode in
  match Store.Chain.history_mode chain_store with
  | Archive | Full _ ->
      return_unit
  | Rolling _ when rolling ->
      return_unit
  | Rolling _ as stored ->
      fail (Incompatible_history_mode {stored; requested = Full {offset = 0}})

let export_floating_block_stream ~snapshot_dir floating_block_stream =
  Lwt_utils_unix.display_progress
    ~every:10
    ~pp_print_step:(fun fmt i ->
      Format.fprintf fmt "Copying floating blocks: %d blocks copied" i)
    (fun notify ->
      (* The target block is in the middle of a cemented cycle, the
         cycle prefix becomes the floating store. *)
      let floating_file = Naming.(snapshot_dir // Snapshot.floating_blocks) in
      Lwt_unix.openfile floating_file Unix.[O_CREAT; O_TRUNC; O_WRONLY] 0o444
      >>= fun fd ->
      Lwt_stream.iter_s
        (fun b -> write_floating_block fd b >>= fun () -> notify ())
        floating_block_stream
      >>= fun () -> Lwt_utils_unix.safe_close fd >>= fun () -> return_unit)

let export_rolling ~store_dir ~context_dir ~snapshot_dir ~block ~rolling
    genesis =
  let export_rolling_f (chain_store, context_index) =
    check_history_mode chain_store ~rolling
    >>=? fun () ->
    retrieve_export_block chain_store block
    >>=? fun (export_block, pred_block, lowest_block_level_needed) ->
    let export_mode = History_mode.Rolling {offset = 0} in
    lwt_emit (Export_info (export_mode, Store.Block.descriptor export_block))
    >>= fun () ->
    (* Read the store to gather only the necessary blocks *)
    Store.Block.read_block_by_level chain_store lowest_block_level_needed
    >>=? fun minimum_block ->
    Store.Chain_traversal.path chain_store minimum_block pred_block
    >>= (function
          | None ->
              failwith
                "export_rolling: unable to retrieve the necessary blocks to \
                 create the snapshot"
          | Some blocks ->
              (* Don't forget to add the first block as
                 [Chain_traversal.path] does not include the lower-bound
                 block *)
              return (minimum_block :: blocks))
    >>=? fun floating_blocks ->
    (* Prune all blocks except for the export_block's predecessor *)
    let floating_block_stream =
      Lwt_stream.of_list
        (List.filter_map
           (fun b ->
             Some {(Store.Unsafe.repr_of_block b) with metadata = None})
           floating_blocks)
    in
    (* We need to dump the context while locking the store, the
       contexts might get pruned *)
    dump_context context_index ~snapshot_dir ~pred_block ~export_block
    >>=? fun written_context_elements ->
    (* TODO: Export all the protocols: maybe only export the needed one
       s.t. forall proto_level. proto_level >= caboose.proto_level ? *)
    Store.Chain.all_protocol_levels chain_store
    >>= fun protocol_levels ->
    return
      ( export_mode,
        export_block,
        protocol_levels,
        written_context_elements,
        (return_unit, floating_block_stream) )
  in
  Store.Unsafe.open_for_snapshot_export
    ~store_dir
    ~context_dir
    genesis
    ~locked_f:export_rolling_f

(* Copy the cemented_store indexes while the lock is taken. *)
let copy_cemented_indexes chain_store ~dst_cemented_dir =
  let open Cemented_block_store in
  let block_store = Store.Unsafe.get_block_store chain_store in
  let cemented_store = Block_store.cemented_block_store block_store in
  match Cemented_block_store.get_highest_cemented_level cemented_store with
  | None ->
      (* No cemented files: no index to export *)
      return_unit
  | Some highest_level ->
      protect (fun () ->
          Lwt_utils_unix.display_progress
            ~every:1
            ~pp_print_step:(fun fmt _i ->
              Format.fprintf
                fmt
                "Copying %ld entries in cemented indexes"
                highest_level)
            (fun notify ->
              let dst_level_dir =
                Naming.(
                  dst_cemented_dir // cemented_block_level_index_directory)
              in
              let fresh_level_index =
                Cemented_block_level_index.v
                  ~fresh:true
                  ~readonly:false
                  ~log_size:100_000
                  dst_level_dir
              in
              let dst_hash_dir =
                Naming.(
                  dst_cemented_dir // cemented_block_hash_index_directory)
              in
              let fresh_hash_index =
                Cemented_block_hash_index.v
                  ~fresh:true
                  ~readonly:false
                  ~log_size:100_000
                  dst_hash_dir
              in
              let level_index =
                Cemented_block_store.cemented_block_level_index cemented_store
              in
              Cemented_block_level_index.iter
                (fun hash level ->
                  Cemented_block_level_index.replace
                    fresh_level_index
                    hash
                    level ;
                  Cemented_block_hash_index.replace fresh_hash_index level hash)
                level_index ;
              Cemented_block_level_index.close fresh_level_index ;
              Cemented_block_hash_index.close fresh_hash_index ;
              notify () >>= return))

let filter_indexes ~dst_cemented_dir limit =
  let open Cemented_block_store in
  let fresh_level_index =
    Cemented_block_level_index.v
      ~fresh:false
      ~readonly:false
      ~log_size:100_000
      Naming.(dst_cemented_dir // cemented_block_level_index_directory)
  in
  let fresh_hash_index =
    Cemented_block_hash_index.v
      ~fresh:false
      ~readonly:false
      ~log_size:100_00
      Naming.(dst_cemented_dir // cemented_block_hash_index_directory)
  in
  Cemented_block_level_index.filter fresh_level_index (fun (_, level) ->
      level <= limit) ;
  Cemented_block_hash_index.filter fresh_hash_index (fun (level, _) ->
      level <= limit) ;
  Cemented_block_level_index.close fresh_level_index ;
  Cemented_block_hash_index.close fresh_hash_index

let export_full ~store_dir ~context_dir ~snapshot_dir ~dst_cemented_dir ~block
    ~rolling genesis =
  let export_full_f (chain_store, context_index) =
    check_history_mode chain_store ~rolling
    >>=? fun () ->
    retrieve_export_block chain_store block
    >>=? fun (export_block, pred_block, _lowest_block_level_needed) ->
    let export_mode = History_mode.Full {offset = 0} in
    lwt_emit (Export_info (export_mode, Store.Block.descriptor export_block))
    >>= fun () ->
    let chain_id = Store.Chain.chain_id chain_store in
    let chain_dir = Naming.(store_dir // chain_store chain_id) in
    (* Open the floating FDs (in readonly) while the lock is present *)
    let ro_filename = Naming.(chain_dir // floating_blocks RO) in
    let rw_filename = Naming.(chain_dir // floating_blocks RW) in
    Lwt_unix.openfile ro_filename [Unix.O_RDONLY] 0o444
    >>= fun ro_fd ->
    Lwt_unix.openfile rw_filename [Unix.O_RDONLY] 0o644
    >>= fun rw_fd ->
    Lwt.catch
      (fun () ->
        let src_cemented_dir =
          Naming.(chain_dir // cemented_blocks_directory)
        in
        (* Compute the necessary cemented table *)
        compute_cemented_table_and_extra_cycle
          chain_store
          ~src_cemented_dir
          ~export_block
        >>=? fun (cemented_table, extra_floating_blocks) ->
        (* Copy the indexes *)
        copy_cemented_indexes chain_store ~dst_cemented_dir
        >>=? fun () ->
        Store.Chain.all_protocol_levels chain_store
        >>= fun protocol_levels ->
        (* Dump the context while the store is locked to avoid reading on
           pruning *)
        dump_context context_index ~snapshot_dir ~pred_block ~export_block
        >>=? fun written_context_elements ->
        let block_store = Store.Unsafe.get_block_store chain_store in
        let cemented_store = Block_store.cemented_block_store block_store in
        let should_filter_indexes =
          match
            Cemented_block_store.get_highest_cemented_level cemented_store
          with
          | None ->
              false
          | Some max_cemented_level ->
              Compare.Int32.(
                max_cemented_level > Store.Block.level export_block)
        in
        return
          ( export_mode,
            export_block,
            protocol_levels,
            written_context_elements,
            (src_cemented_dir, cemented_table),
            (ro_fd, rw_fd),
            extra_floating_blocks,
            should_filter_indexes ))
      (fun exn ->
        Lwt_utils_unix.safe_close ro_fd
        >>= fun () ->
        Lwt_utils_unix.safe_close rw_fd >>= fun () -> Lwt.fail exn)
  in
  Store.Unsafe.open_for_snapshot_export
    ~store_dir
    ~context_dir
    genesis
    ~locked_f:export_full_f
  >>=? fun ( export_mode,
             export_block,
             protocol_levels,
             written_context_elements,
             (src_cemented_dir, cemented_table),
             (floating_ro_fd, floating_rw_fd),
             extra_floating_blocks,
             should_filter_indexes ) ->
  let finalizer () =
    Lwt_utils_unix.safe_close floating_ro_fd
    >>= fun () -> Lwt_utils_unix.safe_close floating_rw_fd
  in
  copy_cemented_blocks ~src_cemented_dir ~dst_cemented_dir cemented_table
  >>=? fun () ->
  if should_filter_indexes then
    filter_indexes ~dst_cemented_dir (List.last_exn cemented_table).end_level ;
  ( match extra_floating_blocks with
  | Some floating_blocks ->
      finalizer ()
      >>= fun () ->
      return
        ( return_unit,
          Lwt_stream.of_list
            (List.map Store.Unsafe.repr_of_block floating_blocks) )
  | None ->
      (* The export block is in the floating stores, copy all the
         floating stores until the block is reached *)
      export_floating_blocks ~floating_ro_fd ~floating_rw_fd ~export_block
      >>=? fun (reading_thread, floating_block_stream) ->
      let reading_thread = Lwt.finalize (fun () -> reading_thread) finalizer in
      return (reading_thread, floating_block_stream) )
  >>=? fun (reading_thread, floating_block_stream) ->
  return
    ( export_mode,
      export_block,
      protocol_levels,
      written_context_elements,
      (reading_thread, floating_block_stream) )

let export ?(rolling = false) ~block ~store_dir ~context_dir ~chain_name
    ~snapshot_dir genesis =
  create_snapshot_dir ~snapshot_dir
  >>=? fun (dst_cemented_dir, dst_protocol_dir) ->
  ( if rolling then
    export_rolling
      ~store_dir
      ~context_dir
      ~snapshot_dir
      ~block
      ~rolling
      genesis
  else
    export_full
      ~store_dir
      ~context_dir
      ~snapshot_dir
      ~dst_cemented_dir
      ~block
      ~rolling
      genesis )
  >>=? fun ( export_mode,
             export_block,
             protocol_levels,
             written_context_elements,
             (reading_thread, floating_block_stream) ) ->
  export_floating_block_stream ~snapshot_dir floating_block_stream
  >>=? fun () ->
  reading_thread
  >>=? fun () ->
  export_protocols
    protocol_levels
    ~src_dir:Naming.(store_dir // protocol_store_directory)
    ~dst_dir:dst_protocol_dir
  >>=? fun () ->
  let metadata =
    ( {
        snapshot_version = Snapshot_version.current_version;
        chain_name;
        history_mode = export_mode;
        block_hash = Store.Block.hash export_block;
        level = Store.Block.level export_block;
        timestamp = Store.Block.timestamp export_block;
        context_elements = written_context_elements;
      }
      : Snapshot_version.metadata )
  in
  write_snapshot_metadata metadata Naming.(snapshot_dir // Snapshot.metadata)
  >>= fun () ->
  lwt_emit (Export_success snapshot_dir) >>= fun () -> return_unit

let copy_and_restore_cemented_blocks ?(check_consistency = true)
    ~snapshot_cemented_dir ~dst_cemented_dir ~genesis_hash =
  (* Copy the cemented files *)
  let stream = Lwt_unix.files_of_directory snapshot_cemented_dir in
  Lwt_stream.to_list stream
  >>= fun files ->
  let dst_level_dir =
    Naming.(
      snapshot_cemented_dir // Naming.cemented_block_level_index_directory)
  in
  let dst_hash_dir =
    Naming.(
      snapshot_cemented_dir // Naming.cemented_block_hash_index_directory)
  in
  ( if Sys.file_exists dst_level_dir && Sys.file_exists dst_hash_dir then
    Lwt_utils_unix.copy_dir
      dst_level_dir
      Naming.(dst_cemented_dir // cemented_block_level_index_directory)
    >>= fun () ->
    Lwt_utils_unix.copy_dir
      dst_hash_dir
      Naming.(dst_cemented_dir // cemented_block_hash_index_directory)
  else Lwt.return_unit )
  >>= fun () ->
  filter_s
    (function
      | "." | ".." ->
          return_false
      | file
        when file = Naming.cemented_block_hash_index_directory
             || file = Naming.cemented_block_level_index_directory ->
          return_false
      | file ->
          let is_valid =
            match String.split_on_char '_' file with
            | [s; e] ->
                Int32.of_string_opt s <> None || Int32.of_string_opt e <> None
            | _ ->
                false
          in
          if not is_valid then
            failwith "Found a invalid cemented block file: %s" file
          else return_true)
    files
  >>=? fun cemented_files ->
  let nb_cemented_files = List.length cemented_files in
  ( if nb_cemented_files > 0 then
    Lwt_utils_unix.display_progress
      ~pp_print_step:(fun fmt i ->
        Format.fprintf
          fmt
          "Copying cycles: %d/%d (%d)"
          i
          nb_cemented_files
          (100 * i / nb_cemented_files))
      (fun notify ->
        Lwt_list.iter_s
          (fun file ->
            let src = Naming.(snapshot_cemented_dir // file) in
            let dst = Naming.(dst_cemented_dir // file) in
            Lwt_utils_unix.copy_file ~src ~dst >>= fun () -> notify ())
          cemented_files)
  else Lwt.return_unit )
  >>= fun () ->
  Cemented_block_store.init
    ~cemented_blocks_dir:dst_cemented_dir
    ~readonly:false
  >>=? fun cemented_store ->
  ( if check_consistency && nb_cemented_files > 0 then
    iter_s
      (fun cemented_file ->
        if
          not
            (Array.exists
               (fun {Cemented_block_store.filename; _} ->
                 Compare.String.equal filename cemented_file)
               (Cemented_block_store.cemented_blocks_files cemented_store))
        then failwith "Cemented copy error: cannot find file %s" cemented_file
        else return_unit)
      (List.sort compare cemented_files)
    >>=? fun () ->
    Lwt_utils_unix.display_progress
      ~pp_print_step:(fun fmt i ->
        Format.fprintf
          fmt
          "Restoring cycles consistency: %d/%d (%d)"
          i
          nb_cemented_files
          (100 * i / nb_cemented_files))
      (fun notify ->
        Cemented_block_store.check_indexes_consistency
          ~post_step:notify
          ~genesis_hash
          cemented_store)
  else return_unit )
  >>=? fun () ->
  Cemented_block_store.close cemented_store ;
  return_unit

let read_floating_blocks ~genesis_hash ~floating_blocks_file =
  Lwt_unix.openfile floating_blocks_file Unix.[O_RDONLY] 0o444
  >>= fun fd ->
  let (stream, bounded_push) = Lwt_stream.create_bounded 1000 in
  Lwt_unix.lseek fd 0 Unix.SEEK_END
  >>= fun eof_offset ->
  Lwt_unix.lseek fd 0 Unix.SEEK_SET
  >>= fun _ ->
  let rec loop ?pred_block nb_bytes_left =
    if nb_bytes_left < 0 then failwith "read_floating_blocks: corrupted blocks"
    else if nb_bytes_left = 0 then return_unit
    else
      Block_repr.read_next_block fd
      >>= fun (block, len_read) ->
      Block_repr.check_block_consistency ~genesis_hash ?pred_block block
      >>=? fun () ->
      bounded_push#push block >>= fun () -> loop (nb_bytes_left - len_read)
  in
  let reading_thread =
    Lwt.finalize
      (fun () -> loop eof_offset)
      (fun () ->
        bounded_push#close ;
        Lwt_utils_unix.safe_close fd)
  in
  return (reading_thread, stream)

let copy_protocols ~snapshot_protocol_dir ~dst_protocol_dir =
  (* Import protocol table *)
  let protocol_tbl_filename =
    Naming.(snapshot_protocol_dir // Snapshot.protocols_table)
  in
  Lwt_utils_unix.read_file protocol_tbl_filename
  >>= fun table_bytes ->
  let protocol_levels =
    Data_encoding.Binary.of_bytes_exn
      Protocol_levels.encoding
      (Bytes.unsafe_of_string table_bytes)
  in
  (* Retrieve protocol files *)
  let stream = Lwt_unix.files_of_directory snapshot_protocol_dir in
  Lwt_stream.to_list stream
  >>= fun files ->
  let protocol_files =
    List.filter_map
      (function
        | "." | ".." ->
            None
        | file when file = Filename.basename protocol_tbl_filename ->
            None
        | file ->
            Some file)
      files
  in
  map_s
    (fun file ->
      match Protocol_hash.of_b58check_opt file with
      | Some ph ->
          return (ph, file)
      | None ->
          failwith "Invalid protocol filename %s" file)
    protocol_files
  >>=? fun protocols ->
  Lwt_utils_unix.display_progress
    ~pp_print_step:(fun fmt i ->
      Format.fprintf fmt "Copying protocols: %d/%d" i (List.length protocols))
    (fun notify ->
      let validate_and_copy (expected_hash, protocol_filename) =
        lwt_emit (Validate_protocol_sources expected_hash)
        >>= fun () ->
        let src = Naming.(snapshot_protocol_dir // protocol_filename) in
        let dst = Naming.(dst_protocol_dir // protocol_filename) in
        Lwt_utils_unix.copy_file ~src ~dst
        >>= fun () ->
        Lwt_utils_unix.read_file dst
        >>= fun protocol_sources ->
        match Protocol.of_bytes (Bytes.unsafe_of_string protocol_sources) with
        | None ->
            failwith
              "import_protocol: cannot decode protocol %s"
              protocol_filename
        | Some p ->
            let hash = Protocol.hash p in
            notify ()
            >>= fun () ->
            fail_unless
              (Protocol_hash.equal expected_hash hash)
              (Exn
                 (Failure
                    (Format.asprintf
                       "Inconsistent protocol hash: got %a expected %a"
                       Protocol_hash.pp
                       hash
                       Protocol_hash.pp
                       expected_hash)))
      in
      iter_s validate_and_copy protocols)
  >>=? fun () -> return protocol_levels

let import_log_notice ?snapshot_metadata filename block =
  lwt_emit (Import_info (filename, snapshot_metadata))
  >>= fun () ->
  ( match block with
  | None ->
      lwt_emit Import_unspecified_hash
  | Some _ ->
      Lwt.return_unit )
  >>= fun () -> lwt_emit Import_loading

let check_context_hash_consistency validation_store block_header =
  fail_unless
    (Context_hash.equal
       validation_store.Tezos_validation.Block_validation.context_hash
       block_header.Block_header.shell.context)
    (Snapshot_import_failure "resulting context hash does not match")

let restore_and_apply_context ?expected_block ~context_index ~snapshot_dir
    ~user_activated_upgrades ~user_activated_protocol_overrides
    snapshot_metadata genesis chain_id =
  (* Restore context *)
  (* Start by commiting genesis *)
  Context.commit_genesis
    context_index
    ~chain_id
    ~time:genesis.Genesis.time
    ~protocol:genesis.protocol
  >>=? fun genesis_ctxt_hash ->
  Context.restore_context
    context_index
    ?expected_block
    ~context_file_path:Naming.(snapshot_dir // Snapshot.context)
    ~metadata:snapshot_metadata
  >>=? fun ({block_header; operations; predecessor_header} as block_data) ->
  let pred_context_hash = predecessor_header.shell.context in
  Context.checkout context_index pred_context_hash
  >>= (function
        | Some ch ->
            return ch
        | None ->
            failwith
              "Failed to checkout context with hash %a. Something is wrong \
               with your storage."
              Context_hash.pp
              pred_context_hash)
  >>=? fun predecessor_context ->
  Tezos_validation.Block_validation.apply
    chain_id
    ~user_activated_upgrades
    ~user_activated_protocol_overrides
    ~max_operations_ttl:(Int32.to_int predecessor_header.shell.level)
    ~predecessor_block_header:predecessor_header
    ~predecessor_context
    ~block_header
    operations
  >>=? fun block_validation_result ->
  check_context_hash_consistency
    block_validation_result.validation_store
    block_header
  >>=? fun () -> return (block_data, genesis_ctxt_hash, block_validation_result)

(* TODO parallelise in another process *)
(* TODO? remove patch context *)
let import ?patch_context ?block:expected_block ?(check_consistency = true)
    ~snapshot_dir ~dst_store_dir ~dst_context_dir ~user_activated_upgrades
    ~user_activated_protocol_overrides (genesis : Genesis.t) =
  let chain_id = Chain_id.of_block_hash genesis.block in
  read_snapshot_metadata Naming.(snapshot_dir // Snapshot.metadata)
  >>= fun snapshot_metadata ->
  import_log_notice ~snapshot_metadata snapshot_dir expected_block
  >>= fun () ->
  Context.init ~readonly:false ?patch_context dst_context_dir
  >>= fun context_index ->
  (* Restore context *)
  restore_and_apply_context
    ?expected_block
    ~context_index
    ~snapshot_dir
    ~user_activated_upgrades
    ~user_activated_protocol_overrides
    snapshot_metadata
    genesis
    chain_id
  >>=? fun (block_data, genesis_context_hash, block_validation_result) ->
  let dst_protocol_dir = Naming.(dst_store_dir // protocol_store_directory) in
  let dst_chain_store_dir = Naming.(dst_store_dir // chain_store chain_id) in
  let dst_cemented_dir =
    Naming.(dst_chain_store_dir // cemented_blocks_directory)
  in
  Lwt_list.iter_s
    (Lwt_utils_unix.create_dir ~perm:0o755)
    [dst_store_dir; dst_protocol_dir; dst_chain_store_dir; dst_cemented_dir]
  >>= fun () ->
  (* Restore store *)
  (* Restore protocols *)
  copy_protocols
    ~snapshot_protocol_dir:Naming.(snapshot_dir // Snapshot.(protocols))
    ~dst_protocol_dir
  >>=? fun protocol_levels ->
  (* Restore cemented dir *)
  copy_and_restore_cemented_blocks
    ~check_consistency
    ~snapshot_cemented_dir:Naming.(snapshot_dir // Snapshot.cemented_blocks)
    ~dst_cemented_dir
    ~genesis_hash:genesis.block
  >>=? fun () ->
  let floating_blocks_file =
    Naming.(snapshot_dir // Snapshot.floating_blocks)
  in
  read_floating_blocks ~genesis_hash:genesis.block ~floating_blocks_file
  >>=? fun (reading_thread, floating_blocks_stream) ->
  let {Block_validation.validation_store; block_metadata; ops_metadata; _} =
    block_validation_result
  in
  let contents =
    {
      Block_repr.header = block_data.block_header;
      operations = block_data.operations;
    }
  in
  let metadata =
    Some
      ( {
          message = validation_store.message;
          max_operations_ttl = validation_store.max_operations_ttl;
          last_allowed_fork_level = validation_store.last_allowed_fork_level;
          block_metadata;
          operations_metadata = ops_metadata;
        }
        : Block_repr.metadata )
  in
  let new_head_with_metadata =
    ( {hash = Block_header.hash block_data.block_header; contents; metadata}
      : Block_repr.block )
  in
  (* TODO: parametrize this *)
  (* Set the history mode with the default offset*)
  (let open History_mode in
  match snapshot_metadata.history_mode with
  | Archive ->
      failwith "unexpected history mode found in snapshot"
  | Rolling _ ->
      return (Rolling {offset = default_offset})
  | Full _ ->
      return (Full {offset = default_offset}))
  >>=? fun history_mode ->
  Lwt_utils_unix.display_progress
    ~every:100
    ~pp_print_step:(fun fmt i ->
      Format.fprintf fmt "Storing floating blocks: %d blocks wrote" i)
    (fun notify ->
      Store.Unsafe.restore_from_snapshot
        ~notify
        ~store_dir:dst_store_dir
        ~context_index
        ~genesis
        ~genesis_context_hash
        ~floating_blocks_stream
        ~new_head_with_metadata
        ~protocol_levels
        ~history_mode)
  >>=? fun () ->
  reading_thread
  >>=? fun () ->
  Context.close context_index
  >>= fun () ->
  lwt_emit (Import_success snapshot_dir) >>= fun () -> return_unit

let snapshot_info ~snapshot_dir =
  read_snapshot_metadata Naming.(snapshot_dir // Snapshot.metadata)
  >>= fun metadata ->
  Format.printf "@[%a@]@." Snapshot_version.metadata_pp metadata ;
  Lwt.return_unit

(* Legacy import *)

let legacy_verify_predecessors header_opt pred_hash =
  match header_opt with
  | None ->
      return_unit
  | Some header ->
      fail_unless
        ( header.Block_header.shell.level >= 2l
        && Block_hash.equal header.shell.predecessor pred_hash )
        (Snapshot_import_failure "predecessors inconsistency")

let legacy_check_operations_consistency block_header operations
    operation_hashes =
  (* Compute operations hashes and compare *)
  List.iter2
    (fun (_, op) (_, oph) ->
      let expected_op_hash = List.map Operation.hash op in
      List.iter2
        (fun expected found -> assert (Operation_hash.equal expected found))
        expected_op_hash
        oph)
    operations
    operation_hashes ;
  (* Check header hashes based on merkle tree *)
  let hashes =
    List.map
      (fun (_, opl) -> List.map Operation.hash opl)
      (List.rev operations)
  in
  let computed_hash =
    Operation_list_list_hash.compute
      (List.map Operation_list_hash.compute hashes)
  in
  let are_oph_equal =
    Operation_list_list_hash.equal
      computed_hash
      block_header.Block_header.shell.operations_hash
  in
  fail_unless
    are_oph_equal
    (Inconsistent_operation_hashes
       (computed_hash, block_header.Block_header.shell.operations_hash))

let legacy_block_validation succ_header_opt header_hash
    {Context.Pruned_block.block_header; operations; operation_hashes} =
  legacy_verify_predecessors succ_header_opt header_hash
  >>=? fun () ->
  legacy_check_operations_consistency block_header operations operation_hashes
  >>=? fun () -> return_unit

let import_legacy ?patch_context ?block:expected_block ~dst_store_dir
    ~dst_context_dir ~chain_name ~user_activated_upgrades
    ~user_activated_protocol_overrides ~snapshot_file genesis =
  (* First: check that the imported snapshot is compatible with the
     hardcoded networks *)
  Legacy.Hardcoded.check_network ~chain_name
  >>=? fun () ->
  import_log_notice snapshot_file expected_block
  >>= fun () ->
  let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
  let dst_protocol_dir = Naming.(dst_store_dir // protocol_store_directory) in
  let dst_chain_store_dir = Naming.(dst_store_dir // chain_store chain_id) in
  let dst_cemented_dir =
    Naming.(dst_chain_store_dir // cemented_blocks_directory)
  in
  Lwt_list.iter_s
    (Lwt_utils_unix.create_dir ~perm:0o755)
    [dst_store_dir; dst_protocol_dir; dst_chain_store_dir; dst_cemented_dir]
  >>= fun () ->
  Context.init ~readonly:false ?patch_context dst_context_dir
  >>= fun context_index ->
  (* Start by commiting genesis in the context *)
  Context.commit_genesis
    context_index
    ~chain_id
    ~time:genesis.Genesis.time
    ~protocol:genesis.protocol
  >>=? fun genesis_context_hash ->
  let cycle_length = Legacy.Hardcoded.cycle_length ~chain_name in
  Cemented_block_store.init
    ~cemented_blocks_dir:dst_cemented_dir
    ~readonly:false
  >>=? fun cemented_store ->
  let floating_blocks = ref [] in
  let current_blocks = ref [] in
  let has_reached_cemented = ref false in
  let genesis_block =
    Store.Chain.create_genesis_block ~genesis genesis_context_hash
  in
  let handle_block ((hash : Block_hash.t), (block : Context.Pruned_block.t)) =
    (* We are given blocks in a reverse order ! *)
    (let proj (hash, (block : Context.Pruned_block.t)) =
       let contents =
         {
           Block_repr.header = block.block_header;
           operations = List.map (fun (_, l) -> l) block.operations;
         }
       in
       {Block_repr.hash; contents; metadata = None}
     in
     let block = proj (hash, block) in
     match Block_repr.level block with
     (* Hardcoded special treatement for first two blocks *)
     | 0l ->
         (* No genesis in previous format *) assert false
     | 1l ->
         (* Cement from genesis to this block *)
         if !current_blocks <> [] then (
           assert (!floating_blocks = []) ;
           current_blocks := !floating_blocks ) ;
         Cemented_block_store.cement_blocks
           ~check_consistency:false
           cemented_store
           ~write_metadata:false
           [Store.Unsafe.repr_of_block genesis_block; block]
     | level ->
         (* 4 cases :
             - in future floating blocks => after the cementing part
             - at the end of a cycle
             - in the midle of a cycle
             - at the dawn of a cycle
            *)
         let is_end_of_a_cycle =
           Compare.Int32.equal 1l Int32.(rem level (of_int cycle_length))
         in
         if is_end_of_a_cycle then (
           if not !has_reached_cemented then (
             has_reached_cemented := true ;
             (* All current blocks should be written in floating *)
             (* We will write them later on *)
             floating_blocks := !current_blocks ) ;
           (* Start building up the cycle to cement *)
           current_blocks := [block] ;
           return_unit )
         else
           let is_dawn_of_a_cycle =
             Compare.Int32.equal 2l Int32.(rem level (of_int cycle_length))
           in
           if is_dawn_of_a_cycle && !has_reached_cemented then (
             (* Cycle is complete, cement it *)
             Cemented_block_store.cement_blocks
               ~check_consistency:false
               cemented_store
               ~write_metadata:false
               (block :: !current_blocks)
             >>=? fun () ->
             current_blocks := [] ;
             return_unit )
           else (
             current_blocks := block :: !current_blocks ;
             return_unit ))
    >>=? fun () -> return_unit
  in
  let partial_protocol_levels :
      (int32 * Protocol_hash.t * Protocol_levels.commit_info option) list ref =
    ref []
  in
  let handle_protocol_data (transition_level, protocol) =
    let open Context.Protocol_data_legacy in
    let open Protocol_levels in
    let { info = {author; message; _};
          protocol_hash;
          test_chain_status;
          data_key;
          parents } =
      protocol
    in
    let commit_info =
      {
        author;
        message;
        test_chain_status;
        data_merkle_root = data_key;
        parents_contexts = parents;
      }
    in
    partial_protocol_levels :=
      (transition_level, protocol_hash, Some commit_info)
      :: !partial_protocol_levels ;
    return_unit
  in
  (* Restore context and fetch data *)
  Context.restore_context_legacy
    ?expected_block
    context_index
    ~snapshot_file
    ~handle_block
    ~handle_protocol_data
    ~block_validation:legacy_block_validation
  >>=? fun ( pred_block_header,
             block_data,
             _oldest_header_opt,
             legacy_history_mode ) ->
  let history_mode = History_mode.convert legacy_history_mode in
  (* Floating blocks should be initialized now *)
  let floating_blocks =
    if not !has_reached_cemented then !current_blocks else !floating_blocks
  in
  (* Apply pred block *)
  let pred_context_hash = pred_block_header.shell.context in
  Context.checkout_exn context_index pred_context_hash
  >>= fun predecessor_context ->
  let {Context.Block_data_legacy.block_header; operations} = block_data in
  Tezos_validation.Block_validation.apply
    chain_id
    ~user_activated_upgrades
    ~user_activated_protocol_overrides
    ~max_operations_ttl:
      (Int32.to_int pred_block_header.shell.level) (* Allows old operations *)
    ~predecessor_block_header:pred_block_header
    ~predecessor_context
    ~block_header
    operations
  >>=? fun block_validation_result ->
  fail_unless
    (Context_hash.equal
       block_validation_result.validation_store.Block_validation.context_hash
       block_header.Block_header.shell.context)
    (Snapshot_import_failure "Resulting context hash does not match")
  >>=? fun () ->
  let {Block_validation.validation_store; block_metadata; ops_metadata; _} =
    block_validation_result
  in
  let contents = {Block_repr.header = block_header; operations} in
  let {Block_validation.message; max_operations_ttl; last_allowed_fork_level; _}
      =
    validation_store
  in
  let metadata =
    Some
      {
        Block_repr.message;
        max_operations_ttl;
        last_allowed_fork_level;
        block_metadata;
        operations_metadata = ops_metadata;
      }
  in
  let new_head_with_metadata =
    ( {hash = Block_header.hash block_header; contents; metadata}
      : Block_repr.block )
  in
  (* Append the new head with the floating blocks *)
  Lwt_utils_unix.display_progress
    ~every:100
    ~pp_print_step:(fun fmt i ->
      Format.fprintf fmt "Storing floating blocks: %d blocks wrote" i)
    (fun notify ->
      Store.Unsafe.restore_from_legacy_snapshot
        ~notify
        ~store_dir:dst_store_dir
        ~context_index
        ~genesis
        ~genesis_context_hash
        ~floating_blocks_stream:(Lwt_stream.of_list floating_blocks)
        ~new_head_with_metadata
        ~partial_protocol_levels:!partial_protocol_levels
        ~history_mode)
  >>=? fun () ->
  (* Protocol will be stored next time the store is loaded *)
  lwt_emit (Import_success snapshot_file) >>= fun () -> return_unit
