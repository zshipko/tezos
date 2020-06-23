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

let current_version = 2

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
  | Invalid_export_path of string
  | Snapshot_file_not_found of string
  | Inconsistent_protocol_hash of {
      expected : Protocol_hash.t;
      got : Protocol_hash.t;
    }
  | Inconsistent_context_hash of {
      expected : Context_hash.t;
      got : Context_hash.t;
    }
  | Inconsistent_context of Context_hash.t
  | Cannot_decode_protocol of string
  | Cannot_write_metadata of string
  | Cannot_read_metadata of string
  | Inconsistent_floating_store of block_descriptor * block_descriptor
  | Missing_target_block of block_descriptor
  | Cannot_read_floating_store of string
  | Cannot_retrieve_block_interval
  | Invalid_cemented_file of string
  | Missing_cemented_file of string
  | Corrupted_floating_store
  | Invalid_protocol_file of string
  | Target_block_validation_failed of Block_hash.t * string
  | Directory_already_exists of string
  | Empty_floating_store
  | Inconsistent_predecessors
  | Snapshot_import_failure of string
  | Snapshot_export_failure of string
  | Inconsistent_chain_import of {
      expected : Distributed_db_version.Name.t;
      got : Distributed_db_version.Name.t;
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
         compatible with the given storage (running with history mode %a)."
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
    ~id:"snapshots.invalid_export_path"
    ~title:"Invalid export path"
    ~description:"Invalid path to export snapshot"
    ~pp:(fun ppf path ->
      Format.fprintf
        ppf
        "Failed to export snapshot: the file or directory %s already exists."
        path)
    (obj1 (req "path" string))
    (function Invalid_export_path path -> Some path | _ -> None)
    (fun path -> Invalid_export_path path) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.snapshot_file_not_found"
    ~title:"Snapshot file not found"
    ~description:"The snapshot file cannot be found."
    ~pp:(fun ppf given_file ->
      Format.fprintf ppf "The snapshot file %s does not exists." given_file)
    (obj1 (req "given_snapshot_file" string))
    (function Snapshot_file_not_found file -> Some file | _ -> None)
    (fun file -> Snapshot_file_not_found file) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.inconsistent_protocol_hash"
    ~title:"Inconsistent protocol hash"
    ~description:"The announced protocol hash doesn't match the computed hash."
    ~pp:(fun ppf (oph, oph') ->
      Format.fprintf
        ppf
        "Inconsistent protocol_hash. Expected: %a, got %a."
        Protocol_hash.pp
        oph
        Protocol_hash.pp
        oph')
    (obj2
       (req "expected" Protocol_hash.encoding)
       (req "got" Protocol_hash.encoding))
    (function
      | Inconsistent_protocol_hash {expected; got} ->
          Some (expected, got)
      | _ ->
          None)
    (fun (expected, got) -> Inconsistent_protocol_hash {expected; got}) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.inconsistent_context_hash"
    ~title:"Inconsistent context hash"
    ~description:"The announced context hash doesn't match the computed hash."
    ~pp:(fun ppf (oph, oph') ->
      Format.fprintf
        ppf
        "Inconsistent context_hash. Expected: %a, got %a."
        Context_hash.pp
        oph
        Context_hash.pp
        oph')
    (obj2
       (req "expected" Context_hash.encoding)
       (req "got" Context_hash.encoding))
    (function
      | Inconsistent_context_hash {expected; got} ->
          Some (expected, got)
      | _ ->
          None)
    (fun (expected, got) -> Inconsistent_context_hash {expected; got}) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.inconsistent_context"
    ~title:"Inconsistent context"
    ~description:"Inconsistent context after restore."
    ~pp:(fun ppf h ->
      Format.fprintf
        ppf
        "Failed to checkout context %a after restoring it."
        Context_hash.pp
        h)
    (obj1 (req "context_hash" Context_hash.encoding))
    (function Inconsistent_context h -> Some h | _ -> None)
    (fun h -> Inconsistent_context h) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.cannot_decode_protocol"
    ~title:"Protocol import cannot decode"
    ~description:"Failed to decode file when importing protocol"
    ~pp:(fun ppf filename ->
      Format.fprintf ppf "Cannot decode the protocol in file: %s" filename)
    (obj1 (req "filename" string))
    (function Cannot_decode_protocol filename -> Some filename | _ -> None)
    (fun filename -> Cannot_decode_protocol filename) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.cannot_write_metadata"
    ~title:"Cannot write metadata"
    ~description:"Cannot write metadata while exporting snapshot."
    ~pp:(fun ppf msg ->
      Format.fprintf
        ppf
        "Cannot write metadata while exporting snapshot: %s."
        msg)
    (obj1 (req "msg" string))
    (function Cannot_write_metadata msg -> Some msg | _ -> None)
    (fun msg -> Cannot_write_metadata msg) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.cannot_read_metadata"
    ~title:"Cannot read metadata"
    ~description:"Cannot read snapshot's metadata."
    ~pp:(fun ppf msg ->
      Format.fprintf ppf "Cannot read snapshot's metadata: %s." msg)
    (obj1 (req "msg" string))
    (function Cannot_read_metadata msg -> Some msg | _ -> None)
    (fun msg -> Cannot_read_metadata msg) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.inconsistent_floating_store"
    ~title:"Inconsistent floating store"
    ~description:"The floating block store is inconsistent."
    ~pp:(fun ppf (target_blk, first_blk) ->
      Format.fprintf
        ppf
        "Failed to export floating store, the first block %a is above the \
         target block %a (broken invariant)."
        pp_block_descriptor
        first_blk
        pp_block_descriptor
        target_blk)
    (obj2
       (req "target" block_descriptor_encoding)
       (req "first" block_descriptor_encoding))
    (function
      | Inconsistent_floating_store (target, first) ->
          Some (target, first)
      | _ ->
          None)
    (fun (target, first) -> Inconsistent_floating_store (target, first)) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.missing_target_block"
    ~title:"Missing target block in floating stores"
    ~description:"Floating stores does not contain the target block."
    ~pp:(fun ppf target_blk ->
      Format.fprintf
        ppf
        "Failed to export floating blocks as the target block %a cannot be \
         found."
        pp_block_descriptor
        target_blk)
    (obj1 (req "target" block_descriptor_encoding))
    (function Missing_target_block descr -> Some descr | _ -> None)
    (fun descr -> Missing_target_block descr) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.cannot_read_floating_stores"
    ~title:"Cannot read floating stores"
    ~description:"Unable to read floating stores."
    ~pp:(fun ppf msg ->
      Format.fprintf ppf "Cannot read the floating blocks stores: %s" msg)
    (obj1 (req "msg" string))
    (function Cannot_read_floating_store msg -> Some msg | _ -> None)
    (fun msg -> Cannot_read_floating_store msg) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.cannot_retrieve_block_interval"
    ~title:"Cannot retrieve block interval"
    ~description:"Cannot retrieve block interval from store"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Cannot retrieve block interval: failed to retrieve blocks.")
    unit
    (function Cannot_retrieve_block_interval -> Some () | _ -> None)
    (fun () -> Cannot_retrieve_block_interval) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.invalid_cemented_file"
    ~title:"Invalid cemented file"
    ~description:
      "Encountered an invalid cemented file while restoring the cemented store"
    ~pp:(fun ppf file ->
      Format.fprintf
        ppf
        "Failed to restore cemented blocks. Encountered an invalid file '%s'."
        file)
    (obj1 (req "file" string))
    (function Invalid_cemented_file s -> Some s | _ -> None)
    (fun s -> Invalid_cemented_file s) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.missing_cemented_file"
    ~title:"Missing cemented file"
    ~description:"Cannot find cemented file while restoring cemented store"
    ~pp:(fun ppf file ->
      Format.fprintf
        ppf
        "Failed to restore cemented blocks. The cycle '%s' is missing."
        file)
    (obj1 (req "cycle" string))
    (function Missing_cemented_file s -> Some s | _ -> None)
    (fun s -> Missing_cemented_file s) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.corrupted_floating_store"
    ~title:"Corrupted floating store"
    ~description:"Failed to read floating store"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Failed to restore floating blocks. The floating store is corrupted.")
    unit
    (function Corrupted_floating_store -> Some () | _ -> None)
    (fun () -> Corrupted_floating_store) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.protocol_import_invalid_file"
    ~title:"Protocol import invalid file"
    ~description:"Failed to import protocol as the filename is invalid"
    ~pp:(fun ppf filename ->
      Format.fprintf
        ppf
        "Failed to import protocol. The protocol file '%s' is invalid"
        filename)
    (obj1 (req "filename" string))
    (function Invalid_protocol_file filename -> Some filename | _ -> None)
    (fun filename -> Invalid_protocol_file filename) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.target_block_validation_failed"
    ~title:"target block validation failed"
    ~description:"Failed to validate the target block."
    ~pp:(fun ppf (h, errs) ->
      Format.fprintf ppf "Failed to validate block %a: %s" Block_hash.pp h errs)
    (obj2 (req "block" Block_hash.encoding) (req "errors" string))
    (function
      | Target_block_validation_failed (h, errs) -> Some (h, errs) | _ -> None)
    (fun (h, errs) -> Target_block_validation_failed (h, errs)) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.directory_already_exists"
    ~title:"Directory already exists"
    ~description:"The given data directory already exists."
    ~pp:(fun ppf s ->
      Format.fprintf
        ppf
        "Failed to import snasphot as the given directory %s already exists."
        s)
    (obj1 (req "path" string))
    (function Directory_already_exists s -> Some s | _ -> None)
    (fun s -> Directory_already_exists s) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.empty_floating_store"
    ~title:"Empty floating store"
    ~description:"Floating store is empty."
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Failed to export floating blocks: the floating store does not \
         contain any blocks (broken invariant).")
    unit
    (function Empty_floating_store -> Some () | _ -> None)
    (fun () -> Empty_floating_store) ;
  register_error_kind
    `Permanent
    ~id:"snapshot.inconsistent_predecessors"
    ~title:"Inconsistent predecessors"
    ~description:
      "Inconsistent predecessors while validating a legacy snapshot."
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Failed to validate the predecessors: inconsistent hash.")
    unit
    (function Inconsistent_predecessors -> Some () | _ -> None)
    (fun () -> Inconsistent_predecessors) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.import_failure"
    ~title:"Snapshot import failure"
    ~description:"Generic import process failure."
    ~pp:(fun ppf msg -> Format.fprintf ppf "Failure during import: %s." msg)
    (obj1 (req "message" string))
    (function Snapshot_import_failure str -> Some str | _ -> None)
    (fun str -> Snapshot_import_failure str) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.export_failure"
    ~title:"Snapshot export failure"
    ~description:"Generic export process failure."
    ~pp:(fun ppf msg -> Format.fprintf ppf "Failure during export: %s." msg)
    (obj1 (req "message" string))
    (function Snapshot_export_failure str -> Some str | _ -> None)
    (fun str -> Snapshot_export_failure str) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.inconsistent_chain_import"
    ~title:"Inconsistent chain import"
    ~description:
      "The imported chain is inconsistent with the target data directory."
    ~pp:(fun ppf (expected, got) ->
      Format.fprintf
        ppf
        "The chain name contained in the snapshot file (%a) is not consistent \
         with the network configured in the targeted data directory (%a). \
         Please check your configuration file."
        Distributed_db_version.Name.pp
        expected
        Distributed_db_version.Name.pp
        got)
    (obj2
       (req "expected" Distributed_db_version.Name.encoding)
       (req "got" Distributed_db_version.Name.encoding))
    (function
      | Inconsistent_chain_import {expected; got} ->
          Some (expected, got)
      | _ ->
          None)
    (fun (expected, got) -> Inconsistent_chain_import {expected; got})

type metadata = {
  version : int;
  chain_name : Distributed_db_version.Name.t;
  history_mode : History_mode.t;
  block_hash : Block_hash.t;
  level : Int32.t;
  timestamp : Time.Protocol.t;
  context_elements : int;
}

let metadata_encoding =
  let open Data_encoding in
  conv
    (fun { version;
           chain_name;
           history_mode;
           block_hash;
           level;
           timestamp;
           context_elements } ->
      ( version,
        chain_name,
        history_mode,
        block_hash,
        level,
        timestamp,
        context_elements ))
    (fun ( version,
           chain_name,
           history_mode,
           block_hash,
           level,
           timestamp,
           context_elements ) ->
      {
        version;
        chain_name;
        history_mode;
        block_hash;
        level;
        timestamp;
        context_elements;
      })
    (obj7
       (req "version" int31)
       (req "chain_name" Distributed_db_version.Name.encoding)
       (req "mode" History_mode.encoding)
       (req "block_hash" Block_hash.encoding)
       (req "level" int32)
       (req "timestamp" Time.Protocol.encoding)
       (req "context_elements" int31))

let pp_metadata ppf {version; chain_name; history_mode; block_hash; level; _} =
  Format.fprintf
    ppf
    "chain %a, block hash %a at level %ld in %a (snapshot version %d)"
    Distributed_db_version.Name.pp
    chain_name
    Block_hash.pp
    block_hash
    level
    History_mode.pp
    history_mode
    version

let write_snapshot_metadata metadata file =
  let metadata_json =
    Data_encoding.Json.construct metadata_encoding metadata
  in
  Lwt_utils_unix.Json.write_file file metadata_json
  >>= function
  | Error err ->
      Format.kasprintf
        (fun msg -> fail (Cannot_write_metadata msg))
        "%a"
        pp_print_error
        err
  | Ok () ->
      return_unit

let read_snapshot_metadata ~snapshot_file =
  let filename = Naming.(snapshot_file // Snapshot.metadata) in
  let read_config json =
    return (Data_encoding.Json.destruct metadata_encoding json)
  in
  protect
    ~on_error:(fun err ->
      Format.kasprintf
        (fun msg -> fail (Cannot_read_metadata msg))
        "%a"
        pp_print_error
        err)
    (fun () ->
      if Sys.is_directory snapshot_file then
        Lwt_utils_unix.Json.read_file filename
        >>=? fun json -> read_config json
      else
        let ic = Zip.open_in snapshot_file in
        let entry = Zip.find_entry ic Naming.Snapshot.metadata in
        let s = Zip.read_entry ic entry in
        match Data_encoding.Json.from_string s with
        | Ok json ->
            read_config json
        | Error err ->
            fail (Cannot_read_metadata err))

let copy_cemented_blocks ~src_cemented_dir ~dst_cemented_dir
    (files : Cemented_block_store.cemented_blocks_file list) =
  let open Cemented_block_store in
  let nb_cycles = List.length files in
  (* Rebuild fresh indexes: cannot cp because of concurrent accesses *)
  let fresh_level_index =
    Cemented_block_level_index.v
      ~fresh:true
      ~readonly:false
      ~log_size:100_000
      Naming.(dst_cemented_dir // cemented_block_level_index_directory)
  in
  let fresh_hash_index =
    Cemented_block_hash_index.v
      ~fresh:true
      ~readonly:false
      ~log_size:100_000
      Naming.(dst_cemented_dir // cemented_block_hash_index_directory)
  in
  protect (fun () ->
      Lwt_utils_unix.display_progress
        ~pp_print_step:(fun fmt i ->
          Format.fprintf
            fmt
            "Copying cemented blocks and populating indexes: %d/%d cycles"
            i
            nb_cycles)
        (fun notify ->
          (* Bound the number of copying threads *)
          let tasks =
            let rec loop acc l =
              let (l, r) = List.split_n 20 l in
              if r = [] then l :: acc else loop (l :: acc) r
            in
            loop [] files
          in
          Error_monad.iter_s
            (Error_monad.iter_p (fun ({filename; _} as file) ->
                 Cemented_block_store.iter_cemented_file
                   ~cemented_blocks_dir:src_cemented_dir
                   (fun block ->
                     let hash = Block_repr.hash block in
                     let level = Block_repr.level block in
                     Cemented_block_level_index.replace
                       fresh_level_index
                       hash
                       level ;
                     Cemented_block_hash_index.replace
                       fresh_hash_index
                       level
                       hash ;
                     Lwt.return_unit)
                   file
                 >>=? fun () ->
                 let cycle = Naming.(src_cemented_dir // filename) in
                 Lwt_utils_unix.copy_file
                   ~src:cycle
                   ~dst:Naming.(dst_cemented_dir // filename)
                 >>= fun () -> notify () >>= fun () -> return_unit))
            tasks)
      >>=? fun () ->
      Cemented_block_level_index.close fresh_level_index ;
      Cemented_block_hash_index.close fresh_hash_index ;
      return_unit)

let write_floating_block fd (block : Block_repr.t) =
  let bytes = Data_encoding.Binary.to_bytes_exn Block_repr.encoding block in
  let len = Bytes.length bytes in
  Lwt_utils_unix.write_bytes ~pos:0 ~len fd bytes

let export_floating_blocks ~floating_ro_fd ~floating_rw_fd ~export_block =
  let ((limit_hash, limit_level) as export_block_descr) =
    Store.Block.descriptor export_block
  in
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
                fail Empty_floating_store ))
  >>=? fun first_block ->
  let first_block_level = Block_repr.level first_block in
  if Compare.Int32.(limit_level < first_block_level) then
    fail
      (Inconsistent_floating_store
         (export_block_descr, (Block_repr.hash first_block, first_block_level)))
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
              Floating_block_store.iter_s_raw_fd f floating_ro_fd
              >>=? fun () ->
              Lwt_unix.lseek floating_rw_fd 0 Unix.SEEK_SET
              >>= fun _ ->
              Floating_block_store.iter_s_raw_fd f floating_rw_fd
              >>=? fun () -> fail (Missing_target_block export_block_descr))
            (function
              | Done ->
                  return_unit
              | exn ->
                  fail (Cannot_read_floating_store (Printexc.to_string exn))))
        (fun () -> bpush#close ; Lwt.return_unit)
    in
    return (reading_thread, stream)

(* Export the protocol table (info regarding the protocol transitions)
   as well as all the stored protocols *)
let export_protocols protocol_levels ~src_dir ~dst_dir =
  let protocol_tbl_filename = Naming.(dst_dir // Snapshot.protocols_table) in
  Lwt_unix.openfile
    protocol_tbl_filename
    Unix.[O_CREAT; O_TRUNC; O_WRONLY]
    0o444
  >>= fun fd ->
  Lwt.finalize
    (fun () ->
      (* Export protocols table *)
      let bytes =
        Data_encoding.Binary.to_bytes_exn
          Protocol_levels.encoding
          protocol_levels
      in
      Lwt_utils_unix.write_bytes ~pos:0 fd bytes)
    (fun () -> Lwt_utils_unix.safe_close fd >>= fun _ -> Lwt.return_unit)
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

(* Ensures that the data needed to export the snapshot from the target
   block is available:
   - the target_block is not the genesis
   - the target_block and its predecessor are known
   - the context of the predecessor of the target_block must be known
   - at least max_op_ttl(target_block) headers must be available
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

(* Retrieves the block to export based on given block "as hint". As
   the checkpoint is provided as a default value, we must ensure that
   it is valid. It may be not the case when the checkpoint was set in
   the future. In this particular case, the last allowed fork level of
   the current head is chosen. *)
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

(* Returns the list of cemented files to export and an optional list
   of remaining blocks. If the export block is cemented, we need to cut
   the cycle containing the export block accordingly and retrieve the
   extra blocks. *)
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
            Compare.Int32.(export_block_level > end_level))
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
               (* TODO explain this... *)
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
        Store.Chain_traversal.path
          chain_store
          ~from_block:first_block
          ~to_block:export_block
        >>= function
        | None ->
            fail Cannot_retrieve_block_interval
        | Some floating_blocks ->
            (* Don't forget to add the first block as
               [Chain_traversal.path] does not include the lower-bound
               block *)
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

(* Ensures that the history mode requested to export is compatible
   with the current storage *)
let check_history_mode chain_store ~rolling =
  match (Store.Chain.history_mode chain_store : History_mode.t) with
  | Archive | Full _ ->
      return_unit
  | Rolling _ when rolling ->
      return_unit
  | Rolling _ as stored ->
      fail (Incompatible_history_mode {stored; requested = Full {offset = 0}})

let export_floating_block_stream ~snapshot_dir floating_block_stream =
  Lwt_stream.is_empty floating_block_stream
  >>= fun is_empty ->
  if is_empty then return_unit
  else
    Lwt_utils_unix.display_progress
      ~every:10
      ~pp_print_step:(fun fmt i ->
        Format.fprintf fmt "Copying floating blocks: %d blocks copied" i)
      (fun notify ->
        let floating_file =
          Naming.(snapshot_dir // Snapshot.floating_blocks)
        in
        Lwt_unix.openfile floating_file Unix.[O_CREAT; O_TRUNC; O_WRONLY] 0o444
        >>= fun fd ->
        Lwt.finalize
          (fun () ->
            Lwt_stream.iter_s
              (fun b -> write_floating_block fd b >>= fun () -> notify ())
              floating_block_stream)
          (fun () -> Lwt_utils_unix.safe_close fd >>= fun _ -> Lwt.return_unit))
    >>= fun () -> return_unit

let format_snapshot_export_path chain_name export_block export_mode = function
  | Some snapshot_file ->
      snapshot_file
  | None ->
      (* The generated filename follows this pattern:
         <NETWORK>-<BLOCK_HASH>-<BLOCK_LEVEL>.<SNAPSHOT_KIND> *)
      Format.asprintf
        "%a-%a-%ld.%a"
        Distributed_db_version.Name.pp
        chain_name
        Block_hash.pp
        (Store.Block.hash export_block)
        (Store.Block.level export_block)
        History_mode.pp_short
        export_mode

let ensure_valid_export_path snapshot_export_path =
  Lwt_unix.file_exists snapshot_export_path
  >>= fun snapshot_path_exists ->
  fail_when snapshot_path_exists (Invalid_export_path snapshot_export_path)

let export_rolling ?snapshot_file ~store_dir ~context_dir ~snapshot_dir ~block
    ~rolling ~chain_name genesis =
  let export_rolling_f chain_store =
    check_history_mode chain_store ~rolling
    >>=? fun () ->
    retrieve_export_block chain_store block
    >>=? fun (export_block, pred_block, lowest_block_level_needed) ->
    let export_mode = History_mode.Rolling {offset = 0} in
    let snapshot_export_path =
      format_snapshot_export_path
        chain_name
        export_block
        export_mode
        snapshot_file
    in
    lwt_emit (Export_info (export_mode, Store.Block.descriptor export_block))
    >>= fun () ->
    (* Blocks *)
    (* Read the store to gather only the necessary blocks *)
    Store.Block.read_block_by_level chain_store lowest_block_level_needed
    >>=? fun minimum_block ->
    Store.Chain_traversal.path
      chain_store
      ~from_block:minimum_block
      ~to_block:pred_block
    >>= (function
          | None ->
              fail Cannot_retrieve_block_interval
          | Some blocks ->
              (* Don't forget to add the first block as
                 [Chain_traversal.path] does not include the
                 lower-bound block *)
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
    (* Protocols *)
    Store.Chain.all_protocol_levels chain_store
    >>= fun protocol_levels ->
    (* Filter protocols s.t. forall proto. proto.level >=
       caboose.proto_level. *)
    let protocol_levels =
      Protocol_levels.(
        filter
          (fun level {block; _} ->
            level >= Store.Block.proto_level minimum_block
            || Store.Block.is_genesis chain_store (fst block))
          protocol_levels)
    in
    return
      ( export_mode,
        export_block,
        pred_block,
        protocol_levels,
        (return_unit, floating_block_stream),
        snapshot_export_path )
  in
  Store.Unsafe.open_for_snapshot_export
    ~store_dir
    ~context_dir
    genesis
    ~locked_f:export_rolling_f
  >>=? fun ( export_mode,
             export_block,
             pred_block,
             protocol_levels,
             (return_unit, floating_block_stream),
             snapshot_export_path ) ->
  ensure_valid_export_path snapshot_export_path
  >>=? fun () ->
  (* TODO: when the context's GC is implemented, make sure a context
     pruning cannot occur while the dump context is being run. For
     now, it is performed outside the lock to allow the node from
     getting stuck while waiting a merge. *)
  Context.init ~readonly:true context_dir
  >>= fun context_index ->
  dump_context context_index ~snapshot_dir ~pred_block ~export_block
  >>=? fun written_context_elements ->
  return
    ( export_mode,
      export_block,
      protocol_levels,
      written_context_elements,
      (return_unit, floating_block_stream),
      snapshot_export_path )

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

let export_full ?snapshot_file ~store_dir ~context_dir ~snapshot_dir
    ~dst_cemented_dir ~block ~rolling ~chain_name genesis =
  let export_full_f chain_store =
    check_history_mode chain_store ~rolling
    >>=? fun () ->
    retrieve_export_block chain_store block
    >>=? fun (export_block, pred_block, _lowest_block_level_needed) ->
    let export_mode = History_mode.Full {offset = 0} in
    let snapshot_export_path =
      format_snapshot_export_path
        chain_name
        export_block
        export_mode
        snapshot_file
    in
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
        Store.Chain.all_protocol_levels chain_store
        >>= fun protocol_levels ->
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
            pred_block,
            protocol_levels,
            (src_cemented_dir, cemented_table),
            (ro_fd, rw_fd),
            extra_floating_blocks,
            should_filter_indexes,
            snapshot_export_path ))
      (fun exn ->
        Lwt_utils_unix.safe_close ro_fd
        >>= fun _ ->
        Lwt_utils_unix.safe_close rw_fd >>= fun _ -> Lwt.return (error_exn exn))
  in
  Store.Unsafe.open_for_snapshot_export
    ~store_dir
    ~context_dir
    genesis
    ~locked_f:export_full_f
  >>=? fun ( export_mode,
             export_block,
             pred_block,
             protocol_levels,
             (src_cemented_dir, cemented_table),
             (floating_ro_fd, floating_rw_fd),
             extra_floating_blocks,
             should_filter_indexes,
             snapshot_export_path ) ->
  ensure_valid_export_path snapshot_export_path
  >>=? fun () ->
  (* TODO: when the context's GC is implemented, make sure a context
     pruning cannot occur while the dump context is being run. For
     now, it is performed outside the lock to allow the node from
     getting stuck while waiting a merge. *)
  Context.init ~readonly:true context_dir
  >>= fun context_index ->
  dump_context context_index ~snapshot_dir ~pred_block ~export_block
  >>=? fun written_context_elements ->
  copy_cemented_blocks ~src_cemented_dir ~dst_cemented_dir cemented_table
  >>=? fun () ->
  if should_filter_indexes && cemented_table <> [] then
    filter_indexes ~dst_cemented_dir (List.last_exn cemented_table).end_level ;
  let finalizer () =
    Lwt_utils_unix.safe_close floating_ro_fd
    >>= fun _ ->
    Lwt_utils_unix.safe_close floating_rw_fd >>= fun _ -> Lwt.return_unit
  in
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
      (reading_thread, floating_block_stream),
      snapshot_export_path )

let zip_snapshot dir out =
  Lwt_utils_unix.display_progress
    ~every:1
    ~pp_print_step:(fun fmt i ->
      Format.fprintf fmt "Compressing snapshot: %d files treated" i)
    (fun notify ->
      if not (Sys.file_exists dir && Sys.is_directory dir) then
        fail
          (Snapshot_export_failure
             (Format.sprintf "unable to retrieve snapshot directory %s" dir))
      else
        let oc = Zip.open_out out in
        Lwt.finalize
          (fun () ->
            let rec zip_dir pathacc dir =
              let files = Lwt_unix.files_of_directory dir in
              Lwt_stream.iter_s
                (fun file ->
                  if
                    file = Filename.current_dir_name
                    || file = Filename.parent_dir_name
                  then Lwt.return_unit
                  else
                    let file = Filename.concat dir file in
                    if Sys.is_directory file then (
                      Zip.add_entry
                        ""
                        oc
                        ( Filename.(concat pathacc (basename file))
                        ^ Filename.dir_sep ) ;
                      zip_dir Filename.(concat pathacc (basename file)) file )
                    else (
                      Zip.copy_file_to_entry
                        file
                        oc
                        Filename.(concat pathacc (basename file)) ;
                      notify () ))
                files
            in
            zip_dir "" dir >>= return)
          (fun () -> Zip.close_out oc ; Lwt.return_unit))

let unzip_snapshot zipfile path =
  Lwt_utils_unix.display_progress
    ~every:1
    ~pp_print_step:(fun fmt i ->
      Format.fprintf fmt "Inflating snapshot: %d files decompressed" i)
    (fun notify ->
      let ic = Zip.open_in zipfile in
      Lwt.finalize
        (fun () ->
          let entries = Zip.entries ic in
          let make_path path =
            if Sys.file_exists path then Lwt.return_unit
            else
              String.split_path (Filename.dirname path)
              |> Lwt_list.iter_s (fun s -> Lwt_utils_unix.create_dir s)
          in
          if not (List.length entries > 0) then
            fail
              (Snapshot_import_failure
                 "compressed snapshot does not contain any entries")
          else
            Lwt_list.iter_s
              (fun entry ->
                make_path (Filename.dirname entry.Zip.filename)
                >>= fun () ->
                if entry.Zip.is_directory then
                  Lwt_utils_unix.create_dir
                    (Filename.concat path entry.filename)
                else (
                  Zip.copy_entry_to_file
                    ic
                    entry
                    (Filename.concat path entry.filename) ;
                  notify () ))
              entries
            >>= return)
        (fun () -> Zip.close_in ic ; Lwt.return_unit))

let export ?snapshot_file ?(rolling = false) ?(compress = true) ~block
    ~store_dir ~context_dir ~chain_name genesis =
  let snapshot_dir = Filename.temp_file Naming.Snapshot.temp_export_dir "" in
  Lwt_unix.unlink snapshot_dir
  >>= fun () ->
  create_snapshot_dir ~snapshot_dir
  >>=? fun (dst_cemented_dir, dst_protocol_dir) ->
  let dir_cleaner path =
    lwt_emit Cleaning_after_failure
    >>= fun () ->
    Lwt_list.iter_s
      (fun path ->
        Lwt.catch
          (fun () ->
            if Sys.is_directory path then Lwt_utils_unix.remove_dir path
            else Lwt_unix.unlink path)
          (fun _ -> Lwt.return_unit))
      path
  in
  protect
    ~on_error:(fun errors ->
      dir_cleaner [snapshot_dir] >>= fun () -> Lwt.return (Error errors))
    (fun () ->
      ( if rolling then
        export_rolling
          ?snapshot_file
          ~store_dir
          ~context_dir
          ~snapshot_dir
          ~block
          ~rolling
          ~chain_name
          genesis
      else
        export_full
          ?snapshot_file
          ~store_dir
          ~context_dir
          ~snapshot_dir
          ~dst_cemented_dir
          ~block
          ~rolling
          ~chain_name
          genesis )
      >>=? fun ( export_mode,
                 export_block,
                 protocol_levels,
                 written_context_elements,
                 (reading_thread, floating_block_stream),
                 snapshot_export_path ) ->
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
        {
          version = current_version;
          chain_name;
          history_mode = export_mode;
          block_hash = Store.Block.hash export_block;
          level = Store.Block.level export_block;
          timestamp = Store.Block.timestamp export_block;
          context_elements = written_context_elements;
        }
      in
      write_snapshot_metadata
        metadata
        Naming.(snapshot_dir // Snapshot.metadata)
      >>=? fun () -> return snapshot_export_path)
  >>=? fun snapshot_export_path ->
  protect
    ~on_error:(fun errors ->
      dir_cleaner [snapshot_dir; snapshot_export_path]
      >>= fun () -> Lwt.return (Error errors))
    (fun () ->
      ( if compress then
        zip_snapshot snapshot_dir snapshot_export_path
        >>=? fun () -> Lwt_utils_unix.remove_dir snapshot_dir >>= return
      else Lwt_unix.rename snapshot_dir snapshot_export_path >>= return )
      >>=? fun () ->
      lwt_emit (Export_success snapshot_export_path) >>= fun () -> return_unit)

let restore_cemented_blocks ?(check_consistency = true) ~should_rename
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
  ( if should_rename then Lwt_unix.rename snapshot_cemented_dir dst_cemented_dir
  else Lwt.return_unit )
  >>= fun () ->
  ( if
    (not should_rename)
    && Sys.file_exists dst_level_dir
    && Sys.file_exists dst_hash_dir
  then
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
      | file
        when file = Filename.current_dir_name
             || file = Filename.parent_dir_name
             || file = Naming.cemented_block_hash_index_directory
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
          if not is_valid then fail (Invalid_cemented_file file)
          else return_true)
    files
  >>=? fun cemented_files ->
  let nb_cemented_files = List.length cemented_files in
  ( if (not should_rename) && nb_cemented_files > 0 then
    (* Don't copy anything if it was just a renaming, the archive
         inflating is enough. *)
    Lwt_utils_unix.display_progress
      ~pp_print_step:(fun fmt i ->
        Format.fprintf
          fmt
          "Copying cycles: %d/%d (%d%%)"
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
        then fail (Missing_cemented_file cemented_file)
        else return_unit)
      (List.sort compare cemented_files)
    >>=? fun () ->
    Lwt_utils_unix.display_progress
      ~pp_print_step:(fun fmt i ->
        Format.fprintf
          fmt
          "Restoring cycles consistency: %d/%d (%d%%)"
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
  if not (Sys.file_exists floating_blocks_file) then
    return (return_unit, Lwt_stream.of_list [])
  else
    Lwt_unix.openfile floating_blocks_file Unix.[O_RDONLY] 0o444
    >>= fun fd ->
    let (stream, bounded_push) = Lwt_stream.create_bounded 1000 in
    let rec loop ?pred_block nb_bytes_left =
      if nb_bytes_left < 0 then fail Corrupted_floating_store
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
        (fun () ->
          Lwt_unix.lseek fd 0 Unix.SEEK_END
          >>= fun eof_offset ->
          Lwt_unix.lseek fd 0 Unix.SEEK_SET >>= fun _ -> loop eof_offset)
        (fun () ->
          bounded_push#close ;
          Lwt_utils_unix.safe_close fd >>= fun _ -> Lwt.return_unit)
    in
    return (reading_thread, stream)

let restore_protocols ~should_rename ~snapshot_protocol_dir ~dst_protocol_dir =
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
        | file
          when file = Filename.current_dir_name
               || file = Filename.parent_dir_name
               || file = Filename.basename protocol_tbl_filename ->
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
          fail (Invalid_protocol_file file))
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
        ( if should_rename then Lwt_unix.rename src dst
        else Lwt_utils_unix.copy_file ~src ~dst )
        >>= fun () ->
        Lwt_utils_unix.read_file dst
        >>= fun protocol_sources ->
        match Protocol.of_bytes (Bytes.unsafe_of_string protocol_sources) with
        | None ->
            fail (Cannot_decode_protocol protocol_filename)
        | Some p ->
            let hash = Protocol.hash p in
            notify ()
            >>= fun () ->
            fail_unless
              (Protocol_hash.equal expected_hash hash)
              (Inconsistent_protocol_hash
                 {expected = expected_hash; got = hash})
      in
      iter_s validate_and_copy protocols)
  >>=? fun () -> return protocol_levels

let import_log_notice ?snapshot_metadata filename block =
  let metadata =
    Option.map
      (fun metadata -> Format.asprintf "%a" pp_metadata metadata)
      snapshot_metadata
  in
  lwt_emit (Import_info {filename; metadata})
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
    (Inconsistent_context_hash
       {
         expected = block_header.Block_header.shell.context;
         got = validation_store.Tezos_validation.Block_validation.context_hash;
       })

let restore_and_apply_context ?expected_block ~context_index ~snapshot_dir
    ~user_activated_upgrades ~user_activated_protocol_overrides metadata
    genesis chain_id =
  (* Start by committing genesis *)
  Context.commit_genesis
    context_index
    ~chain_id
    ~time:genesis.Genesis.time
    ~protocol:genesis.protocol
  >>=? fun genesis_ctxt_hash ->
  (* Restore context *)
  Context.restore_context
    context_index
    ?expected_block
    ~context_file_path:Naming.(snapshot_dir // Snapshot.context)
    ~target_block:metadata.block_hash
    ~nb_context_elements:metadata.context_elements
  >>=? fun ({block_header; operations; predecessor_header} as block_data) ->
  let pred_context_hash = predecessor_header.shell.context in
  Context.checkout context_index pred_context_hash
  >>= (function
        | Some ch ->
            return ch
        | None ->
            fail (Inconsistent_context pred_context_hash))
  >>=? fun predecessor_context ->
  let apply_environment =
    {
      Block_validation.max_operations_ttl =
        Int32.to_int predecessor_header.shell.level;
      chain_id;
      predecessor_block_header = predecessor_header;
      predecessor_context;
      user_activated_upgrades;
      user_activated_protocol_overrides;
    }
  in
  Block_validation.apply apply_environment block_header operations
  >>= (function
        | Ok block_validation_result ->
            return block_validation_result
        | Error errs ->
            Format.kasprintf
              (fun errs ->
                fail
                  (Target_block_validation_failed
                     (Block_header.hash block_header, errs)))
              "%a"
              pp_print_error
              errs)
  >>=? fun block_validation_result ->
  check_context_hash_consistency
    block_validation_result.validation_store
    block_header
  >>=? fun () -> return (block_data, genesis_ctxt_hash, block_validation_result)

(* TODO parallelise in another process *)
(* TODO? remove patch context *)
let import ?patch_context ?block:expected_block ?(check_consistency = true)
    ~snapshot_file ~dst_store_dir ~dst_context_dir ~chain_name
    ~user_activated_upgrades ~user_activated_protocol_overrides
    (genesis : Genesis.t) =
  fail_when
    (Sys.file_exists dst_store_dir)
    (Directory_already_exists dst_store_dir)
  >>=? fun () ->
  let dst_protocol_dir = Naming.(dst_store_dir // protocol_store_directory) in
  let chain_id = Chain_id.of_block_hash genesis.block in
  let dst_chain_store_dir = Naming.(dst_store_dir // chain_store chain_id) in
  let dst_cemented_dir =
    Naming.(dst_chain_store_dir // cemented_blocks_directory)
  in
  Lwt_list.iter_s
    (Lwt_utils_unix.create_dir ~perm:0o755)
    [dst_store_dir; dst_protocol_dir; dst_chain_store_dir; dst_cemented_dir]
  >>= fun () ->
  fail_unless
    (Sys.file_exists snapshot_file)
    (Snapshot_file_not_found snapshot_file)
  >>=? fun () ->
  read_snapshot_metadata ~snapshot_file
  >>=? fun snapshot_metadata ->
  fail_unless
    (Distributed_db_version.Name.equal chain_name snapshot_metadata.chain_name)
    (Inconsistent_chain_import
       {expected = snapshot_metadata.chain_name; got = chain_name})
  >>=? fun () ->
  import_log_notice ~snapshot_metadata snapshot_file expected_block
  >>= fun () ->
  ( if Sys.is_directory snapshot_file then
    (* Not compressed *)
    return (false, snapshot_file)
  else
    let snapshot_dir =
      Filename.concat dst_store_dir (Filename.basename snapshot_file)
    in
    Lwt_utils_unix.create_dir snapshot_dir
    >>= fun () ->
    protect (fun () -> unzip_snapshot snapshot_file snapshot_dir)
    >>=? fun () -> return (true, snapshot_dir) )
  >>=? fun (is_compressed, snapshot_dir) ->
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
  (* Restore store *)
  (* Restore protocols *)
  restore_protocols
    ~should_rename:is_compressed
    ~snapshot_protocol_dir:Naming.(snapshot_dir // Snapshot.(protocols))
    ~dst_protocol_dir
  >>=? fun protocol_levels ->
  (* Restore cemented dir *)
  restore_cemented_blocks
    ~should_rename:is_compressed
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
  (* Set the history mode with the default offset *)
  (let open History_mode in
  match snapshot_metadata.history_mode with
  | Archive ->
      assert false
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
  ( if is_compressed then Lwt_utils_unix.remove_dir snapshot_dir
  else Lwt.return_unit )
  >>= fun () ->
  lwt_emit (Import_success snapshot_dir) >>= fun () -> return_unit
