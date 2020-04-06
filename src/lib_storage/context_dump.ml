(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2018-2020 Nomadic Labs. <nomadic@tezcore.com>               *)
(* Copyright (c) 2018-2020 Tarides <contact@tarides.com>                     *)
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

type error +=
  | System_write_error of string
  | Bad_hash of string * Bytes.t * Bytes.t
  | Context_not_found of Bytes.t
  | System_read_error of string
  | Inconsistent_snapshot_file
  | Inconsistent_snapshot_data
  | Missing_snapshot_data
  | Invalid_snapshot_version of string * string
  | Restore_context_failure
  | Inconsistent_imported_block of Block_hash.t * Block_hash.t

let () =
  let open Data_encoding in
  register_error_kind
    `Permanent
    ~id:"context_dump.writing_error"
    ~title:"Writing error"
    ~description:"Cannot write in file for context dump"
    ~pp:(fun ppf s ->
      Format.fprintf ppf "Unable to write file for context dumping: %s" s)
    (obj1 (req "context_dump_no_space" string))
    (function System_write_error s -> Some s | _ -> None)
    (fun s -> System_write_error s) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.bad_hash"
    ~title:"Bad hash"
    ~description:"Wrong hash given"
    ~pp:(fun ppf (ty, his, hshould) ->
      Format.fprintf
        ppf
        "Wrong hash [%s] given: %s, should be %s"
        ty
        (Bytes.to_string his)
        (Bytes.to_string hshould))
    (obj3
       (req "hash_ty" string)
       (req "hash_is" bytes)
       (req "hash_should" bytes))
    (function
      | Bad_hash (ty, his, hshould) -> Some (ty, his, hshould) | _ -> None)
    (fun (ty, his, hshould) -> Bad_hash (ty, his, hshould)) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.context_not_found"
    ~title:"Context not found"
    ~description:"Cannot find context corresponding to hash"
    ~pp:(fun ppf mb ->
      Format.fprintf ppf "No context with hash: %s" (Bytes.to_string mb))
    (obj1 (req "context_not_found" bytes))
    (function Context_not_found mb -> Some mb | _ -> None)
    (fun mb -> Context_not_found mb) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.system_read_error"
    ~title:"System read error"
    ~description:"Failed to read file"
    ~pp:(fun ppf uerr ->
      Format.fprintf
        ppf
        "Error while reading file for context dumping: %s"
        uerr)
    (obj1 (req "system_read_error" string))
    (function System_read_error e -> Some e | _ -> None)
    (fun e -> System_read_error e) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.inconsistent_snapshot_file"
    ~title:"Inconsistent snapshot file"
    ~description:"Error while opening snapshot file"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Failed to read snapshot file. The provided file is inconsistent.")
    empty
    (function Inconsistent_snapshot_file -> Some () | _ -> None)
    (fun () -> Inconsistent_snapshot_file) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.inconsistent_snapshot_data"
    ~title:"Inconsistent snapshot data"
    ~description:"The data provided by the snapshot is inconsistent"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "The data provided by the snapshot file is inconsistent (context_hash \
         does not correspond for block).")
    empty
    (function Inconsistent_snapshot_data -> Some () | _ -> None)
    (fun () -> Inconsistent_snapshot_data) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.missing_snapshot_data"
    ~title:"Missing data in imported snapshot"
    ~description:"Mandatory data missing while reaching end of snapshot file."
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Mandatory data is missing is the provided snapshot file.")
    empty
    (function Missing_snapshot_data -> Some () | _ -> None)
    (fun () -> Missing_snapshot_data) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.invalid_snapshot_version"
    ~title:"Invalid snapshot version"
    ~description:"The version of the snapshot to import is not valid"
    ~pp:(fun ppf (found, expected) ->
      Format.fprintf
        ppf
        "The snapshot to import has version \"%s\" but \"%s\" was expected."
        found
        expected)
    (obj2 (req "found" string) (req "expected" string))
    (function
      | Invalid_snapshot_version (found, expected) ->
          Some (found, expected)
      | _ ->
          None)
    (fun (found, expected) -> Invalid_snapshot_version (found, expected)) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.restore_context_failure"
    ~title:"Failed to restore context"
    ~description:"Internal error while restoring the context"
    ~pp:(fun ppf () ->
      Format.fprintf ppf "Internal error while restoring the context.")
    empty
    (function Restore_context_failure -> Some () | _ -> None)
    (fun () -> Restore_context_failure) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.inconsistent_imported_block"
    ~title:"Inconsistent imported block"
    ~description:"The imported block is not the expected one."
    ~pp:(fun ppf (got, exp) ->
      Format.fprintf
        ppf
        "The block contained in the file is %a instead of %a."
        Block_hash.pp
        got
        Block_hash.pp
        exp)
    (obj2
       (req "block_hash" Block_hash.encoding)
       (req "block_hash_expected" Block_hash.encoding))
    (function
      | Inconsistent_imported_block (got, exp) -> Some (got, exp) | _ -> None)
    (fun (got, exp) -> Inconsistent_imported_block (got, exp))

module type Dump_interface = sig
  type index

  type context

  type tree

  type hash

  type step = string

  type key = step list

  type commit_info

  type batch

  val batch : index -> (batch -> 'a Lwt.t) -> 'a Lwt.t

  val commit_info_encoding : commit_info Data_encoding.t

  val hash_encoding : hash Data_encoding.t

  module Block_header : sig
    type t = Block_header.t

    val to_bytes : t -> Bytes.t

    val of_bytes : Bytes.t -> t option

    val equal : t -> t -> bool

    val encoding : t Data_encoding.t
  end

  module Pruned_block : sig
    type t

    val to_bytes : t -> Bytes.t

    val of_bytes : Bytes.t -> t option

    val header : t -> Block_header.t

    val encoding : t Data_encoding.t
  end

  module Block_data : sig
    type t

    val to_bytes : t -> Bytes.t

    val of_bytes : Bytes.t -> t option

    val header : t -> Block_header.t

    val operations : t -> Operation.t list list

    val predecessor_header : t -> Block_header.t

    val encoding : t Data_encoding.t
  end

  module Protocol_data : sig
    type t

    val to_bytes : t -> Bytes.t

    val of_bytes : Bytes.t -> t option

    val encoding : t Data_encoding.t
  end

  module Commit_hash : sig
    type t

    val to_bytes : t -> Bytes.t

    val of_bytes : Bytes.t -> t tzresult

    val encoding : t Data_encoding.t
  end

  (* commit manipulation (for parents) *)
  val context_parents : context -> Commit_hash.t list

  (* Commit info *)
  val context_info : context -> commit_info

  (* block header manipulation *)
  val get_context : index -> Block_header.t -> context option Lwt.t

  val set_context :
    info:commit_info ->
    parents:Commit_hash.t list ->
    context ->
    Block_header.t ->
    bool Lwt.t

  (* for dumping *)
  val context_tree : context -> tree

  val tree_hash : tree -> hash

  val sub_tree : tree -> key -> tree option Lwt.t

  val tree_list : tree -> (step * [`Contents | `Node]) list Lwt.t

  val tree_content : tree -> string option Lwt.t

  (* for restoring *)
  val make_context : index -> context

  val update_context : context -> tree -> context

  val add_string : batch -> string -> tree Lwt.t

  val add_dir : batch -> (step * hash) list -> tree option Lwt.t
end

module type S = sig
  type index

  type context

  type block_header

  type block_data

  type pruned_block

  type protocol_data

  val dump_context_fd :
    index -> block_data -> context_fd:Lwt_unix.file_descr -> int tzresult Lwt.t

  val restore_context_fd :
    index ->
    ?expected_block:string ->
    fd:Lwt_unix.file_descr ->
    metadata:Snapshot_version.metadata ->
    block_data tzresult Lwt.t
end

module type S_legacy = sig
  type index

  type context

  type block_header

  type block_data

  type pruned_block

  type protocol_data

  val restore_context_fd :
    index ->
    fd:Lwt_unix.file_descr ->
    ?expected_block:string ->
    handle_block:(Block_hash.t * pruned_block -> unit tzresult Lwt.t) ->
    handle_protocol_data:(protocol_data -> unit tzresult Lwt.t) ->
    block_validation:(block_header option ->
                     Block_hash.t ->
                     pruned_block ->
                     unit tzresult Lwt.t) ->
    (block_header * block_data * Block_header.t option) tzresult Lwt.t
end

module Make (I : Dump_interface) = struct
  type command =
    | Root of {block_data : I.Block_data.t}
    | Node of (string * I.hash) list
    | Blob of string
    | Eoc of {info : I.commit_info; parents : I.Commit_hash.t list}
    | Eof

  (* Command encoding. *)

  let blob_encoding =
    let open Data_encoding in
    case
      ~title:"blob"
      (Tag (Char.code 'b'))
      string
      (function Blob string -> Some string | _ -> None)
      (function string -> Blob string)

  let node_encoding =
    let open Data_encoding in
    case
      ~title:"node"
      (Tag (Char.code 'n'))
      (list (obj2 (req "name" string) (req "hash" I.hash_encoding)))
      (function Node x -> Some x | _ -> None)
      (function x -> Node x)

  let eof_encoding =
    let open Data_encoding in
    case
      ~title:"eof"
      (Tag (Char.code 'e'))
      empty
      (function Eof -> Some () | _ -> None)
      (fun () -> Eof)

  let root_encoding =
    let open Data_encoding in
    case
      ~title:"root"
      (Tag (Char.code 'r'))
      (obj1 (req "block_data" I.Block_data.encoding))
      (function Root {block_data} -> Some block_data | _ -> None)
      (fun block_data -> Root {block_data})

  let eoc_encoding =
    let open Data_encoding in
    case
      ~title:"eoc"
      (Tag (Char.code 'c'))
      (obj2
         (req "info" I.commit_info_encoding)
         (req "parents" (list I.Commit_hash.encoding)))
      (function Eoc {info; parents} -> Some (info, parents) | _ -> None)
      (fun (info, parents) -> Eoc {info; parents})

  let command_encoding =
    Data_encoding.union
      ~tag_size:`Uint8
      [blob_encoding; node_encoding; eoc_encoding; root_encoding; eof_encoding]

  (* IO toolkit. *)

  let rec read_string rbuf ~len =
    let (fd, buf, ofs, total) = !rbuf in
    if Bytes.length buf - ofs < len then (
      let blen = Bytes.length buf - ofs in
      let neu = Bytes.create (blen + 1_000_000) in
      Bytes.blit buf ofs neu 0 blen ;
      Lwt_unix.read fd neu blen 1_000_000
      >>= fun bread ->
      total := !total + bread ;
      if bread = 0 then fail Inconsistent_snapshot_file
      else
        let neu =
          if bread <> 1_000_000 then Bytes.sub neu 0 (blen + bread) else neu
        in
        rbuf := (fd, neu, 0, total) ;
        read_string rbuf ~len )
    else
      let res = Bytes.sub_string buf ofs len in
      rbuf := (fd, buf, ofs + len, total) ;
      return res

  let read_mbytes rbuf b =
    read_string rbuf ~len:(Bytes.length b)
    >>=? fun string ->
    Bytes.blit_string string 0 b 0 (Bytes.length b) ;
    return ()

  let set_int64 buf i =
    let b = Bytes.create 8 in
    EndianBytes.BigEndian.set_int64 b 0 i ;
    Buffer.add_bytes buf b

  let get_int64 rbuf =
    read_string ~len:8 rbuf
    >>=? fun s -> return @@ EndianString.BigEndian.get_int64 s 0

  let set_mbytes buf b =
    set_int64 buf (Int64.of_int (Bytes.length b)) ;
    Buffer.add_bytes buf b

  let get_mbytes rbuf =
    get_int64 rbuf >>|? Int64.to_int
    >>=? fun l ->
    let b = Bytes.create l in
    read_mbytes rbuf b >>=? fun () -> return b

  (* Getter and setters *)

  let get_command rbuf =
    get_mbytes rbuf
    >>|? fun bytes -> Data_encoding.Binary.of_bytes_exn command_encoding bytes

  let set_root buf block_data =
    let root = Root {block_data} in
    let bytes = Data_encoding.Binary.to_bytes_exn command_encoding root in
    set_mbytes buf bytes

  let set_node buf contents =
    let bytes =
      Data_encoding.Binary.to_bytes_exn command_encoding (Node contents)
    in
    set_mbytes buf bytes

  let set_blob buf data =
    let bytes =
      Data_encoding.Binary.to_bytes_exn command_encoding (Blob data)
    in
    set_mbytes buf bytes

  let set_eoc buf info parents =
    let eoc = Eoc {info; parents} in
    let bytes = Data_encoding.Binary.to_bytes_exn command_encoding eoc in
    set_mbytes buf bytes

  let set_end buf =
    let bytes = Data_encoding.Binary.to_bytes_exn command_encoding Eof in
    set_mbytes buf bytes

  let dump_context_fd idx block_data ~context_fd =
    (* Dumping *)
    let buf = Buffer.create 1_000_000 in
    let written = ref 0 in
    let flush fd () =
      let contents = Buffer.contents buf in
      Buffer.clear buf ;
      written := !written + String.length contents ;
      Lwt_utils_unix.write_string fd contents
    in
    let maybe_flush fd () =
      if Buffer.length buf > 500_000 then flush fd () else Lwt.return_unit
    in
    (* Noticing the visited hashes *)
    let visited_hash = Hashtbl.create 1000 in
    let visited h = Hashtbl.mem visited_hash h in
    let set_visit h = Hashtbl.add visited_hash h () in
    (* Folding through a node *)
    let fold_tree_path ctxt tree notify =
      let elements = ref 0 in
      let rec fold_tree_path ctxt tree =
        I.tree_list tree
        >>= fun keys ->
        let keys = List.sort (fun (a, _) (b, _) -> String.compare a b) keys in
        Lwt_list.map_s
          (fun (name, kind) ->
            I.sub_tree tree [name]
            >>= function
            | None ->
                assert false
            | Some sub_tree ->
                let hash = I.tree_hash sub_tree in
                ( if visited hash then Lwt.return_unit
                else
                  notify ()
                  >>= fun () ->
                  incr elements ;
                  set_visit hash ;
                  (* There cannot be a cycle *)
                  match kind with
                  | `Node ->
                      fold_tree_path ctxt sub_tree
                  | `Contents -> (
                      I.tree_content sub_tree
                      >>= function
                      | None ->
                          assert false
                      | Some data ->
                          set_blob buf data ; maybe_flush context_fd () ) )
                >|= fun () -> (name, hash))
          keys
        >>= fun sub_keys -> set_node buf sub_keys ; maybe_flush context_fd ()
      in
      fold_tree_path ctxt tree >>= fun () -> Lwt.return !elements
    in
    Lwt.catch
      (fun () ->
        let pred_block_header = I.Block_data.predecessor_header block_data in
        I.get_context idx pred_block_header
        >>= function
        | None ->
            fail
            @@ Context_not_found (I.Block_header.to_bytes pred_block_header)
        | Some ctxt ->
            Lwt_utils_unix.display_progress
              ~every:1000
              ~pp_print_step:(fun fmt i ->
                Format.fprintf
                  fmt
                  "Copying context: %dK elements, %s written..."
                  (i / 1000)
                  ( if !written > 1_048_576 then
                    Format.asprintf "%dMiB" (!written / 1_048_576)
                  else Format.asprintf "%dKiB" (!written / 1_024) ))
              (fun notify ->
                set_root buf block_data ;
                let tree = I.context_tree ctxt in
                fold_tree_path ctxt tree notify
                >>= fun nb_elements ->
                let parents = I.context_parents ctxt in
                set_eoc buf (I.context_info ctxt) parents ;
                set_end buf ;
                return_unit
                >>=? fun () ->
                flush context_fd () >>= fun () -> return nb_elements))
      (function
        | Unix.Unix_error (e, _, _) ->
            fail @@ System_write_error (Unix.error_message e)
        | err ->
            Lwt.fail err)

  (* Restoring *)

  let restore_context_fd index ?expected_block ~fd ~metadata =
    let read = ref 0 in
    let rbuf = ref (fd, Bytes.empty, 0, read) in
    (* Editing the repository *)
    let add_blob t blob = I.add_string t blob >>= fun tree -> return tree in
    let add_dir t keys =
      I.add_dir t keys
      >>= function
      | None -> fail Restore_context_failure | Some tree -> return tree
    in
    let restore (metadata : Snapshot_version.metadata) =
      let context_elements = metadata.context_elements in
      let first_pass () =
        get_command rbuf
        >>=? function
        | Root {block_data} ->
            (* Checks that the block hash imported by the snapshot is the expected one *)
            let imported_block_header = I.Block_data.header block_data in
            let imported_block_hash =
              Block_header.hash imported_block_header
            in
            ( match expected_block with
            | Some str ->
                let bh = Block_hash.of_b58check_exn str in
                fail_unless
                  (Block_hash.equal bh imported_block_hash)
                  (Inconsistent_imported_block (imported_block_hash, bh))
            | None ->
                return_unit )
            >>=? fun () ->
            (* Checks that the block hash of the metadata is the expected one *)
            fail_unless
              (Block_hash.equal metadata.block_hash imported_block_hash)
              (Inconsistent_imported_block
                 (imported_block_hash, metadata.block_hash))
            >>=? fun () -> return block_data
        | _ ->
            fail Inconsistent_snapshot_data
      in
      let rec second_pass batch ctxt block_header notify =
        notify ()
        >>= fun () ->
        get_command rbuf
        >>=? function
        | Node contents ->
            add_dir batch contents
            >>=? fun tree ->
            second_pass batch (I.update_context ctxt tree) block_header notify
        | Blob data ->
            add_blob batch data
            >>=? fun tree ->
            second_pass batch (I.update_context ctxt tree) block_header notify
        | Eoc {info; parents} -> (
            I.set_context ~info ~parents ctxt block_header
            >>= function
            | false -> fail Inconsistent_snapshot_data | true -> return_unit )
        | _ ->
            fail Inconsistent_snapshot_data
      in
      let check_eof () =
        get_command rbuf
        >>=? function
        | Eof -> return_unit | _ -> fail Inconsistent_snapshot_data
      in
      first_pass ()
      >>=? fun block_data ->
      let pred_block_header = I.Block_data.predecessor_header block_data in
      Lwt_utils_unix.display_progress
        ~every:1000
        ~pp_print_step:(fun fmt i ->
          Format.fprintf
            fmt
            "Writing context: %dK/%dK (%d%%) elements, %s read..."
            (i / 1_000)
            (context_elements / 1_000)
            (100 * i / context_elements)
            ( if !read > 1_048_576 then
              Format.asprintf "%dMiB" (!read / 1_048_576)
            else Format.asprintf "%dKiB" (!read / 1_024) ))
        (fun notify ->
          I.batch index (fun batch ->
              second_pass batch (I.make_context index) pred_block_header notify))
      >>=? fun () -> check_eof () >>=? fun () -> return block_data
    in
    Lwt.catch
      (fun () -> restore metadata)
      (function
        | Unix.Unix_error (e, _, _) ->
            fail @@ System_read_error (Unix.error_message e)
        | err ->
            Lwt.fail err)
end

module Make_legacy (I : Dump_interface) = struct
  let current_version = "tezos-snapshot-1.0.0"

  type command =
    | Root of {
        block_header : I.Block_header.t;
        info : I.commit_info;
        parents : I.Commit_hash.t list;
        block_data : I.Block_data.t;
      }
    | Node of (string * I.hash) list
    | Blob of string
    | Proot of I.Pruned_block.t
    | Loot of I.Protocol_data.t
    | End

  (* Command encoding. *)

  let blob_encoding =
    let open Data_encoding in
    case
      ~title:"blob"
      (Tag (Char.code 'b'))
      string
      (function Blob string -> Some string | _ -> None)
      (function string -> Blob string)

  let node_encoding =
    let open Data_encoding in
    case
      ~title:"node"
      (Tag (Char.code 'd'))
      (list (obj2 (req "name" string) (req "hash" I.hash_encoding)))
      (function Node x -> Some x | _ -> None)
      (function x -> Node x)

  let end_encoding =
    let open Data_encoding in
    case
      ~title:"end"
      (Tag (Char.code 'e'))
      empty
      (function End -> Some () | _ -> None)
      (fun () -> End)

  let loot_encoding =
    let open Data_encoding in
    case
      ~title:"loot"
      (Tag (Char.code 'l'))
      I.Protocol_data.encoding
      (function Loot protocol_data -> Some protocol_data | _ -> None)
      (fun protocol_data -> Loot protocol_data)

  let proot_encoding =
    let open Data_encoding in
    case
      ~title:"proot"
      (Tag (Char.code 'p'))
      (obj1 (req "pruned_block" I.Pruned_block.encoding))
      (function Proot pruned_block -> Some pruned_block | _ -> None)
      (fun pruned_block -> Proot pruned_block)

  let root_encoding =
    let open Data_encoding in
    case
      ~title:"root"
      (Tag (Char.code 'r'))
      (obj4
         (req "block_header" (dynamic_size I.Block_header.encoding))
         (req "info" I.commit_info_encoding)
         (req "parents" (list I.Commit_hash.encoding))
         (req "block_data" I.Block_data.encoding))
      (function
        | Root {block_header; info; parents; block_data} ->
            Some (block_header, info, parents, block_data)
        | _ ->
            None)
      (fun (block_header, info, parents, block_data) ->
        Root {block_header; info; parents; block_data})

  let command_encoding =
    Data_encoding.union
      ~tag_size:`Uint8
      [ blob_encoding;
        node_encoding;
        end_encoding;
        loot_encoding;
        proot_encoding;
        root_encoding ]

  (* IO toolkit. *)

  let rec read_string rbuf ~len =
    let (fd, buf, ofs, total) = !rbuf in
    if Bytes.length buf - ofs < len then (
      let blen = Bytes.length buf - ofs in
      let neu = Bytes.create (blen + 1_000_000) in
      Bytes.blit buf ofs neu 0 blen ;
      Lwt_unix.read fd neu blen 1_000_000
      >>= fun bread ->
      total := !total + bread ;
      if bread = 0 then fail Inconsistent_snapshot_file
      else
        let neu =
          if bread <> 1_000_000 then Bytes.sub neu 0 (blen + bread) else neu
        in
        rbuf := (fd, neu, 0, total) ;
        read_string rbuf ~len )
    else
      let res = Bytes.sub_string buf ofs len in
      rbuf := (fd, buf, ofs + len, total) ;
      return res

  let read_mbytes rbuf b =
    read_string rbuf ~len:(Bytes.length b)
    >>=? fun string ->
    Bytes.blit_string string 0 b 0 (Bytes.length b) ;
    return ()

  let get_int64 rbuf =
    read_string ~len:8 rbuf
    >>=? fun s -> return @@ EndianString.BigEndian.get_int64 s 0

  let get_mbytes rbuf =
    get_int64 rbuf >>|? Int64.to_int
    >>=? fun l ->
    let b = Bytes.create l in
    read_mbytes rbuf b >>=? fun () -> return b

  (* Getter and setters *)

  let get_command rbuf =
    get_mbytes rbuf
    >>|? fun bytes -> Data_encoding.Binary.of_bytes_exn command_encoding bytes

  (* Snapshot metadata *)

  type snapshot_metadata = {
    version : string;
    mode : Tezos_shell_services.History_mode.t;
  }

  let snapshot_metadata_encoding =
    let open Data_encoding in
    conv
      (fun {version; mode} -> (version, mode))
      (fun (version, mode) -> {version; mode})
      (obj2
         (req "version" string)
         (req "mode" Tezos_shell_services.History_mode.encoding))

  let read_snapshot_metadata rbuf =
    get_mbytes rbuf
    >>|? fun bytes ->
    Data_encoding.(Binary.of_bytes_exn snapshot_metadata_encoding) bytes

  let check_version v =
    fail_when
      (v.version <> current_version)
      (Invalid_snapshot_version (v.version, current_version))

  (* Restoring legacy *)

  let restore_context_fd index ~fd ?expected_block ~handle_block
      ~handle_protocol_data ~block_validation =
    let read = ref 0 in
    let rbuf = ref (fd, Bytes.empty, 0, read) in
    (* Editing the repository *)
    let add_blob t blob = I.add_string t blob >>= fun tree -> return tree in
    let add_dir t keys =
      I.add_dir t keys
      >>= function
      | None -> fail Restore_context_failure | Some tree -> return tree
    in
    let restore () =
      let rec first_pass ctxt notify batch =
        notify ()
        >>= fun () ->
        get_command rbuf
        >>=? function
        | Root {block_header; info; parents; block_data} -> (
            (* Checks that the block hash imported by the snapshot is the expected one *)
            let imported_block_header = I.Block_data.header block_data in
            let imported_block_hash =
              Block_header.hash imported_block_header
            in
            ( match expected_block with
            | Some str ->
                let bh = Block_hash.of_b58check_exn str in
                fail_unless
                  (Block_hash.equal bh imported_block_hash)
                  (Inconsistent_imported_block (imported_block_hash, bh))
            | None ->
                return_unit )
            >>=? fun () ->
            I.set_context ~info ~parents ctxt block_header
            >>= function
            | false ->
                fail Inconsistent_snapshot_data
            | true ->
                return (block_header, block_data) )
        | Node contents ->
            add_dir batch contents
            >>=? fun tree ->
            first_pass (I.update_context ctxt tree) notify batch
        | Blob data ->
            add_blob batch data
            >>=? fun tree ->
            first_pass (I.update_context ctxt tree) notify batch
        | _ ->
            fail Inconsistent_snapshot_data
      in
      let second_pass notify =
        let rec loop pred_header =
          get_command rbuf
          >>=? function
          | Proot pruned_block ->
              let header = I.Pruned_block.header pruned_block in
              let hash = Block_header.hash header in
              block_validation pred_header hash pruned_block
              >>=? fun () ->
              handle_block (hash, pruned_block)
              >>=? fun () -> notify () >>= fun () -> loop (Some header)
          | Loot protocol_data ->
              handle_protocol_data protocol_data
              >>=? fun () -> loop pred_header
          | End ->
              return pred_header
          | _ ->
              fail Inconsistent_snapshot_data
        in
        loop None
      in
      Lwt_utils_unix.display_progress
        ~every:1000
        ~pp_print_step:(fun fmt i ->
          Format.fprintf
            fmt
            "Writing context: %dK elements, %s read..."
            (i / 1_000)
            ( if !read > 1_048_576 then
              Format.asprintf "%dMiB" (!read / 1_048_576)
            else Format.asprintf "%dKiB" (!read / 1_024) ))
        (fun notify ->
          I.batch index (first_pass (I.make_context index) notify))
      >>=? fun (pred_block_header, export_block_data) ->
      Lwt_utils_unix.display_progress
        ~every:1000
        ~pp_print_step:(fun fmt i ->
          Format.fprintf fmt "Storing blocks: %d blocks wrote..." i)
        (fun notify -> second_pass notify)
      >>=? fun oldest_header_opt ->
      return (pred_block_header, export_block_data, oldest_header_opt)
    in
    Lwt.catch
      (fun () ->
        (* Check snapshot version *)
        read_snapshot_metadata rbuf
        >>=? fun version -> check_version version >>=? fun () -> restore ())
      (function
        | Unix.Unix_error (e, _, _) ->
            fail (System_read_error (Unix.error_message e))
        | Invalid_argument _ ->
            fail Inconsistent_snapshot_file
        | err ->
            Lwt.fail err)
end
