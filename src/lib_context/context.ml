(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2018-2020 Nomadic Labs <contact@nomadic-labs.com>           *)
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

(* Errors *)

type error +=
  | Cannot_create_file of string
  | Cannot_open_file of string
  | Cannot_find_protocol
  | Suspicious_file of int

let () =
  register_error_kind
    `Permanent
    ~id:"context_dump.write.cannot_open"
    ~title:"Cannot open file for context dump"
    ~description:""
    ~pp:(fun ppf uerr ->
      Format.fprintf
        ppf
        "@[Error while opening file for context dumping: %s@]"
        uerr)
    Data_encoding.(obj1 (req "context_dump_cannot_open" string))
    (function Cannot_create_file e -> Some e | _ -> None)
    (fun e -> Cannot_create_file e) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.read.cannot_open"
    ~title:"Cannot open file for context restoring"
    ~description:""
    ~pp:(fun ppf uerr ->
      Format.fprintf
        ppf
        "@[Error while opening file for context restoring: %s@]"
        uerr)
    Data_encoding.(obj1 (req "context_restore_cannot_open" string))
    (function Cannot_open_file e -> Some e | _ -> None)
    (fun e -> Cannot_open_file e) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.cannot_find_protocol"
    ~title:"Cannot find protocol"
    ~description:""
    ~pp:(fun ppf () ->
      Format.fprintf ppf "@[Cannot find protocol in context@]")
    Data_encoding.unit
    (function Cannot_find_protocol -> Some () | _ -> None)
    (fun () -> Cannot_find_protocol) ;
  register_error_kind
    `Permanent
    ~id:"context_dump.read.suspicious"
    ~title:"Suspicious file: data after end"
    ~description:""
    ~pp:(fun ppf uerr ->
      Format.fprintf
        ppf
        "@[Remaining bytes in file after context restoring: %d@]"
        uerr)
    Data_encoding.(obj1 (req "context_restore_suspicious" int31))
    (function Suspicious_file e -> Some e | _ -> None)
    (fun e -> Suspicious_file e)

(** Tezos - Versioned (key x value) store (over Irmin) *)

module Path = Irmin.Path.String_list
module Metadata = Irmin.Metadata.None

let reporter () =
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let with_stamp h _tags k fmt =
      let dt = Mtime.Span.to_us (Mtime_clock.elapsed ()) in
      Fmt.kpf
        k
        Fmt.stderr
        ("%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
        dt
        Fmt.(styled `Magenta string)
        (Logs.Src.name src)
        Logs_fmt.pp_header
        (level, h)
    in
    msgf @@ fun ?header ?tags fmt -> with_stamp header tags k fmt
  in
  {Logs.report}

let index_log_size = ref None

let overcommit = ref true

let () =
  let verbose_app () =
    Logs.set_level (Some Logs.App) ;
    Logs.set_reporter (reporter ())
  in
  let verbose_debug () =
    Logs.set_level (Some Logs.Debug) ;
    Logs.set_reporter (reporter ())
  in
  (* while testing the layered store, print stats and logs from irmin *)
  verbose_app () ;
  let index_log_size n = index_log_size := Some (int_of_string n) in
  let overcommit () = overcommit := true in
  match Unix.getenv "TEZOS_STORAGE" with
  | exception Not_found ->
      ()
  | v ->
      let args = String.split ',' v in
      List.iter
        (function
          | "v" | "verbose" ->
              verbose_app ()
          | "vv" ->
              verbose_debug ()
          | "overcommit" ->
              overcommit ()
          | v -> (
            match String.split '=' v with
            | ["index-log-size"; n] ->
                index_log_size n
            | _ ->
                () ))
        args

module Hash : sig
  include Irmin.Hash.S

  val to_context_hash : t -> Context_hash.t

  val of_context_hash : Context_hash.t -> t
end = struct
  module H = Digestif.Make_BLAKE2B (struct
    let digest_size = 32
  end)

  type t = H.t

  let of_context_hash s = H.of_raw_string (Context_hash.to_string s)

  let to_context_hash h = Context_hash.of_string_exn (H.to_raw_string h)

  let pp ppf t = Context_hash.pp ppf (to_context_hash t)

  let of_string x =
    match Context_hash.of_b58check x with
    | Ok x ->
        Ok (of_context_hash x)
    | Error err ->
        Error
          (`Msg
            (Format.asprintf
               "Failed to read b58check_encoding data: %a"
               Error_monad.pp_print_error
               err))

  let hash_key = Irmin.Type.(unstage (short_hash string))

  let short_hash t = hash_key (H.to_raw_string t)

  let staged_short_hash =
    Irmin.Type.(stage @@ fun ?seed t -> hash_key ?seed (H.to_raw_string t))

  let t : t Irmin.Type.t =
    Irmin.Type.map
      ~pp
      ~of_string
      Irmin.Type.(string_of (`Fixed H.digest_size))
      ~short_hash:staged_short_hash
      H.of_raw_string
      H.to_raw_string

  let hash_size = H.digest_size

  let hash = H.digesti_string
end

module Node = struct
  module M = Irmin.Private.Node.Make (Hash) (Path) (Metadata)

  module V1 = struct
    module Hash = Irmin.Hash.V1 (Hash)

    type kind = [`Node | `Contents of Metadata.t]

    type entry = {kind : kind; name : M.step; node : Hash.t}

    let s = Irmin.Type.(string_of `Int64)

    let pre_hash_v = Irmin.Type.(unstage (pre_hash s))

    (* Irmin 1.4 uses int64 to store string lengths *)
    let step_t =
      let pre_hash = Irmin.Type.(stage @@ fun x -> pre_hash_v x) in
      Irmin.Type.like M.step_t ~pre_hash

    let metadata_t =
      let some = "\255\000\000\000\000\000\000\000" in
      let none = "\000\000\000\000\000\000\000\000" in
      Irmin.Type.(map (string_of (`Fixed 8)))
        (fun s ->
          match s.[0] with
          | '\255' ->
              None
          | '\000' ->
              Some ()
          | _ ->
              assert false)
        (function Some _ -> some | None -> none)

    (* Irmin 1.4 uses int64 to store list lengths *)
    let entry_t : entry Irmin.Type.t =
      let open Irmin.Type in
      record "Tree.entry" (fun kind name node ->
          let kind = match kind with None -> `Node | Some m -> `Contents m in
          {kind; name; node})
      |+ field "kind" metadata_t (function
             | {kind = `Node; _} ->
                 None
             | {kind = `Contents m; _} ->
                 Some m)
      |+ field "name" step_t (fun {name; _} -> name)
      |+ field "node" Hash.t (fun {node; _} -> node)
      |> sealr

    let entries_t : entry list Irmin.Type.t =
      Irmin.Type.(list ~len:`Int64 entry_t)

    let import_entry (s, v) =
      match v with
      | `Node h ->
          {name = s; kind = `Node; node = h}
      | `Contents (h, m) ->
          {name = s; kind = `Contents m; node = h}

    let import t = List.map import_entry (M.list t)

    let pre_hash_entries = Irmin.Type.(unstage (pre_hash entries_t))

    let pre_hash entries = pre_hash_entries entries
  end

  include M

  let pre_hash_v1 x = V1.pre_hash (V1.import x)

  let t = Irmin.Type.(like t ~pre_hash:(stage @@ fun x -> pre_hash_v1 x))
end

module Commit = struct
  module M = Irmin.Private.Commit.Make (Hash)
  module V1 = Irmin.Private.Commit.V1 (M)
  include M

  let pre_hash_v1_t = Irmin.Type.(unstage (pre_hash V1.t))

  let pre_hash_v1 t = pre_hash_v1_t (V1.import t)

  let t = Irmin.Type.(like t ~pre_hash:(stage @@ fun x -> pre_hash_v1 x))
end

module Contents = struct
  type t = string

  let ty = Irmin.Type.(pair (string_of `Int64) unit)

  let pre_hash_ty = Irmin.Type.(unstage (pre_hash ty))

  let pre_hash_v1 x = pre_hash_ty (x, ())

  let t = Irmin.Type.(like string ~pre_hash:(stage @@ fun x -> pre_hash_v1 x))

  let merge = Irmin.Merge.(idempotent (Irmin.Type.option t))
end

module Conf = struct
  let entries = 32

  let stable_hash = 256
end

module Store =
  Irmin_pack.Layered.Make_ext (Conf) (Irmin.Metadata.None) (Contents)
    (Irmin.Path.String_list)
    (Irmin.Branch.String)
    (Hash)
    (Node)
    (Commit)
module P = Store.Private

module Checks = struct
  module Pack =
    Irmin_pack.Checks.Make (Conf) (Irmin.Metadata.None) (Contents)
      (Irmin.Path.String_list)
      (Irmin.Branch.String)
      (Hash)
      (Node)
      (Commit)

  module Index = struct
    module I = Irmin_pack.Index.Make (Hash)
    include I.Checks
  end
end

type index = {
  path : string;
  repo : Store.Repo.t;
  patch_context : (context -> context tzresult Lwt.t) option;
  readonly : bool;
}

and context = {index : index; parents : Store.Commit.t list; tree : Store.tree}

type t = context

(*-- Version Access and Update -----------------------------------------------*)

let current_protocol_key = ["protocol"]

let current_test_chain_key = ["test_chain"]

let current_data_key = ["data"]

let restore_integrity ?ppf index =
  let l = Store.integrity_check ?ppf ~auto_repair:true index.repo in
  let rec report expected = function
    | Error (`Cannot_fix msg, layer) :: _ when expected = `Cannot_fix ->
        error (failure "%s in layer %a" msg Irmin_layers.Layer_id.pp layer)
    | Error (`Corrupted n, layer) :: _ when expected = `Corrupted ->
        error
          (failure
             "unable to fix the corrupted context: %d bad entries detected in \
              layer %a"
             n
             Irmin_layers.Layer_id.pp
             layer)
    | Ok (`Fixed n) :: _ when expected = `Fixed ->
        Ok (Some n)
    | _ :: tl ->
        report expected tl
    | [] ->
        raise Not_found
  in
  try report `Cannot_fix l
  with Not_found -> (
    try report `Corrupted l
    with Not_found -> ( try report `Fixed l with Not_found -> Ok None ) )

let syncs index = Store.sync index.repo

let exists index key =
  if index.readonly then syncs index ;
  Store.Commit.of_hash index.repo (Hash.of_context_hash key)
  >|= function None -> false | Some _ -> true

let checkout index key =
  if index.readonly then syncs index ;
  Store.Commit.of_hash index.repo (Hash.of_context_hash key)
  >>= function
  | None ->
      Lwt.return_none
  | Some commit ->
      let tree = Store.Commit.tree commit in
      let ctxt = {index; tree; parents = [commit]} in
      Lwt.return_some ctxt

let checkout_exn index key =
  checkout index key
  >>= function None -> Lwt.fail Not_found | Some p -> Lwt.return p

(* unshallow possible 1-st level objects from previous partial
   checkouts ; might be better to pass directly the list of shallow
   objects. *)
let unshallow context =
  Store.Tree.list context.tree []
  >>= fun children ->
  P.Repo.batch context.index.repo (fun x y _ ->
      Lwt_list.iter_s
        (fun (s, k) ->
          match k with
          | `Contents ->
              Lwt.return ()
          | `Node ->
              Store.Tree.get_tree context.tree [s]
              >>= fun tree ->
              Store.save_tree ~clear:true context.index.repo x y tree
              >|= fun _ -> ())
        children)

let total = ref 0

let pp_commit_stats h =
  let num_objects = Irmin_layers.Stats.get_adds () in
  total := !total + num_objects ;
  Irmin_layers.Stats.reset_adds () ;
  Format.printf
    "Irmin stats: Objects created by commit %a = %d \n@."
    Store.Commit.pp_hash
    h
    num_objects

let pp_stats () =
  let stats = Irmin_layers.Stats.get () in
  let pp_comma ppf () = Fmt.pf ppf "," in
  let copied_objects =
    List.map2 (fun x y -> x + y) stats.copied_contents stats.copied_commits
    |> List.map2 (fun x y -> x + y) stats.copied_nodes
    |> List.map2 (fun x y -> x + y) stats.copied_branches
  in
  Format.printf
    "%a Irmin stats: nb_freeze = %d copied_objects = %a waiting_freeze  = %a \
     completed_freeze = %a \n\
    \  objects added in upper since last freeze = %d \n\
     @."
    Time.System.pp_hum
    (Systime_os.now ())
    stats.nb_freeze
    Fmt.(list ~sep:pp_comma int)
    copied_objects
    Fmt.(list ~sep:pp_comma float)
    stats.waiting_freeze
    Fmt.(list ~sep:pp_comma float)
    stats.completed_freeze
    !total ;
  total := 0

let raw_commit ~time ?(message = "") context =
  let info =
    Irmin.Info.v ~date:(Time.Protocol.to_seconds time) ~author:"Tezos" message
  in
  let parents = List.map Store.Commit.hash context.parents in
  unshallow context
  >>= fun () ->
  Store.Commit.v context.index.repo ~info ~parents context.tree
  >|= fun h ->
  pp_commit_stats h ;
  Store.Tree.clear context.tree ;
  h

let freeze ?recovery ~max ~heads index =
  (* TODO: allow to drop lower *)
  if index.readonly then syncs index ;
  let to_commit ctxt_hash =
    let hash = Hash.of_context_hash ctxt_hash in
    Store.Commit.of_hash index.repo hash
    >>= function None -> assert false | Some commit -> Lwt.return commit
  in
  to_commit max
  >>= fun max ->
  Lwt_list.map_s to_commit heads
  >>= fun heads ->
  pp_stats () ;
  Store.freeze ?recovery ~min_upper:[max] ~max:heads index.repo

let hash ~time ?(message = "") context =
  let info =
    Irmin.Info.v ~date:(Time.Protocol.to_seconds time) ~author:"Tezos" message
  in
  let parents = List.map (fun c -> Store.Commit.hash c) context.parents in
  let node = Store.Tree.hash context.tree in
  let commit = P.Commit.Val.v ~parents ~node ~info in
  let x = P.Commit.Key.hash commit in
  Hash.to_context_hash x

let commit ~time ?message context =
  raw_commit ~time ?message context
  >|= fun commit -> Hash.to_context_hash (Store.Commit.hash commit)

(*-- Generic Store Primitives ------------------------------------------------*)

let data_key key = current_data_key @ key

type key = string list

type value = bytes

let mem ctxt key =
  Store.Tree.mem ctxt.tree (data_key key) >>= fun v -> Lwt.return v

let dir_mem ctxt key =
  Store.Tree.mem_tree ctxt.tree (data_key key) >>= fun v -> Lwt.return v

let raw_get ctxt key =
  Store.Tree.find ctxt.tree key >|= Option.map Bytes.of_string

let get t key = raw_get t (data_key key)

let raw_set ctxt key data =
  let data = Bytes.to_string data in
  Store.Tree.add ctxt.tree key data >>= fun tree -> Lwt.return {ctxt with tree}

let set t key data = raw_set t (data_key key) data

let raw_del ctxt key =
  Store.Tree.remove ctxt.tree key >>= fun tree -> Lwt.return {ctxt with tree}

let remove_rec ctxt key =
  Store.Tree.remove ctxt.tree (data_key key)
  >>= fun tree -> Lwt.return {ctxt with tree}

let copy ctxt ~from ~to_ =
  Store.Tree.find_tree ctxt.tree (data_key from)
  >>= function
  | None ->
      Lwt.return_none
  | Some sub_tree ->
      Store.Tree.add_tree ctxt.tree (data_key to_) sub_tree
      >>= fun tree -> Lwt.return_some {ctxt with tree}

let fold ctxt key ~init ~f =
  Store.Tree.list ctxt.tree (data_key key)
  >>= fun keys ->
  Lwt_list.fold_left_s
    (fun acc (name, kind) ->
      let key =
        match kind with
        | `Contents ->
            `Key (key @ [name])
        | `Node ->
            `Dir (key @ [name])
      in
      f key acc)
    init
    keys

(*-- Predefined Fields -------------------------------------------------------*)

let get_protocol v =
  raw_get v current_protocol_key
  >>= function
  | None ->
      fail Cannot_find_protocol
  | Some data -> (
    match Protocol_hash.of_bytes data with
    | Ok x ->
        return x
    | Error e ->
        Lwt.return (Error e) )

let get_protocol_exn v =
  get_protocol v
  >>= function
  | Ok x ->
      Lwt.return x
  | Error _ ->
      Lwt.fail (Failure "Unexpected error (Context.get_protocol_exn)")

let set_protocol v key =
  let key = Protocol_hash.to_bytes key in
  raw_set v current_protocol_key key

let get_test_chain v =
  raw_get v current_test_chain_key
  >>= function
  | None ->
      Lwt.fail (Failure "Unexpected error (Context.get_test_chain)")
  | Some data -> (
    match Data_encoding.Binary.of_bytes Test_chain_status.encoding data with
    | Error re ->
        Format.kasprintf
          (fun s -> Lwt.fail (Failure s))
          "Error in Context.get_test_chain: %a"
          Data_encoding.Binary.pp_read_error
          re
    | Ok r ->
        Lwt.return r )

let set_test_chain v id =
  let id = Data_encoding.Binary.to_bytes_exn Test_chain_status.encoding id in
  raw_set v current_test_chain_key id

let del_test_chain v = raw_del v current_test_chain_key

let fork_test_chain v ~protocol ~expiration =
  set_test_chain v (Forking {protocol; expiration})

(*-- Initialisation ----------------------------------------------------------*)

let config ?readonly root =
  let index_throttle =
    if !overcommit then `Overcommit_memory else `Block_writes
  in
  let conf =
    Irmin_pack.config
      ?readonly
      ?index_log_size:!index_log_size
      ~index_throttle
      root
  in
  Irmin_pack.config_layers
    ~conf
    ~copy_in_upper:true
    ~with_lower:false
    ~blocking_copy_size:8000
    ()

let init ?patch_context ?(readonly = false) root =
  let config = config ~readonly root in
  let open_store () =
    Store.Repo.v config
    >>= fun repo ->
    let v = {path = root; repo; patch_context; readonly} in
    if Store.needs_recovery repo then
      Format.printf
        "Node aborted during a freeze; set recovery flag to true at next call \
         to freeze." ;
    Lwt.return v
  in
  Lwt.catch open_store (function
      | Irmin_pack.Unsupported_version `V1 ->
          Logs.app (fun l -> l "Migrating store to v2, this may take a while") ;
          Store.migrate config ;
          Logs.app (fun l -> l "Migration ended, opening the store") ;
          open_store ()
      | exn ->
          Lwt.fail exn)

let close index = Store.Repo.close index.repo

let get_branch chain_id = Format.asprintf "%a" Chain_id.pp chain_id

let commit_genesis index ~chain_id ~time ~protocol =
  let tree = Store.Tree.empty in
  let ctxt = {index; tree; parents = []} in
  ( match index.patch_context with
  | None ->
      return ctxt
  | Some patch_context ->
      patch_context ctxt )
  >>=? fun ctxt ->
  set_protocol ctxt protocol
  >>= fun ctxt ->
  set_test_chain ctxt Not_running
  >>= fun ctxt ->
  raw_commit ~time ~message:"Genesis" ctxt
  >>= fun commit ->
  Store.Branch.set index.repo (get_branch chain_id) commit
  >>= fun () -> return (Hash.to_context_hash (Store.Commit.hash commit))

let compute_testchain_chain_id genesis =
  let genesis_hash = Block_hash.hash_bytes [Block_hash.to_bytes genesis] in
  Chain_id.of_block_hash genesis_hash

let compute_testchain_genesis forked_block =
  let genesis = Block_hash.hash_bytes [Block_hash.to_bytes forked_block] in
  genesis

let commit_test_chain_genesis ctxt (forked_header : Block_header.t) =
  let message =
    Format.asprintf "Forking testchain at level %ld." forked_header.shell.level
  in
  raw_commit ~time:forked_header.shell.timestamp ~message ctxt
  >>= fun commit ->
  let faked_shell_header : Block_header.shell_header =
    {
      forked_header.shell with
      proto_level = succ forked_header.shell.proto_level;
      predecessor = Block_hash.zero;
      validation_passes = 0;
      operations_hash = Operation_list_list_hash.empty;
      context = Hash.to_context_hash (Store.Commit.hash commit);
    }
  in
  let forked_block = Block_header.hash forked_header in
  let genesis_hash = compute_testchain_genesis forked_block in
  let chain_id = compute_testchain_chain_id genesis_hash in
  let genesis_header : Block_header.t =
    {
      shell = {faked_shell_header with predecessor = genesis_hash};
      protocol_data = Bytes.create 0;
    }
  in
  let branch = get_branch chain_id in
  Store.Branch.set ctxt.index.repo branch commit
  >>= fun () -> Lwt.return genesis_header

let clear_test_chain index chain_id =
  (* TODO remove commits... ??? *)
  let branch = get_branch chain_id in
  Store.Branch.remove index.repo branch

let set_head index chain_id commit =
  let branch = get_branch chain_id in
  Store.Commit.of_hash index.repo (Hash.of_context_hash commit)
  >>= function
  | None ->
      assert false
  | Some commit ->
      Store.Branch.set index.repo branch commit

let set_master index commit =
  Store.Commit.of_hash index.repo (Hash.of_context_hash commit)
  >>= function
  | None ->
      assert false
  | Some commit ->
      Store.Branch.set index.repo Store.Branch.master commit

(* Context dumping *)

module Pruned_block = struct
  type t = {
    block_header : Block_header.t;
    operations : (int * Operation.t list) list;
    operation_hashes : (int * Operation_hash.t list) list;
  }

  let encoding =
    let open Data_encoding in
    conv
      (fun {block_header; operations; operation_hashes} ->
        (operations, operation_hashes, block_header))
      (fun (operations, operation_hashes, block_header) ->
        {block_header; operations; operation_hashes})
      (obj3
         (req
            "operations"
            (list (tup2 int31 (list (dynamic_size Operation.encoding)))))
         (req
            "operation_hashes"
            (list (tup2 int31 (list (dynamic_size Operation_hash.encoding)))))
         (req "block_header" Block_header.encoding))

  let to_bytes pruned_block =
    Data_encoding.Binary.to_bytes_exn encoding pruned_block

  let of_bytes pruned_block =
    Data_encoding.Binary.of_bytes_opt encoding pruned_block

  let header {block_header; _} = block_header
end

module Block_data = struct
  type t = {
    block_header : Block_header.t;
    operations : Operation.t list list;
    predecessor_header : Block_header.t;
  }

  let header {block_header; _} = block_header

  let predecessor_header {predecessor_header; _} = predecessor_header

  let encoding =
    let open Data_encoding in
    conv
      (fun {block_header; operations; predecessor_header} ->
        (operations, block_header, predecessor_header))
      (fun (operations, block_header, predecessor_header) ->
        {block_header; operations; predecessor_header})
      (obj3
         (req "operations" (list (list (dynamic_size Operation.encoding))))
         (req "block_header" (dynamic_size Block_header.encoding))
         (req "predecessor_header" Block_header.encoding))

  let to_bytes = Data_encoding.Binary.to_bytes_exn encoding

  let of_bytes = Data_encoding.Binary.of_bytes_opt encoding
end

module Protocol_data = struct
  type info = {author : string; message : string; timestamp : Time.Protocol.t}

  let info_encoding =
    let open Data_encoding in
    conv
      (fun {author; message; timestamp} -> (author, message, timestamp))
      (fun (author, message, timestamp) -> {author; message; timestamp})
      (obj3
         (req "author" string)
         (req "message" string)
         (req "timestamp" Time.Protocol.encoding))

  type data = {
    info : info;
    protocol_hash : Protocol_hash.t;
    test_chain_status : Test_chain_status.t;
    data_key : Context_hash.t;
    parents : Context_hash.t list;
    protocol : Protocol.t;
  }

  let data_encoding =
    let open Data_encoding in
    conv
      (fun {info; protocol_hash; test_chain_status; data_key; parents; protocol}
           ->
        (info, protocol_hash, test_chain_status, data_key, parents, protocol))
      (fun (info, protocol_hash, test_chain_status, data_key, parents, protocol)
           ->
        {info; protocol_hash; test_chain_status; data_key; parents; protocol})
      (obj6
         (req "info" info_encoding)
         (req "protocol_hash" Protocol_hash.encoding)
         (req "test_chain_status" Test_chain_status.encoding)
         (req "data_key" Context_hash.encoding)
         (req "parents" (list Context_hash.encoding))
         (req "protocol" Protocol.encoding))

  type t = Int32.t * data

  let encoding =
    let open Data_encoding in
    tup2 int32 data_encoding

  let to_bytes = Data_encoding.Binary.to_bytes_exn encoding

  let of_bytes = Data_encoding.Binary.of_bytes_opt encoding
end

module Protocol_data_legacy = struct
  type info = {author : string; message : string; timestamp : Time.Protocol.t}

  let info_encoding =
    let open Data_encoding in
    conv
      (fun {author; message; timestamp} -> (author, message, timestamp))
      (fun (author, message, timestamp) -> {author; message; timestamp})
      (obj3
         (req "author" string)
         (req "message" string)
         (req "timestamp" Time.Protocol.encoding))

  type data = {
    info : info;
    protocol_hash : Protocol_hash.t;
    test_chain_status : Test_chain_status.t;
    data_key : Context_hash.t;
    parents : Context_hash.t list;
  }

  let data_encoding =
    let open Data_encoding in
    conv
      (fun {info; protocol_hash; test_chain_status; data_key; parents} ->
        (info, protocol_hash, test_chain_status, data_key, parents))
      (fun (info, protocol_hash, test_chain_status, data_key, parents) ->
        {info; protocol_hash; test_chain_status; data_key; parents})
      (obj5
         (req "info" info_encoding)
         (req "protocol_hash" Protocol_hash.encoding)
         (req "test_chain_status" Test_chain_status.encoding)
         (req "data_key" Context_hash.encoding)
         (req "parents" (list Context_hash.encoding)))

  type t = Int32.t * data

  let encoding =
    let open Data_encoding in
    tup2 int32 data_encoding

  let to_bytes = Data_encoding.Binary.to_bytes_exn encoding

  let of_bytes = Data_encoding.Binary.of_bytes_opt encoding
end

module Block_data_legacy = struct
  type t = {block_header : Block_header.t; operations : Operation.t list list}

  let header {block_header; _} = block_header

  let encoding =
    let open Data_encoding in
    conv
      (fun {block_header; operations} -> (operations, block_header))
      (fun (operations, block_header) -> {block_header; operations})
      (obj2
         (req "operations" (list (list (dynamic_size Operation.encoding))))
         (req "block_header" Block_header.encoding))

  let to_bytes = Data_encoding.Binary.to_bytes_exn encoding

  let of_bytes = Data_encoding.Binary.of_bytes_opt encoding
end

module Dumpable_context = struct
  type nonrec index = index

  type nonrec context = context

  type tree = Store.tree

  type hash = [`Blob of Store.hash | `Node of Store.hash]

  type commit_info = Irmin.Info.t

  type batch =
    | Batch of
        Store.repo * [`Read | `Write] P.Contents.t * [`Read | `Write] P.Node.t

  let batch index f =
    P.Repo.batch index.repo (fun x y _ -> f (Batch (index.repo, x, y)))

  let commit_info_encoding =
    let open Data_encoding in
    conv
      (fun irmin_info ->
        let author = Irmin.Info.author irmin_info in
        let message = Irmin.Info.message irmin_info in
        let date = Irmin.Info.date irmin_info in
        (author, message, date))
      (fun (author, message, date) -> Irmin.Info.v ~author ~date message)
      (obj3 (req "author" string) (req "message" string) (req "date" int64))

  let hash_encoding : hash Data_encoding.t =
    let open Data_encoding in
    let kind_encoding = string_enum [("node", `Node); ("blob", `Blob)] in
    conv
      (function
        | `Blob h ->
            (`Blob, Context_hash.to_bytes (Hash.to_context_hash h))
        | `Node h ->
            (`Node, Context_hash.to_bytes (Hash.to_context_hash h)))
      (function
        | (`Blob, h) ->
            `Blob (Hash.of_context_hash (Context_hash.of_bytes_exn h))
        | (`Node, h) ->
            `Node (Hash.of_context_hash (Context_hash.of_bytes_exn h)))
      (obj2 (req "kind" kind_encoding) (req "value" bytes))

  let hash_equal (h1 : hash) (h2 : hash) = h1 = h2

  let context_parents ctxt =
    match ctxt with
    | {parents = [commit]; _} ->
        let parents = Store.Commit.parents commit in
        let parents = List.map Hash.to_context_hash parents in
        List.sort Context_hash.compare parents
    | _ ->
        assert false

  let context_info = function
    | {parents = [c]; _} ->
        Store.Commit.info c
    | _ ->
        assert false

  let get_context idx bh = checkout idx bh.Block_header.shell.context

  let set_context ~info ~parents ctxt bh =
    let parents = List.sort Context_hash.compare parents in
    let parents = List.map Hash.of_context_hash parents in
    Store.Commit.v ctxt.index.repo ~info ~parents ctxt.tree
    >>= fun c ->
    let h = Store.Commit.hash c in
    let are_context_equal =
      Context_hash.equal bh.Block_header.shell.context (Hash.to_context_hash h)
    in
    Lwt.return are_context_equal

  let context_tree ctxt = ctxt.tree

  let tree_hash tree =
    tree |> Store.Tree.destruct
    |> function
    | `Node _ ->
        `Node (Store.Tree.hash tree)
    | `Contents (b, _) ->
        `Blob (Store.Contents.hash b)

  type binding = {
    key : string;
    value : tree;
    value_kind : [`Node | `Contents];
    value_hash : hash;
  }

  (** Unpack the bindings in a tree node (in lexicographic order) and clear its
      internal cache. *)
  let bindings tree : binding list Lwt.t =
    Store.Tree.list tree []
    >>= fun keys ->
    keys
    |> List.sort (fun (a, _) (b, _) -> String.compare a b)
    |> Lwt_list.map_s (fun (key, value_kind) ->
           Store.Tree.get_tree tree [key]
           >|= fun value ->
           let value_hash = tree_hash value in
           {key; value; value_kind; value_hash})
    >|= fun bindings -> Store.Tree.clear tree ; bindings

  module Hashtbl = Hashtbl.MakeSeeded (struct
    type t = hash

    let hash = Hashtbl.seeded_hash

    let equal = hash_equal
  end)

  let tree_iteri_unique f tree =
    let total_visited = ref 0 in
    (* Noting the visited hashes *)
    let visited_hash = Hashtbl.create 1000 in
    let visited h = Hashtbl.mem visited_hash h in
    let set_visit h =
      incr total_visited ;
      Hashtbl.add visited_hash h ()
    in
    let rec aux : type a. tree -> (unit -> a) -> a Lwt.t =
     fun tree k ->
      bindings tree
      >>= Lwt_list.map_s (fun {key; value; value_hash; value_kind} ->
              let kv = (key, value_hash) in
              if visited value_hash then Lwt.return kv
              else
                match value_kind with
                | `Node ->
                    (* Visit children first, in left-to-right order. *)
                    (aux [@ocaml.tailcall]) value (fun () ->
                        (* There cannot be a cycle. *)
                        set_visit value_hash ; kv)
                | `Contents ->
                    Store.Tree.get value []
                    >>= fun data ->
                    f (`Leaf data) >|= fun () -> set_visit value_hash ; kv)
      >>= fun sub_keys -> f (`Branch sub_keys) >|= k
    in
    aux tree Fun.id >>= fun () -> Lwt.return !total_visited

  let make_context index = {index; tree = Store.Tree.empty; parents = []}

  let update_context context tree = {context with tree}

  let add_blob_hash (Batch (repo, _, _)) tree key hash =
    Store.Contents.of_hash repo hash
    >>= function
    | None ->
        Lwt.return_none
    | Some v ->
        Store.Tree.add tree key v >>= Lwt.return_some

  let add_node_hash (Batch (repo, _, _)) tree key hash =
    Store.Tree.of_hash repo hash
    >>= function
    | None ->
        Lwt.return_none
    | Some t ->
        Store.Tree.add_tree tree key (t :> tree) >>= Lwt.return_some

  let add_string (Batch (_, t, _)) string =
    (* Save the contents in the store *)
    Store.save_contents t string >|= fun _ -> Store.Tree.of_contents string

  let add_dir batch l =
    let rec fold_list sub_tree = function
      | [] ->
          Lwt.return_some sub_tree
      | (step, hash) :: tl -> (
        match hash with
        | `Blob hash -> (
            add_blob_hash batch sub_tree [step] hash
            >>= function
            | None -> Lwt.return_none | Some sub_tree -> fold_list sub_tree tl
            )
        | `Node hash -> (
            add_node_hash batch sub_tree [step] hash
            >>= function
            | None -> Lwt.return_none | Some sub_tree -> fold_list sub_tree tl
            ) )
    in
    fold_list Store.Tree.empty l
    >>= function
    | None ->
        Lwt.return_none
    | Some tree ->
        let (Batch (repo, x, y)) = batch in
        (* Save the node in the store ... *)
        Store.save_tree ~clear:true repo x y tree >|= fun _ -> Some tree

  module Commit_hash = Context_hash
  module Block_header = Block_header
  module Block_data = Block_data
  module Pruned_block = Pruned_block
  module Protocol_data = Protocol_data_legacy
end

module Dumpable_context_legacy = struct
  type nonrec index = index

  type nonrec context = context

  type tree = Store.tree

  type hash = [`Blob of Store.hash | `Node of Store.hash]

  type commit_info = Irmin.Info.t

  type batch =
    | Batch of
        Store.repo * [`Read | `Write] P.Contents.t * [`Read | `Write] P.Node.t

  let batch index f =
    P.Repo.batch index.repo (fun x y _ -> f (Batch (index.repo, x, y)))

  let commit_info_encoding =
    let open Data_encoding in
    conv
      (fun irmin_info ->
        let author = Irmin.Info.author irmin_info in
        let message = Irmin.Info.message irmin_info in
        let date = Irmin.Info.date irmin_info in
        (author, message, date))
      (fun (author, message, date) -> Irmin.Info.v ~author ~date message)
      (obj3 (req "author" string) (req "message" string) (req "date" int64))

  let hash_encoding : hash Data_encoding.t =
    let open Data_encoding in
    let kind_encoding = string_enum [("node", `Node); ("blob", `Blob)] in
    conv
      (function
        | `Blob h ->
            (`Blob, Context_hash.to_bytes (Hash.to_context_hash h))
        | `Node h ->
            (`Node, Context_hash.to_bytes (Hash.to_context_hash h)))
      (function
        | (`Blob, h) ->
            `Blob (Hash.of_context_hash (Context_hash.of_bytes_exn h))
        | (`Node, h) ->
            `Node (Hash.of_context_hash (Context_hash.of_bytes_exn h)))
      (obj2 (req "kind" kind_encoding) (req "value" bytes))

  let hash_equal (h1 : hash) (h2 : hash) = h1 = h2

  let context_parents ctxt =
    match ctxt with
    | {parents = [commit]; _} ->
        let parents = Store.Commit.parents commit in
        let parents = List.map Hash.to_context_hash parents in
        List.sort Context_hash.compare parents
    | _ ->
        assert false

  let context_info = function
    | {parents = [c]; _} ->
        Store.Commit.info c
    | _ ->
        assert false

  let get_context idx bh = checkout idx bh.Block_header.shell.context

  let set_context ~info ~parents ctxt bh =
    let parents = List.sort Context_hash.compare parents in
    let parents = List.map Hash.of_context_hash parents in
    Store.Commit.v ctxt.index.repo ~info ~parents ctxt.tree
    >>= fun c ->
    let h = Store.Commit.hash c in
    let are_context_equal =
      Context_hash.equal bh.Block_header.shell.context (Hash.to_context_hash h)
    in
    Lwt.return are_context_equal

  let context_tree ctxt = ctxt.tree

  let tree_hash tree =
    tree |> Store.Tree.destruct
    |> function
    | `Node _ ->
        `Node (Store.Tree.hash tree)
    | `Contents (b, _) ->
        `Blob (Store.Contents.hash b)

  type binding = {
    key : string;
    value : tree;
    value_kind : [`Node | `Contents];
    value_hash : hash;
  }

  (** Unpack the bindings in a tree node (in lexicographic order) and clear its
      internal cache. *)
  let bindings tree : binding list Lwt.t =
    Store.Tree.list tree []
    >>= fun keys ->
    keys
    |> List.sort (fun (a, _) (b, _) -> String.compare a b)
    |> Lwt_list.map_s (fun (key, value_kind) ->
           Store.Tree.get_tree tree [key]
           >|= fun value ->
           let value_hash = tree_hash value in
           {key; value; value_kind; value_hash})

  module Hashtbl = Hashtbl.MakeSeeded (struct
    type t = hash

    let hash = Hashtbl.seeded_hash

    let equal = hash_equal
  end)

  let tree_iteri_unique f tree =
    let total_visited = ref 0 in
    (* Noting the visited hashes *)
    let visited_hash = Hashtbl.create 1000 in
    let visited h = Hashtbl.mem visited_hash h in
    let set_visit h =
      incr total_visited ;
      Hashtbl.add visited_hash h ()
    in
    let rec aux : type a. tree -> (unit -> a) -> a Lwt.t =
     fun tree k ->
      bindings tree
      >>= Lwt_list.map_s (fun {key; value; value_hash; value_kind} ->
              let kv = (key, value_hash) in
              if visited value_hash then Lwt.return kv
              else
                match value_kind with
                | `Node ->
                    (* Visit children first, in left-to-right order. *)
                    (aux [@ocaml.tailcall]) value (fun () ->
                        (* There cannot be a cycle. *)
                        set_visit value_hash ; kv)
                | `Contents ->
                    Store.Tree.get value []
                    >>= fun data ->
                    f !total_visited (`Leaf data)
                    >|= fun () -> set_visit value_hash ; kv)
      >>= fun sub_keys -> f !total_visited (`Branch sub_keys) >|= k
    in
    aux tree Fun.id

  let make_context index = {index; tree = Store.Tree.empty; parents = []}

  let update_context context tree = {context with tree}

  let add_blob_hash (Batch (repo, _, _)) tree key hash =
    Store.Contents.of_hash repo hash
    >>= function
    | None ->
        Lwt.return_none
    | Some v ->
        Store.Tree.add tree key v >>= Lwt.return_some

  let add_node_hash (Batch (repo, _, _)) tree key hash =
    Store.Tree.of_hash repo hash
    >>= function
    | None ->
        Lwt.return_none
    | Some t ->
        Store.Tree.add_tree tree key (t :> tree) >>= Lwt.return_some

  let add_string (Batch (_, t, _)) string =
    (* Save the contents in the store *)
    Store.save_contents t string >|= fun _ -> Store.Tree.of_contents string

  let add_dir batch l =
    let rec fold_list sub_tree = function
      | [] ->
          Lwt.return_some sub_tree
      | (step, hash) :: tl -> (
        match hash with
        | `Blob hash -> (
            add_blob_hash batch sub_tree [step] hash
            >>= function
            | None -> Lwt.return_none | Some sub_tree -> fold_list sub_tree tl
            )
        | `Node hash -> (
            add_node_hash batch sub_tree [step] hash
            >>= function
            | None -> Lwt.return_none | Some sub_tree -> fold_list sub_tree tl
            ) )
    in
    fold_list Store.Tree.empty l
    >>= function
    | None ->
        Lwt.return_none
    | Some tree ->
        let (Batch (repo, x, y)) = batch in
        (* Save the node in the store ... *)
        Store.save_tree ~clear:true repo x y tree >|= fun _ -> Some tree

  module Commit_hash = Context_hash
  module Block_header = Block_header
  module Block_data = Block_data_legacy
  module Pruned_block = Pruned_block
  module Protocol_data = Protocol_data_legacy
end

(* Protocol data *)

let data_merkle_root_hash context =
  Store.Tree.get_tree context.tree current_data_key
  >|= fun tree -> Hash.to_context_hash (Store.Tree.hash tree)

let retrieve_commit_info index block_header =
  checkout_exn index block_header.Block_header.shell.context
  >>= fun context ->
  let irmin_info = Dumpable_context.context_info context in
  let author = Irmin.Info.author irmin_info in
  let message = Irmin.Info.message irmin_info in
  let timestamp = Time.Protocol.of_seconds (Irmin.Info.date irmin_info) in
  get_protocol context
  >>=? fun protocol_hash ->
  get_test_chain context
  >>= fun test_chain_status ->
  data_merkle_root_hash context
  >>= fun data_merkle_root ->
  let parents_contexts = Dumpable_context.context_parents context in
  return
    ( protocol_hash,
      author,
      message,
      timestamp,
      test_chain_status,
      data_merkle_root,
      parents_contexts )

let check_protocol_commit_consistency index ~expected_context_hash
    ~given_protocol_hash ~author ~message ~timestamp ~test_chain_status
    ~data_merkle_root ~parents_contexts =
  let data_merkle_root = Hash.of_context_hash data_merkle_root in
  let parents = List.map Hash.of_context_hash parents_contexts in
  let protocol_hash_bytes = Protocol_hash.to_bytes given_protocol_hash in
  let tree = Store.Tree.empty in
  Store.Tree.add
    tree
    current_protocol_key
    (Bytes.unsafe_to_string protocol_hash_bytes)
  >>= fun tree ->
  let test_chain_status_bytes =
    Bytes.unsafe_to_string
      (Data_encoding.Binary.to_bytes_exn
         Test_chain_status.encoding
         test_chain_status)
  in
  Store.Tree.add tree current_test_chain_key test_chain_status_bytes
  >>= fun tree ->
  let info =
    Irmin.Info.v ~date:(Time.Protocol.to_seconds timestamp) ~author message
  in
  let data_tree = Store.Tree.shallow index.repo data_merkle_root in
  Store.Tree.add_tree tree ["data"] data_tree
  >>= fun node ->
  let node = Store.Tree.hash node in
  let commit = P.Commit.Val.v ~parents ~node ~info in
  let computed_context_hash =
    Hash.to_context_hash (P.Commit.Key.hash commit)
  in
  if Context_hash.equal expected_context_hash computed_context_hash then
    let ctxt =
      let parent = Store.of_private_commit index.repo commit in
      {index; tree = Store.Tree.empty; parents = [parent]}
    in
    set_test_chain ctxt test_chain_status
    >>= fun ctxt ->
    set_protocol ctxt given_protocol_hash
    >>= fun ctxt ->
    let data_t = Store.Tree.shallow index.repo data_merkle_root in
    Store.Tree.add_tree ctxt.tree current_data_key data_t
    >>= fun new_tree ->
    Store.Commit.v ctxt.index.repo ~info ~parents new_tree
    >|= fun commit ->
    let ctxt_h = Hash.to_context_hash (Store.Commit.hash commit) in
    Context_hash.equal ctxt_h expected_context_hash
  else Lwt.return_false

(* Context dumper *)

module Context_dumper = Context_dump.Make (Dumpable_context)
module Context_dumper_legacy =
  Context_dump.Make_legacy (Dumpable_context_legacy)

(* provides functions dump_context and restore_context *)
let dump_context idx data ~context_file_path =
  (* TODO better naming *)
  Lwt.catch
    (fun () ->
      Lwt_unix.openfile
        context_file_path
        Lwt_unix.[O_WRONLY; O_CREAT; O_TRUNC]
        0o644
      >>= return)
    (function
      | Unix.Unix_error (e, _, _) ->
          fail (Cannot_create_file (Unix.error_message e))
      | exc ->
          let msg =
            Printf.sprintf "unknown error: %s" (Printexc.to_string exc)
          in
          fail (Cannot_create_file msg))
  >>=? fun context_fd ->
  Lwt.finalize
    (fun () -> Context_dumper.dump_context_fd idx data ~context_fd)
    (fun () -> Lwt_unix.close context_fd)

let restore_context ?expected_block idx ~context_file_path ~target_block
    ~nb_context_elements =
  Lwt.catch
    (fun () ->
      Lwt_unix.openfile context_file_path Lwt_unix.[O_RDONLY] 0o444 >>= return)
    (function
      | Unix.Unix_error (e, _, _) ->
          fail (Cannot_open_file (Unix.error_message e))
      | exc ->
          let msg =
            Printf.sprintf "unknown error: %s" (Printexc.to_string exc)
          in
          fail (Cannot_open_file msg))
  >>=? fun fd ->
  Lwt.finalize
    (fun () ->
      Context_dumper.restore_context_fd
        idx
        ~fd
        ?expected_block
        ~target_block
        ~nb_context_elements
      >>=? fun result ->
      Lwt_unix.lseek fd 0 Lwt_unix.SEEK_CUR
      >>= fun current ->
      Lwt_unix.fstat fd
      >>= fun stats ->
      let total = stats.Lwt_unix.st_size in
      if current = total then return result
      else fail (Suspicious_file (total - current)))
    (fun () -> Lwt_unix.close fd)

let legacy_restore_contexts idx ~filename k_store_pruned_block
    pipeline_validation =
  let file_init () =
    Lwt_unix.openfile filename Lwt_unix.[O_RDONLY] 0o600 >>= return
  in
  Lwt.catch file_init (function
      | Unix.Unix_error (e, _, _) ->
          fail @@ Cannot_open_file (Unix.error_message e)
      | exc ->
          let msg =
            Printf.sprintf "unknown error: %s" (Printexc.to_string exc)
          in
          fail (Cannot_open_file msg))
  >>=? fun fd ->
  Lwt.finalize
    (fun () ->
      Context_dumper_legacy.legacy_restore_contexts_fd
        idx
        ~fd
        k_store_pruned_block
        pipeline_validation
      >>=? fun result ->
      Lwt_unix.lseek fd 0 Lwt_unix.SEEK_CUR
      >>= fun current ->
      Lwt_unix.fstat fd
      >>= fun stats ->
      let total = stats.Lwt_unix.st_size in
      if current = total then return result
      else fail @@ Suspicious_file (total - current))
    (fun () -> Lwt_unix.close fd)

let restore_context_legacy ?expected_block idx ~snapshot_file ~handle_block
    ~handle_protocol_data ~block_validation =
  Lwt.catch
    (fun () ->
      Lwt_unix.openfile snapshot_file [Unix.O_RDONLY] 0o600 >>= return)
    (function
      | Unix.Unix_error (e, _, _) ->
          fail (Cannot_open_file (Unix.error_message e))
      | exc ->
          let msg =
            Printf.sprintf "unknown error: %s" (Printexc.to_string exc)
          in
          fail (Cannot_open_file msg))
  >>=? fun fd ->
  Lwt.finalize
    (fun () ->
      Context_dumper_legacy.restore_context_fd
        idx
        ~fd
        ?expected_block
        ~handle_block
        ~handle_protocol_data
        ~block_validation
      >>=? fun result ->
      Lwt_unix.lseek fd 0 Lwt_unix.SEEK_CUR
      >>= fun current ->
      Lwt_unix.fstat fd
      >>= fun stats ->
      let total = stats.Lwt_unix.st_size in
      if current = total then return result
      else fail (Suspicious_file (total - current)))
    (fun () -> Lwt_unix.close fd)

(* For testing purposes only *)
let dump_legacy_snapshot idx datas ~filename =
  Lwt.catch
    (fun () ->
      Lwt_unix.openfile filename Lwt_unix.[O_WRONLY; O_CREAT; O_TRUNC] 0o666
      >>= fun fd ->
      Lwt.finalize
        (fun () -> Context_dumper_legacy.dump_contexts_fd idx datas ~fd)
        (fun () -> Lwt_unix.close fd))
    (function
      | Unix.Unix_error (e, _, _) ->
          fail @@ Cannot_create_file (Unix.error_message e)
      | exc ->
          let msg =
            Printf.sprintf "unknown error: %s" (Printexc.to_string exc)
          in
          fail (Cannot_create_file msg))

(* For testing purposes only *)
let validate_context_hash_consistency_and_commit ~data_hash
    ~expected_context_hash ~timestamp ~test_chain ~protocol_hash ~message
    ~author ~parents ~index =
  let data_hash = Hash.of_context_hash data_hash in
  let parents = List.map Hash.of_context_hash parents in
  let protocol_value = Protocol_hash.to_bytes protocol_hash in
  let test_chain_value =
    Data_encoding.Binary.to_bytes_exn Test_chain_status.encoding test_chain
  in
  let tree = Store.Tree.empty in
  Store.Tree.add tree current_protocol_key (Bytes.to_string protocol_value)
  >>= fun tree ->
  Store.Tree.add tree current_test_chain_key (Bytes.to_string test_chain_value)
  >>= fun tree ->
  let info =
    Irmin.Info.v ~date:(Time.Protocol.to_seconds timestamp) ~author message
  in
  let data_tree = Store.Tree.shallow index.repo data_hash in
  Store.Tree.add_tree tree ["data"] data_tree
  >>= fun node ->
  let node = Store.Tree.hash node in
  let commit = P.Commit.Val.v ~parents ~node ~info in
  let computed_context_hash =
    Hash.to_context_hash (P.Commit.Key.hash commit)
  in
  if Context_hash.equal expected_context_hash computed_context_hash then
    let ctxt =
      let parent = Store.of_private_commit index.repo commit in
      {index; tree = Store.Tree.empty; parents = [parent]}
    in
    set_test_chain ctxt test_chain
    >>= fun ctxt ->
    set_protocol ctxt protocol_hash
    >>= fun ctxt ->
    let data_t = Store.Tree.shallow index.repo data_hash in
    Store.Tree.add_tree ctxt.tree current_data_key data_t
    >>= fun new_tree ->
    Store.Commit.v ctxt.index.repo ~info ~parents new_tree
    >|= fun commit ->
    let ctxt_h = Hash.to_context_hash (Store.Commit.hash commit) in
    Context_hash.equal ctxt_h expected_context_hash
  else Lwt.return_false
