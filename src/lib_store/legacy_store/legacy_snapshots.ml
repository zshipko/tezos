(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2019 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2019 Nomadic Labs. <nomadic@tezcore.com>                    *)
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
  | Export_unspecified_hash of Block_hash.t
  | Export_info of History_mode.Legacy.t * Block_hash.t * Int32.t
  | Export_success of string
  | Set_history_mode of History_mode.Legacy.t
  | Import_info of string
  | Import_unspecified_hash
  | Import_loading
  | Set_head of Block_hash.t
  | Import_success of string
  | Reconstruct_start_default
  | Reconstruct_end_default of Block_hash.t
  | Reconstruct_enum
  | Reconstruct_success

let status_pp ppf = function
  | Export_unspecified_hash h ->
      Format.fprintf
        ppf
        "There is no block hash specified with the `--block` option. Using %a \
         (last checkpoint)"
        Block_hash.pp
        h
  | Export_info (hm, h, l) ->
      Format.fprintf
        ppf
        "Exporting a snapshot in %a mode, targeting block hash %a at level %a"
        History_mode.Legacy.pp
        hm
        Block_hash.pp
        h
        Format.pp_print_int
        (Int32.to_int l)
  | Export_success filename ->
      Format.fprintf ppf "@[Successful export: %s@]" filename
  | Set_history_mode hm ->
      Format.fprintf ppf "Setting history-mode to %a" History_mode.Legacy.pp hm
  | Import_info filename ->
      Format.fprintf ppf "Importing data from snapshot file %s" filename
  | Import_unspecified_hash ->
      Format.fprintf
        ppf
        "You may consider using the --block <block_hash> argument to verify \
         that the block imported is the one you expected"
  | Import_loading ->
      Format.fprintf
        ppf
        "Retrieving and validating data. This can take a while, please bear \
         with us"
  | Set_head h ->
      Format.fprintf ppf "Setting current head to block %a" Block_hash.pp h
  | Import_success filename ->
      Format.fprintf ppf "@[Successful import from file %s@]" filename
  | Reconstruct_start_default ->
      Format.fprintf ppf "Starting reconstruct from genesis"
  | Reconstruct_end_default h ->
      Format.fprintf
        ppf
        "Starting reconstruct toward the predecessor of the current head (%a)"
        Block_hash.pp
        h
  | Reconstruct_enum ->
      Format.fprintf ppf "Enumerating all blocks to reconstruct"
  | Reconstruct_success ->
      Format.fprintf ppf "The storage was successfully reconstructed."

module Definition = struct
  let name = "legacy_snapshot"

  type t = status Time.System.stamped

  let encoding =
    let open Data_encoding in
    Time.System.stamped_encoding
    @@ union
         [ case
             (Tag 0)
             ~title:"Export_unspecified_hash"
             Block_hash.encoding
             (function Export_unspecified_hash h -> Some h | _ -> None)
             (fun h -> Export_unspecified_hash h);
           case
             (Tag 1)
             ~title:"Export_info"
             (obj3
                (req "history_mode" History_mode.Legacy.encoding)
                (req "block_hash" Block_hash.encoding)
                (req "level" int32))
             (function Export_info (hm, h, l) -> Some (hm, h, l) | _ -> None)
             (fun (hm, h, l) -> Export_info (hm, h, l));
           case
             (Tag 2)
             ~title:"Export_success"
             string
             (function Export_success s -> Some s | _ -> None)
             (fun s -> Export_success s);
           case
             (Tag 3)
             ~title:"Set_history_mode"
             History_mode.Legacy.encoding
             (function Set_history_mode hm -> Some hm | _ -> None)
             (fun hm -> Set_history_mode hm);
           case
             (Tag 4)
             ~title:"Import_info"
             string
             (function Import_info s -> Some s | _ -> None)
             (fun s -> Import_info s);
           case
             (Tag 5)
             ~title:"Import_unspecified_hash"
             empty
             (function Import_unspecified_hash -> Some () | _ -> None)
             (fun () -> Import_unspecified_hash);
           case
             (Tag 6)
             ~title:"Import_loading"
             empty
             (function Import_loading -> Some () | _ -> None)
             (fun () -> Import_loading);
           case
             (Tag 7)
             ~title:"Set_head"
             Block_hash.encoding
             (function Set_head h -> Some h | _ -> None)
             (fun h -> Set_head h);
           case
             (Tag 8)
             ~title:"Import_success"
             string
             (function Import_success s -> Some s | _ -> None)
             (fun s -> Import_success s);
           case
             (Tag 9)
             ~title:"Reconstruct_start_default"
             empty
             (function Reconstruct_start_default -> Some () | _ -> None)
             (fun () -> Reconstruct_start_default);
           case
             (Tag 10)
             ~title:"Reconstruct_end_default"
             Block_hash.encoding
             (function Reconstruct_end_default h -> Some h | _ -> None)
             (fun h -> Reconstruct_end_default h);
           case
             (Tag 11)
             ~title:"Reconstruct_enum"
             empty
             (function Reconstruct_enum -> Some () | _ -> None)
             (fun () -> Reconstruct_enum);
           case
             (Tag 12)
             ~title:"Reconstruct_success"
             empty
             (function Reconstruct_success -> Some () | _ -> None)
             (fun () -> Reconstruct_success) ]

  let pp ~short:_ ppf (status : t) =
    Format.fprintf ppf "%a" status_pp status.data

  let doc = "Snapshots status."

  let level (status : t) =
    match status.data with
    | Export_unspecified_hash _
    | Export_info _
    | Export_success _
    | Set_history_mode _
    | Import_info _
    | Import_unspecified_hash
    | Import_loading
    | Set_head _
    | Import_success _
    | Reconstruct_start_default
    | Reconstruct_end_default _
    | Reconstruct_enum
    | Reconstruct_success ->
        Internal_event.Notice
end

module Event_snapshot = Internal_event.Make (Definition)

let lwt_emit (status : status) =
  let time = Systime_os.now () in
  Event_snapshot.emit
    ~section:(Internal_event.Section.make_sanitized [Definition.name])
    (fun () -> Time.System.stamp ~time status)
  >>= function
  | Ok () ->
      Lwt.return_unit
  | Error el ->
      Format.kasprintf
        Lwt.fail_with
        "Snapshot_event.emit: %a"
        pp_print_error
        el

type error +=
  | Wrong_snapshot_export of History_mode.Legacy.t * History_mode.Legacy.t

type wrong_block_export_kind =
  | Pruned of Block_hash.t
  | Too_few_predecessors of Block_hash.t
  | Unknown_block of string

type error += Wrong_block_export of wrong_block_export_kind

let compute_export_limit block_store chain_data_store block_header
    export_rolling =
  let block_hash = Block_header.hash block_header in
  Legacy_store.Block.Contents.read_opt (block_store, block_hash)
  >>= (function
        | Some contents ->
            return contents
        | None ->
            fail (Wrong_block_export (Pruned block_hash)))
  >>=? fun {max_operations_ttl; _} ->
  if not export_rolling then
    Legacy_store.Chain_data.Caboose.read chain_data_store
    >>=? fun (caboose_level, _) -> return (max 1l caboose_level)
  else
    let limit =
      Int32.(
        sub block_header.Block_header.shell.level (of_int max_operations_ttl))
    in
    (* fails when the limit exceeds the genesis or the genesis is
       included in the export limit *)
    fail_when
      (limit <= 0l)
      (Wrong_block_export (Too_few_predecessors block_hash))
    >>=? fun () -> return limit

type data = {
  info : Context.Protocol_data.info;
  protocol_hash : Protocol_hash.t;
  test_chain_status : Test_chain_status.t;
  data_key : Context_hash.t;
  parents : Context_hash.t list;
}

let get_protocol_data_from_header index block_header =
  let open Context in
  let level = block_header.Block_header.shell.level in
  Context.retrieve_commit_info index block_header
  >>=? fun ( protocol,
             author,
             message,
             timestamp,
             test_chain_status,
             context_hash,
             parents ) ->
  let info = {Protocol_data.timestamp; author; message} in
  return
    ( level,
      {
        protocol_hash = protocol;
        test_chain_status;
        data_key = context_hash;
        parents;
        info;
      } )

(** When called with a block, returns its predecessor if it exists and
    its protocol_data if the block is a transition block (i.e. protocol
    level changing block) or when there is no more predecessor. *)
let pruned_block_iterator index block_store limit header =
  if header.Block_header.shell.level <= limit then
    get_protocol_data_from_header index header
    >>= fun protocol_data -> return (None, Some protocol_data)
  else
    let pred_hash = header.Block_header.shell.predecessor in
    Legacy_state.Block.Header.read (block_store, pred_hash)
    >>=? fun pred_header ->
    Legacy_store.Block.Operations.bindings (block_store, pred_hash)
    >>= fun pred_operations ->
    Legacy_store.Block.Operation_hashes.bindings (block_store, pred_hash)
    >>= fun pred_operation_hashes ->
    let pruned_block =
      {
        Context.Pruned_block.block_header = pred_header;
        operations = pred_operations;
        operation_hashes = pred_operation_hashes;
      }
    in
    let header_proto_level = header.Block_header.shell.proto_level in
    let pred_header_proto_level = pred_header.Block_header.shell.proto_level in
    if header_proto_level <> pred_header_proto_level then
      get_protocol_data_from_header index header
      >>= fun proto_data -> return (Some pruned_block, Some proto_data)
    else return (Some pruned_block, None)

let export ?(export_rolling = false) ~context_root ~store_root ~genesis
    filename block_hash =
  Legacy_state.init ~context_root ~store_root genesis ~readonly:true
  >>=? fun (state, _chain_state, context_index, history_mode) ->
  Legacy_store.init store_root
  >>=? fun store ->
  let chain_id = Chain_id.of_block_hash genesis.block in
  let chain_store = Legacy_store.Chain.get store chain_id in
  let chain_data_store = Legacy_store.Chain_data.get chain_store in
  let block_store = Legacy_store.Block.get chain_store in
  ( match history_mode with
  | Archive | Full ->
      return_unit
  | Rolling as history_mode ->
      if export_rolling then return_unit
      else
        fail (Wrong_snapshot_export (history_mode, History_mode.Legacy.Full))
  )
  >>=? fun () ->
  Legacy_state.Block.Header.read_opt (block_store, block_hash)
  >>= (function
        | None ->
            fail
              (Wrong_block_export
                 (Unknown_block (Block_hash.to_b58check block_hash)))
        | Some block_header ->
            let export_mode =
              if export_rolling then History_mode.Legacy.Rolling else Full
            in
            lwt_emit
              (Export_info (export_mode, block_hash, block_header.shell.level))
            >>= fun () ->
            (* Get block predecessor's block header *)
            Legacy_store.Block.Predecessors.read (block_store, block_hash) 0
            >>=? fun pred_block_hash ->
            Legacy_state.Block.Header.read (block_store, pred_block_hash)
            >>=? fun pred_block_header ->
            (* Get operation list *)
            let validations_passes = block_header.shell.validation_passes in
            map_s
              (fun i ->
                Legacy_store.Block.Operations.read (block_store, block_hash) i)
              (0 -- (validations_passes - 1))
            >>=? fun operations ->
            compute_export_limit
              block_store
              chain_data_store
              block_header
              export_rolling
            >>=? fun export_limit ->
            let iterator =
              pruned_block_iterator context_index block_store export_limit
            in
            let block_data =
              {
                Context.Block_data.block_header;
                operations;
                predecessor_header = pred_block_header;
              }
            in
            return (pred_block_header, block_data, export_mode, iterator))
  >>=? fun (_, data_to_dump, _, _) ->
  Context.dump_context context_index data_to_dump ~context_file_path:filename
  >>=? fun _ ->
  lwt_emit (Export_success filename)
  >>= fun () ->
  Legacy_store.close store ;
  Legacy_state.close state >>= fun () -> return_unit
