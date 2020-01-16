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
  | Export_info of History_mode.t * Store_types.block_descriptor
  | Export_success of string
  | Set_history_mode of History_mode.t
  | Import_info of string
  | Import_unspecified_hash
  | Import_loading
  | Set_head of Block_hash.t
  | Import_success of string
  | Validate_protocol_sources of Protocol_hash.t

let status_pp ppf = function
  | Export_unspecified_hash h ->
      Format.fprintf
        ppf
        "There is no block hash specified with the `--block` option. Using %a \
         (last savepoint)"
        Block_hash.pp
        h
  | Export_info (hm, (h, l)) ->
      Format.fprintf
        ppf
        "Exporting a snapshot in %a mode, targeting block hash %a at level %a"
        History_mode.pp
        hm
        Block_hash.pp
        h
        Format.pp_print_int
        (Int32.to_int l)
  | Export_success filename ->
      Format.fprintf ppf "Successful export: %s" filename
  | Set_history_mode hm ->
      Format.fprintf ppf "Setting history-mode to %a" History_mode.pp hm
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
  | Validate_protocol_sources protocol_hash ->
      Format.fprintf
        ppf
        "Validating protocol %a against sources."
        Protocol_hash.pp
        protocol_hash

module Definition = struct
  let name = "snapshot"

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
                (req "history_mode" History_mode.encoding)
                (req "block_hash" Block_hash.encoding)
                (req "level" int32))
             (function
               | Export_info (hm, (h, l)) -> Some (hm, h, l) | _ -> None)
             (fun (hm, h, l) -> Export_info (hm, (h, l)));
           case
             (Tag 2)
             ~title:"Export_success"
             string
             (function Export_success s -> Some s | _ -> None)
             (fun s -> Export_success s);
           case
             (Tag 3)
             ~title:"Set_history_mode"
             History_mode.encoding
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
             ~title:"Validate_protocol_sources"
             Protocol_hash.encoding
             (function Validate_protocol_sources h -> Some h | _ -> None)
             (fun h -> Validate_protocol_sources h) ]

  let pp ~short:_ ppf (status : t) =
    Format.fprintf ppf "%a" status_pp status.data

  let doc = "Snapshot status."

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
    | Validate_protocol_sources _ ->
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
