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
  | Reconstruct_start_default
  | Reconstruct_end_default of Block_hash.t
  | Reconstruct_enum
  | Reconstruct_success

let status_pp ppf = function
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
  let name = "reconstruction"

  type t = status Time.System.stamped

  let encoding =
    let open Data_encoding in
    Time.System.stamped_encoding
    @@ union
         [ case
             (Tag 0)
             ~title:"Reconstruct_start_default"
             empty
             (function Reconstruct_start_default -> Some () | _ -> None)
             (fun () -> Reconstruct_start_default);
           case
             (Tag 1)
             ~title:"Reconstruct_end_default"
             Block_hash.encoding
             (function Reconstruct_end_default h -> Some h | _ -> None)
             (fun h -> Reconstruct_end_default h);
           case
             (Tag 2)
             ~title:"Reconstruct_enum"
             empty
             (function Reconstruct_enum -> Some () | _ -> None)
             (fun () -> Reconstruct_enum);
           case
             (Tag 3)
             ~title:"Reconstruct_success"
             empty
             (function Reconstruct_success -> Some () | _ -> None)
             (fun () -> Reconstruct_success) ]

  let pp ~short:_ ppf (status : t) =
    Format.fprintf ppf "%a" status_pp status.data

  let doc = "Reconstruction status."

  let level (status : t) =
    match status.data with
    | Reconstruct_start_default
    | Reconstruct_end_default _
    | Reconstruct_enum
    | Reconstruct_success ->
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

type error += Reconstruction_failure of string

type error += Cannot_reconstruct of History_mode.t

let () =
  let open Data_encoding in
  register_error_kind
    `Permanent
    ~id:"ReconstructionFailure"
    ~title:"Reconstruction failure"
    ~description:"Error while performing storage reconstruction."
    ~pp:(fun ppf msg ->
      Format.fprintf
        ppf
        "The data contained in the storage is not valid. The reconstruction \
         procedure failed: %s."
        msg)
    (obj1 (req "message" string))
    (function Reconstruction_failure str -> Some str | _ -> None)
    (fun str -> Reconstruction_failure str) ;
  register_error_kind
    `Permanent
    ~id:"CannotReconstruct"
    ~title:"Cannot reconstruct"
    ~description:"Cannot reconstruct"
    ~pp:(fun ppf hm ->
      Format.fprintf
        ppf
        "Cannot reconstruct storage from %a mode."
        History_mode.pp
        hm)
    (obj1 (req "history_mode " History_mode.encoding))
    (function Cannot_reconstruct hm -> Some hm | _ -> None)
    (fun hm -> Cannot_reconstruct hm)
