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

type failure_kind =
  | Nothing_to_reconstruct
  | Context_hash_mismatch of Block_header.t * Context_hash.t * Context_hash.t
  | Missing_block of Block_hash.t

let failure_kind_encoding =
  let open Data_encoding in
  union
    [ case
        (Tag 0)
        ~title:"nothing_to_reconstruct"
        empty
        (function Nothing_to_reconstruct -> Some () | _ -> None)
        (fun () -> Nothing_to_reconstruct);
      case
        (Tag 1)
        ~title:"context_hash_mismatch"
        (obj3
           (req "block_header" Block_header.encoding)
           (req "expected" Context_hash.encoding)
           (req "got" Context_hash.encoding))
        (function
          | Context_hash_mismatch (h, e, g) -> Some (h, e, g) | _ -> None)
        (fun (h, e, g) -> Context_hash_mismatch (h, e, g));
      case
        (Tag 2)
        ~title:"missing_block"
        Block_hash.encoding
        (function Missing_block h -> Some h | _ -> None)
        (fun h -> Missing_block h) ]

let failure_kind_pp ppf = function
  | Nothing_to_reconstruct ->
      Format.fprintf ppf "nothing to reconstruct"
  | Context_hash_mismatch (h, e, g) ->
      Format.fprintf
        ppf
        "resulting context hash for block %a (level %ld) does not match. \
         Context hash expected %a, got %a"
        Block_hash.pp
        (Block_header.hash h)
        h.shell.level
        Context_hash.pp
        e
        Context_hash.pp
        g
  | Missing_block h ->
      Format.fprintf
        ppf
        "Unexpected missing block in store: %a"
        Block_hash.pp
        h

type error += Reconstruction_failure of failure_kind

type error += Cannot_reconstruct of History_mode.t

let () =
  let open Data_encoding in
  register_error_kind
    `Permanent
    ~id:"ReconstructionFailure"
    ~title:"Reconstruction failure"
    ~description:"Error while performing storage reconstruction."
    ~pp:(fun ppf reason ->
      Format.fprintf
        ppf
        "The data contained in the storage is not valid. The reconstruction \
         procedure failed: %a."
        failure_kind_pp
        reason)
    (obj1 (req "reason" failure_kind_encoding))
    (function Reconstruction_failure r -> Some r | _ -> None)
    (fun r -> Reconstruction_failure r) ;
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
