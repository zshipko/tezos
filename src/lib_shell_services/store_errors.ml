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

type error += Block_not_found of Block_hash.t

type error += Bad_level of {head_level : Int32.t; given_level : Int32.t}

type error += Block_metadata_not_found of Block_hash.t

type error +=
  | Incorrect_history_mode_switch of {
      previous_mode : History_mode.t;
      next_mode : History_mode.t;
    }

type error +=
  | Invalid_head_switch of {
      minimum_allowed_level : int32;
      given_head : Block_hash.t * int32;
    }

type error += Inconsistent_store_state of string

let () =
  register_error_kind
    `Permanent
    ~id:"store.block.not_found"
    ~title:"Block not found"
    ~description:"Block not found"
    ~pp:(fun ppf block_hash ->
      Format.fprintf ppf "Cannot find block %a" Block_hash.pp block_hash)
    Data_encoding.(obj1 (req "block_not_found" @@ Block_hash.encoding))
    (function Block_not_found block_hash -> Some block_hash | _ -> None)
    (fun block_hash -> Block_not_found block_hash) ;
  register_error_kind
    `Permanent
    ~id:"store.block.bad_level"
    ~title:"Bad level"
    ~description:"Read a block at level past our current head."
    ~pp:(fun ppf (head_level, given_level) ->
      Format.fprintf
        ppf
        "The block at level %ld is beyond our current head's level : %ld."
        given_level
        head_level)
    Data_encoding.(obj2 (req "head_level" int32) (req "given_level" int32))
    (function
      | Bad_level {head_level; given_level} ->
          Some (head_level, given_level)
      | _ ->
          None)
    (fun (head_level, given_level) -> Bad_level {head_level; given_level}) ;
  register_error_kind
    `Permanent
    ~id:"store.block.metadata_not_found"
    ~title:"Block metadata not found"
    ~description:"Block metadata not found"
    ~pp:(fun ppf block_hash ->
      Format.fprintf
        ppf
        "Unable to find block %a's metadata"
        Block_hash.pp
        block_hash)
    Data_encoding.(obj1 (req "block_metadata_not_found" Block_hash.encoding))
    (function
      | Block_metadata_not_found block_hash -> Some block_hash | _ -> None)
    (fun block_hash -> Block_metadata_not_found block_hash) ;
  register_error_kind
    `Permanent
    ~id:"node_config_file.incorrect_history_mode_switch"
    ~title:"Incorrect history mode switch"
    ~description:"Incorrect history mode switch."
    ~pp:(fun ppf (prev, next) ->
      Format.fprintf
        ppf
        "Cannot switch from history mode %a mode to %a mode"
        History_mode.pp
        prev
        History_mode.pp
        next)
    (Data_encoding.obj2
       (Data_encoding.req "previous_mode" History_mode.encoding)
       (Data_encoding.req "next_mode" History_mode.encoding))
    (function
      | Incorrect_history_mode_switch x ->
          Some (x.previous_mode, x.next_mode)
      | _ ->
          None)
    (fun (previous_mode, next_mode) ->
      Incorrect_history_mode_switch {previous_mode; next_mode}) ;
  register_error_kind
    `Permanent
    ~id:"store.invalid_head_switch"
    ~title:"Invalid head switch"
    ~description:
      "The given head is not consistent with the current store's savepoint"
    ~pp:(fun ppf (minimum_allowed_level, (head_hash, head_level)) ->
      Format.fprintf
        ppf
        "The given head %a (%ld) is below the minimum allowed level %ld"
        Block_hash.pp
        head_hash
        head_level
        minimum_allowed_level)
    Data_encoding.(
      obj2
        (req "minimum_allowed_level" int32)
        (req "given_head" @@ tup2 Block_hash.encoding int32))
    (function
      | Invalid_head_switch {minimum_allowed_level; given_head} ->
          Some (minimum_allowed_level, given_head)
      | _ ->
          None)
    (fun (minimum_allowed_level, given_head) ->
      Invalid_head_switch {minimum_allowed_level; given_head}) ;
  register_error_kind
    `Permanent
    ~id:"store.inconsistent_state"
    ~title:"Store inconsistent state"
    ~description:"The store is in an unexpected state."
    ~pp:(fun ppf msg ->
      Format.fprintf ppf "The store is in an unexpected state: %s" msg)
    Data_encoding.(obj1 (req "message" string))
    (function Inconsistent_store_state msg -> Some msg | _ -> None)
    (fun msg -> Inconsistent_store_state msg)
