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

(** The number of predecessors stored per block.
    This value chosen to compute efficiently block locators that
    can cover a chain of 2 months, at 1 block/min, which is ~86K
    blocks at the cost in space of ~72MB.
    |locator| = log2(|chain|/10) -1
*)
let stored_predecessors_size = 12

let nb_predecessors = 12

(* Invariant: 12 predecessors at all time (if the block is too low to
   have 12 predecessors then it is padded with genesis hashes) *)
module Block_info = struct
  type t = {offset : int; predecessors : Block_hash.t list}

  let encoding =
    let open Data_encoding in
    conv
      (fun {offset; predecessors} -> (offset, predecessors))
      (fun (offset, predecessors) -> {offset; predecessors})
      (obj2
         (req "offset" int31)
         (req
            "predecessors"
            (* assumed dynamic *)
            (list ~max_length:nb_predecessors Block_hash.encoding)))

  let encoded_size = 4 + 4 + (32 * nb_predecessors)

  let encode v =
    let bytes = Data_encoding.Binary.to_bytes_exn encoding v in
    let padding = encoded_size - Bytes.length bytes in
    Bytes.unsafe_to_string (Bytes.cat bytes (Bytes.create padding))

  let decode str i =
    let bytes = Bytes.sub (Bytes.unsafe_of_string str) i encoded_size in
    (* Remove the padding if need be *)
    let len_list = Bytes.sub bytes 4 4 in
    let len = Data_encoding.(Binary.of_bytes_exn int31 len_list) in
    let bytes = Bytes.sub bytes 0 (4 + 4 + len) in
    Data_encoding.Binary.of_bytes_exn encoding bytes

  let pp fmt v =
    let json = Data_encoding.Json.construct encoding v in
    Data_encoding.Json.pp fmt json
end

(* Hashmap from block's hashes to location *)
include Index_unix.Make (Block_key) (Block_info)
