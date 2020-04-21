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

let ( // ) = Filename.concat

let chain_store chain_id =
  Format.asprintf "chain_%a" Chain_id.pp_short chain_id

let configuration_file = "config.json"

let protocol_store_directory = "protocols"

let protocol_file hash = Protocol_hash.to_b58check hash

let lockfile = "lock"

module Chain_data = struct
  let protocol_levels = "protocol_levels"

  let genesis = "genesis"

  let current_head = "current_head"

  let alternate_heads = "alternate_heads"

  let checkpoint = "checkpoint"

  let savepoint = "savepoint"

  let caboose = "caboose"

  let invalid_blocks = "invalid_blocks"

  let forked_chains = "forked_chains"
end

let cemented_blocks_directory = "cemented"

let cemented_blocks_metadata_directory = "metadata"

let cemented_metadata_file ~cemented_filename = cemented_filename ^ ".zip"

let cemented_block_level_index_directory = "level_index"

let cemented_block_hash_index_directory = "hash_index"

let testchain_dir = "testchains"

type floating_kind = RO | RW | RW_TMP | RO_TMP

let floating_block_index = function
  | RO ->
      "ro_floating_block_index"
  | RW ->
      "rw_floating_block_index"
  | RO_TMP ->
      "ro_tmp_floating_block_index"
  | RW_TMP ->
      "rw_tmp_floating_block_index"

let floating_blocks = function
  | RO ->
      "ro_floating_blocks"
  | RW ->
      "rw_floating_blocks"
  | RO_TMP ->
      "ro_tmp_floating_blocks"
  | RW_TMP ->
      "rw_tmp_floating_blocks"

module Snapshot = struct
  let context = "context"

  let cemented_blocks = "cemented_blocks"

  let floating_blocks = "floating_blocks"

  let protocols = "protocols"

  let protocols_table = "protocols_index"

  let metadata = "metadata.json"
end
