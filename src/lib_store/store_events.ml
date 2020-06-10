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

open Store_types

module Event = struct
  include Internal_event.Simple

  let section = ["node"; "store"]

  (* Info *)
  let set_head =
    declare_2
      ~section
      ~name:"set_head"
      ~msg:"new head {block_hash} for level {level} was set"
      ~pp1:Block_hash.pp
      ("block_hash", Block_hash.encoding)
      ("level", Data_encoding.int32)

  let set_checkpoint =
    declare_1
      ~section
      ~name:"set_checkpoint"
      ~msg:"the checkpoint was updated to {new_checkpoint}"
      ~pp1:pp_block_descriptor
      ("new_checkpoint", block_descriptor_encoding)

  let set_savepoint =
    declare_1
      ~section
      ~name:"set_savepoint"
      ~msg:"the savepoint was updated to {new_savepoint}"
      ~pp1:pp_block_descriptor
      ("new_savepoint", block_descriptor_encoding)

  let set_caboose =
    declare_1
      ~section
      ~name:"set_caboose"
      ~msg:"the caboose was updated to {new_caboose}"
      ~pp1:pp_block_descriptor
      ("new_caboose", block_descriptor_encoding)

  let store_block =
    declare_2
      ~section
      ~name:"store_block"
      ~msg:"block {block_hash} (level {level}) was stored"
      ~pp1:Block_hash.pp
      ("block_hash", Block_hash.encoding)
      ("level", Data_encoding.int32)

  (* Notice*)
  let fork_testchain =
    declare_5
      ~section
      ~level:Notice
      ~name:"fork_testchain"
      ~msg:
        "the test chain {chain_id} for protocol {protocol_hash} with genesis \
         block hash {genesis_hash} was forked from forking block {fork_hash} \
         (level {fork_level})"
      ~pp1:Chain_id.pp
      ("chain_id", Chain_id.encoding)
      ~pp2:Protocol_hash.pp
      ("protocol_hash", Protocol_hash.encoding)
      ~pp3:Block_hash.pp
      ("genesis_hash", Block_hash.encoding)
      ~pp4:Block_hash.pp
      ("fork_hash", Block_hash.encoding)
      ("fork_level", Data_encoding.int32)

  let start_merging_stores =
    declare_2
      ~section
      ~level:Notice
      ~name:"start_merging_stores"
      ~msg:"starting store merging (from {start} to {end})"
      ("start", Data_encoding.int32)
      ("end", Data_encoding.int32)

  let end_merging_stores =
    declare_1
      ~section
      ~level:Notice
      ~name:"end_merging_stores"
      ~msg:"the store was successfully merged in {time}"
      ("time", Data_encoding.string)
end
