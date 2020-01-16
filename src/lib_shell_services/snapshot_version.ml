(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018-2019 Dynamic Ledger Solutions, Inc. <contact@tezos.com>*)
(* Copyright (c) 2018-2019 Nomadic Labs. <nomadic@tezcore.com>               *)
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

let current_version = "tezos-snapshot-2.0.0"

type metadata_v_2_0_0 = {
  snapshot_version : string;
  chain_name : Distributed_db_version.Name.t;
  history_mode : History_mode.t;
  block_hash : Block_hash.t;
  level : Int32.t;
  timestamp : Time.Protocol.t;
  context_elements : int;
}

let metadata_v_2_0_0_encoding =
  let open Data_encoding in
  conv
    (fun { snapshot_version;
           chain_name;
           history_mode;
           block_hash;
           level;
           timestamp;
           context_elements } ->
      ( snapshot_version,
        chain_name,
        history_mode,
        block_hash,
        level,
        timestamp,
        context_elements ))
    (fun ( snapshot_version,
           chain_name,
           history_mode,
           block_hash,
           level,
           timestamp,
           context_elements ) ->
      {
        snapshot_version;
        chain_name;
        history_mode;
        block_hash;
        level;
        timestamp;
        context_elements;
      })
    (obj7
       (req "snasphot_version" string)
       (req "chain_name" Distributed_db_version.Name.encoding)
       (req "mode" History_mode.encoding)
       (req "block_hash" Block_hash.encoding)
       (req "level" int32)
       (req "timestamp" Time.Protocol.encoding)
       (req "context_elements" int31))

let metadata_v_2_0_0_pp ppf
    { snapshot_version;
      chain_name;
      history_mode;
      block_hash;
      level;
      timestamp;
      _ } =
  Format.fprintf
    ppf
    "@[<v>Snapshot version: %s@ Chain name: %a@ Block hash: %a@ Block level: \
     %ld@ Block timestamp: %a@ Snapshot mode: %a@]"
    snapshot_version
    Distributed_db_version.Name.pp
    chain_name
    Block_hash.pp
    block_hash
    level
    Time.Protocol.pp_hum
    timestamp
    History_mode.pp
    history_mode

type metadata = metadata_v_2_0_0

let metadata_encoding = metadata_v_2_0_0_encoding

let metadata_pp = metadata_v_2_0_0_pp
