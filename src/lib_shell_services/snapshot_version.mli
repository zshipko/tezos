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

(* Mainnet version is "tezos-snapshot-1.0.0".
   It is not necessary to update the current_version if it is
   already greater than mainnet version.
   This comment should be updated every time a mainnet release is done.
*)

(* Snapshot metadata v2.0.0 *)
(* Includes :
 *    - History_mode
 *    - Contained Block_hash, timestamp and level
 *    - Elements (context, store) *)

val current_version : string

type metadata_v_2_0_0 = {
  snapshot_version : string;
  chain_name : Distributed_db_version.Name.t;
  history_mode : History_mode.t;
  block_hash : Block_hash.t;
  level : Int32.t;
  timestamp : Time.Protocol.t;
  context_elements : int;
}

and metadata = metadata_v_2_0_0

val metadata_encoding : metadata Data_encoding.t

val metadata_pp : Format.formatter -> metadata -> unit
