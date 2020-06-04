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

type error +=
  | Incompatible_history_mode of {
      requested : History_mode.t;
      stored : History_mode.t;
    }
  | Invalid_export_block of {
      block : Block_hash.t option;
      reason :
        [ `Pruned
        | `Pruned_pred
        | `Unknown
        | `Caboose
        | `Genesis
        | `Not_enough_pred ];
    }
  | Snapshot_file_not_found of string
  | Inconsistent_protocol_hash of {
      expected : Protocol_hash.t;
      got : Protocol_hash.t;
    }
  | Inconsistent_context_hash of {
      expected : Context_hash.t;
      got : Context_hash.t;
    }
  | Inconsistent_context of Context_hash.t
  | Cannot_decode_protocol of string
  | Cannot_write_metadata of string
  | Cannot_read_metadata of string
  | Inconsistent_floating_store of block_descriptor * block_descriptor
  | Missing_target_block of block_descriptor
  | Cannot_read_floating_store of string
  | Cannot_retrieve_block_interval
  | Invalid_cemented_file of string
  | Missing_cemented_file of string
  | Corrupted_floating_store
  | Invalid_protocol_file of string
  | Target_block_validation_failed of Block_hash.t * string
  | Directory_already_exists of string
  | Empty_floating_store
  | Inconsistent_predecessors
  | Snapshot_import_failure of string
  | Snapshot_export_failure of string

val current_version : int

type metadata = {
  version : int;
  chain_name : Distributed_db_version.Name.t;
  history_mode : History_mode.t;
  block_hash : Block_hash.t;
  level : Int32.t;
  timestamp : Time.Protocol.t;
  context_elements : int;
}

val metadata_encoding : metadata Data_encoding.t

val pp_metadata : Format.formatter -> metadata -> unit

val read_snapshot_metadata : snapshot_file:string -> metadata tzresult Lwt.t

val export :
  ?rolling:bool ->
  ?compress:bool ->
  block:Block_services.block ->
  store_dir:string ->
  context_dir:string ->
  chain_name:Distributed_db_version.Name.t ->
  snapshot_file:string ->
  Genesis.t ->
  unit tzresult Lwt.t

val import :
  ?patch_context:(Context.t -> Context.t tzresult Lwt.t) ->
  ?block:string ->
  ?check_consistency:bool ->
  snapshot_file:string ->
  dst_store_dir:string ->
  dst_context_dir:string ->
  user_activated_upgrades:User_activated.upgrades ->
  user_activated_protocol_overrides:User_activated.protocol_overrides ->
  Genesis.t ->
  unit tzresult Lwt.t

val import_legacy :
  ?patch_context:(Context.t -> Context.t tzresult Lwt.t) ->
  ?block:string ->
  dst_store_dir:string ->
  dst_context_dir:string ->
  chain_name:Distributed_db_version.Name.t ->
  user_activated_upgrades:User_activated.upgrades ->
  user_activated_protocol_overrides:User_activated.protocol_overrides ->
  snapshot_file:string ->
  Genesis.t ->
  unit tzresult Lwt.t
