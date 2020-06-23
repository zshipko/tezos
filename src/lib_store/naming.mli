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

(** Filenames convention. *)

(** Path concatenation operator (alias of {!Filename.concat}). *)
val ( // ) : string -> string -> string

(** Chain store directory name. *)
val chain_store : Chain_id.t -> string

(** {1 Protocols} *)

(** Protocol directory name. *)
val protocol_store_directory : string

(** Protocol filename *)
val protocol_file : Protocol_hash.t -> string

(** Chain configuration filename. *)
val chain_config_file : string

(** Store lockfile name. *)
val lockfile : string

(** Reconstruction lock filename. *)
val reconstruction_lock : string

(** Persistent chain data name. *)
module Chain_data : sig
  (** Protocol levels filename. *)
  val protocol_levels : string

  (** Genesis block filename. *)
  val genesis : string

  (** Current head filename. *)
  val current_head : string

  (** Alternate heads filename. *)
  val alternate_heads : string

  (** Checkpoint filename. *)
  val checkpoint : string

  (** Savepoint filename. *)
  val savepoint : string

  (** Caboose filename. *)
  val caboose : string

  (** Invalid blocks filename. *)
  val invalid_blocks : string

  (** Forked chains filename. *)
  val forked_chains : string
end

(** Cemented blocks directory name. *)
val cemented_blocks_directory : string

(** Cemented blocks metadata directory name. *)
val cemented_blocks_metadata_directory : string

(** Cemented blocks metadata file. *)
val cemented_metadata_file : cemented_filename:string -> string

(** Cemented blocks level index directory name. *)
val cemented_block_level_index_directory : string

(** Cemented blocks hash index directory name. *)
val cemented_block_hash_index_directory : string

(** Cemented block file name based on [start_level] and [end_level]. *)
val cemented_block_filename : start_level:int32 -> end_level:int32 -> string

(** Testchain directory name. *)
val testchain_dir : string

(** The type of floating store's kind. *)
type floating_kind = RO | RW | RW_TMP | RO_TMP

(** Floating block index directory name. *)
val floating_block_index : floating_kind -> string

(** Floating block store filename. *)
val floating_blocks : floating_kind -> string

(** Snapshots filenames. *)
module Snapshot : sig
  (** Snapshots context filename. *)
  val context : string

  (** Snapshot cemented blocks filename. *)
  val cemented_blocks : string

  (** Snapshot floating blocks filename. *)
  val floating_blocks : string

  (** Snapshot protocols filename. *)
  val protocols : string

  (** Snapshot protocols table filename. *)
  val protocols_table : string

  (** Snapshot metadata filename. *)
  val metadata : string

  (** Snapshot temporary export directory *)
  val temp_export_dir : string
end
