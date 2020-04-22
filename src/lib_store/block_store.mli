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

type block_store

type t = block_store

type key = Hash of (Block_hash.t * int)

val cemented_block_store : t -> Cemented_block_store.t

val floating_block_stores : t -> Floating_block_store.t list

val global_predecessor_lookup :
  t -> Block_hash.t -> int -> Block_hash.t option Lwt.t

val compute_predecessors : t -> Block_repr.t -> Block_hash.t list Lwt.t

val get_predecessor : t -> Block_hash.t -> int -> Block_hash.t option Lwt.t

val is_known : t -> key -> bool Lwt.t

val read_block : read_metadata:bool -> t -> key -> Block_repr.t option Lwt.t

val read_block_metadata : t -> key -> Block_repr.metadata option Lwt.t

val store_block : t -> Block_repr.t -> unit Lwt.t

val check_blocks_consistency : Block_repr.t list -> bool

val split_cycles : Block_repr.t list -> Block_repr.t list list

val cement_blocks :
  ?check_consistency:bool ->
  write_metadata:bool ->
  t ->
  Block_repr.t list ->
  unit tzresult Lwt.t

val store_metadata_chunk : t -> Block_repr.t list -> unit tzresult Lwt.t

val retrieve_n_predecessors :
  Floating_block_store.t list -> Block_hash.t -> int -> Block_repr.t list Lwt.t

val update_floating_stores :
  ro_store:Floating_block_store.t ->
  rw_store:Floating_block_store.t ->
  new_store:Floating_block_store.t ->
  from_block:block_descriptor ->
  to_block:block_descriptor ->
  nb_blocks_to_preserve:int ->
  Block_repr.t list tzresult Lwt.t

val swap_floating_store :
  t ->
  src:Floating_block_store.t ->
  dst_kind:Floating_block_store.floating_kind ->
  unit tzresult Lwt.t

val try_remove_temporary_stores : t -> unit Lwt.t

val await_merging : t -> unit tzresult Lwt.t

val merge_stores :
  t ->
  ?finalizer:(unit -> unit Lwt.t) ->
  nb_blocks_to_preserve:int ->
  history_mode:History_mode.t ->
  from_block:block_descriptor ->
  to_block:block_descriptor ->
  unit ->
  unit Lwt.t

val create : chain_dir:string -> genesis_block:Block_repr.t -> t tzresult Lwt.t

val load :
  chain_dir:string ->
  genesis_block:Block_repr.t ->
  readonly:bool ->
  t tzresult Lwt.t

val merging_state : t -> [> `Done | `Ongoing of int32]

val close : t -> unit tzresult Lwt.t
