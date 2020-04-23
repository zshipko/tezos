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

(** Persistent block store with arborescent history

    The floating block store is an append-only store where blocks are
    stored arbitrarily. This structure possess an indexed map
    {Block_hash.t} -> (offset × predecessors) which points to its
    offset in the associated file along with a list of block
    predecessors. The structure access/modification is protected by a
    mutex ({Lwt_idle_waiter}) and thus can be concurently manipulated.
    Stored blocks may or may not contain metadata. The instance
    maintains an opened file descriptor therefore it must be properly
    closed or it might lead to a fd leak.

    Four differents kind of instance are allowed to co-exist for an
    identitcal path: - RO, a read-only instance; - RW, a read-write
    instance - RO_TMP, RW_TMP, read-write instances.  See {Block_store}

    {1 Invariants}

    This store is expected to respect the following invariant:

    - Every block stored is correctly indexed.

    {1 Files format}

    The floating block store is composed of the following files:

    - file : /<kind>_floating_block_store, a list of {Block_repr.t}:

    | <block> * |

    where <kind> is RO(_TMP), RW(_TMP) (see {Naming}) and <block>, a
    {Block_repr.t} value encoded using {Block_repr.encoding} (thus
    prefixed by the its size).
*)

(** The type of the floating store. *)
type t

(** The type for the kind of floating store opened. *)
type floating_kind = Naming.floating_kind = RO | RW | RW_TMP | RO_TMP

(** [kind floating_store] return the kind. *)
val kind : t -> floating_kind

(** [mem floating_store hash] test whether [hash] is stored in
    [floating_store]. *)
val mem : t -> Block_hash.t -> bool Lwt.t

(** [find_predecessors floating_store hash] reads from the index the
    list of predecessors for [hash] if the block is stored in
    [floating_store], returns [None] otherwise. *)
val find_predecessors : t -> Block_hash.t -> Block_hash.t list option Lwt.t

(** [read_block floating_store hash] reads from the file the block of
    [hash] if the block is stored in [floating_store], returns [None]
    otherwise. *)
val read_block : t -> Block_hash.t -> Block_repr.t option Lwt.t

(** [append_block ?should_flush floating_store preds block] stores the
    [block] in [floating_store] updating its index with the given
    predecessors [preds]. *)
val append_block : t -> Block_hash.t list -> Block_repr.t -> unit Lwt.t

(** [append_all floating_store chunk] stores the [chunk] of
    (predecessors × blocks) in [floating_store] updating its index
    accordingly. *)
val append_all : t -> (Block_hash.t list * Block_repr.t) list -> unit Lwt.t

(** [iter_raw_fd f fd] unsafe sequential iterator on a direct file descriptor
    [fd]. Applies [f] on every block encountered.

    {b Warning}: should be used for internal use only (e.g. snapshots). *)
val iter_raw_fd :
  (Block_repr.t -> unit tzresult Lwt.t) ->
  Lwt_unix.file_descr ->
  unit tzresult Lwt.t

(** [iter_seq f floating_store] sequential iterator on the
    [floating_store]. Applies [f] on every block encountered. *)
val iter_seq :
  (Block_repr.t * Block_hash.t list -> unit tzresult Lwt.t) ->
  t ->
  unit tzresult Lwt.t

(** [init ~chain_dir ~readonly kind] creates or load an existing
    floating store at path [chain_dir] with [kind]. [RO] floating
    stores may be written into if [readonly] is false. *)
val init : chain_dir:string -> readonly:bool -> floating_kind -> t Lwt.t

(** [close floating_store] closes [floating_store] closing the index
    and the opened file descriptor. *)
val close : t -> unit Lwt.t
