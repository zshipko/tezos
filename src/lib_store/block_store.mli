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

(** Persistent and cached generic block store

    The store instanciate a cemented block store and multiple floating
    block store. The floating stores serves as buffer until enough
    blocks arrived to perform a "cementing" (also called a "merge").
    Under normal circumstances, there are two different kind
    ({!Floating_block_store.floating_kind} of floating stores instances:
    a [RO] and a [RW]. Newly arrived blocks are {b always} pushed in
    the [RW] instance. The block lookup is first tried in [RW] then
    [RO] and finally in the cement blocks.

    This store also instanciate a LRU block cache to reduce the number
    of I/O operations. This cache is updated whenever a block is read
    or stored.

    When a merge occurs, the [RW] instance is promoted as another
    [RO'] and a new [RW'] instance replaces it. This allows retrieving
    the new cycle to be cemented from [RO] and [RO'] (former [RW]) {b
    asynchronously} and thus allowing new blocks to be stored in the
    newly instanciated [RW] store without pausing. This asynchronous
    merging thread, while retrieving the cycle is to cemented, also
    combines [RO] and [RO'] into a {b new} [RO''] without the cemented
    cycle. After the merging thread is done, the former [RO] and [RO']
    instances are deleted from the disk and the new [RO''] replaces
    them. A merging thread has to wait for the previous one to finish.

    Retrieving the new cycle from [RO] and [RO'] from blocks [B_start]
    and [B_end] means that we must retrieve the set of blocks between
    them but also trim potential branches that has roots in this set.
    To achieve that, we iterate over [RO] and [RO'] {b linearly}. This
    means that {b every block's predecessor in floating stores must be
    previously known} either we previously encountered it in the same
    floating store file, either in [RO] if the block is in [RO'] or in
    the cemented store (see invariants below). This invariant is
    required to ensure minimal memory usage. The iterations done to
    retrieve the cycle and merge the floating stores works in a similar
    fashion as a stop and copy GC algorithm and works as follows:

    - We retrieve the blocks from [B_end] to [B_start] by sequentially
      reading predecessors.

    - We instanciate a set of encountered blocks hash with [B_end]'s
      hash as initial value.

    - We iterate sequentially over [RO] and [RO'] blocks and copy them
      only if their predecessors is present in the visited set, adding
      their hash in the process.

    The result is a correct, in order and trimmed new [RO] floating
    store. A visual example of merging is given below.

    The merging thread will also trigger a garbage-collection of the
    cemented block store w.r.t. the given history mode.


    {1 Invariants}

    This store is expected to respect the following invariants:

    - If no merging thread is pending, two floatings stores are
      present: a [RO] and a [RW].

    - If a merging thread is pending, there is three floatings stores
      present: a [RO], a [RO'] and a [RW].

    - No blocks are stored twice in {b floating stores}, however,
      blocks can be present twice in cemented and in a floating store.

    - For every stored block in floating stores, its predecessor is
      either stored previously in the same floating store file, in a
      previous floating store file or in the cemented store.

    {b Warning} This previous invariant does not hold for the first
    block of a [RO] store running a rolling history mode but should
    never be part of a cycle.

    - A merging thread does not start until the previous one has
      completed.

*)

(** {1 Merging example}
{v
           RO          RW

                 | C' - D' - E'   G'
              /  |               /
         A - B - | C - D - E - F - G
                 |  \
                 |    D'' - E''
                 |     \
                 |       E'''
v}
*)

(** For instance, a merging from [A] to [C] will first retrieve blocks
    [C], [B] then [A]. Then iterate over all the blocks in both [RO]
    and [RW] files, respectively in this order. By construction, blocks
    are stored after their predecessors. For example, \[ A ; B ; C ;
    C'; D'' ; D'; D ; E ; E'' ; F ; E''' ; G' ; G \] is a correctly
    ordered storing sequence.

    The algorithm start iterating over this sequence and will only
    copy blocks for which predecessors are present in the set S of hash
    (initially S = \{ hash([C]) \}). Thus, for the given sequence, [D'']
    will first be considered, S will be updated to \{ hash([C]),
    hash([D]) \} and so on, until [RO] and [RW] are fully read.

    The new RO will then be :
{v
                G'
               /
    - D - E - F - G

    - D'' - E''
       \
        E'''
v}
    where its storing order will be correct with regards to the
    invariant.
*)

open Store_types

(** The type of the block store *)
type block_store

type t = block_store

(** The type of for block's key to be accessed : a hash and an
    offset.

    - Block (h, 0) represents the block h itself ;

    - Block (h, n) represents the block's [n]th predecessor. *)
type key = Block of (Block_hash.t * int)

(** [cemented_block_store block_store] returns the instance of the
    cemented block store for [block_store]. *)
val cemented_block_store : block_store -> Cemented_block_store.t

(** [floating_block_stores block_store] returns all running floating
    block store instances for [block_store]. It will always return two
    or three ordered floating stores:

     - [ [RO] ; [RW] ] if a merge is not occuring;

     - [ [RO] ; [RO'] ; [RW] ] if a merge is occuring.

    {b Warning} These stores should only be accessed when the store is
    not active. *)
val floating_block_stores : block_store -> Floating_block_store.t list

(** [mem block_store key] tests the existence of the block [key] in
    [block_store]. *)
val mem : block_store -> key -> bool Lwt.t

(** [get_hash block_store key] retrieve the hash of [key] in
    [block_store]. Return [None] if the block is unknown. *)
val get_hash : block_store -> key -> Block_hash.t option Lwt.t

(** [read_block block_store key] read the block [key] in
    [block_store] if present. Return [None] if the block is unknown. *)
val read_block :
  read_metadata:bool -> block_store -> key -> Block_repr.t option Lwt.t

(** [read_block_metadata block_store key] read the metadata for the
    block [key] in [block_store] if present. Return [None] if the
    the block is unknown or if the metadata are not present. *)
val read_block_metadata :
  block_store -> key -> Block_repr.metadata option Lwt.t

(** [store_block block_store block] store the [block] in the current
    [RW] floating store. *)
val store_block : block_store -> Block_repr.t -> unit Lwt.t

(** [cement_blocks ?check_consistency ~write_metadata block_store
    chunk]

    Wrapper of {!Cemented_block_store.cement_blocks}. If the flag
    [check_consistency] is set, it verifies that all the blocks in [chunk]
    are in a consecutive order. *)
val cement_blocks :
  ?check_consistency:bool ->
  write_metadata:bool ->
  block_store ->
  Block_repr.t list ->
  unit tzresult Lwt.t

(** [swap_floating_store block_store ~src ~dst_kind] closes the
    floating store [src] and tests the existence of a [dst_kind] store
    opened in [block_store] and try closing it if this is the case. It
    then proceed to replace the files from [src] to [dst].

    This function is unsafe and should only be called in very specific
    cases.

    {b Warning} [block_store] remains unchanged meaning that the
    potential deleted floating store is referenced in the structure.

    Fails if both [src] and [dst] (if it exists) have the same
    {!Floating_block_store.floating_kind}. *)
val swap_floating_store :
  block_store ->
  src:Floating_block_store.t ->
  dst_kind:Floating_block_store.floating_kind ->
  unit tzresult Lwt.t

(** [await_merging block_store] waits for the current merging thread
    in [block_store] to finish if any. *)
val await_merging : block_store -> unit tzresult Lwt.t

(** [merge_stores block_store ?finalizer ~nb_blocks_to_preserve
    ~history_mode ~from_block ~to_block] triggers a merge as described
    in the description above. This will result, {b asynchronously}, by
    a cementing (if needs be) a cycle from [from_block] to [to_block]
    (included), trims the floating stores and preserves [to_block] -
    [nb_blocks_to_preserve] blocks (iff these blocks are present or the
    longest suffix otherwise) along with their metadata in the floating
    store potentially having duplicate in the cemented block store.

    After the cementing, {!Cemented_block_store.trigger_gc} will be
    called with the given [history_mode]. When the merging thread
    succeeds, the callback [finalizer] will be called.

    If a merge thread is already occuring, this function will first
    wait for the previous merge to be done.

    {b Warning} For a given [block_store], the caller must wait for
    this function termination before calling it again or it may result
    in concurrent intertwinings causing the cementing to be out of
    order. *)
val merge_stores :
  block_store ->
  ?finalizer:(unit -> unit Lwt.t) ->
  nb_blocks_to_preserve:int ->
  history_mode:History_mode.t ->
  from_block:block_descriptor ->
  to_block:block_descriptor ->
  unit ->
  unit Lwt.t

(** [create ~chain_dir ~genesis_block] instanciate a fresh block_store
    in directory [chain_dir] and stores the [genesis_block] in it. *)
val create :
  chain_dir:string -> genesis_block:Block_repr.t -> block_store tzresult Lwt.t

(** [load ~chain_dir ~genesis_block] load an existing block_store
    present in directory [chain_dir] and stores the [genesis_block] in it.
    Setting [readonly] will prevent new blocks from being stored. *)
val load :
  chain_dir:string ->
  genesis_block:Block_repr.t ->
  readonly:bool ->
  block_store tzresult Lwt.t

(** [close block_store] close [block_store] and every underyling
    opened stores.

    {b Warning} If a merging thread is occuring, it will wait up to 5s
    for its termination before effectively closing the store. *)
val close : block_store -> unit Lwt.t
