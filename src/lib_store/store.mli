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

(**

 ****************************************************************

   Checkpoint

   Block descriptor (hash x level) designating a block that must be
   part of the chain.

   - If may not be yet known (i.e. in the future), the store will then
   accept all blocks until the checkpoint level is reached. Then, if a
   block's hash with this specific level is not the checkpoint's hash,
   the store will not consider it as a valid block. The store may
   stall if the common ancestor of the head and the checkpoint is
   below the savepoint (see below).

   - If the checkpoint has been reached, the store will only accept
   blocks that have the checkpoint as ancestor.

   - The checkpoint is updated periodically such that the following
   invariant holds:

   [checkpoint.level >= head.last_allowed_fork_level]

   The checkpoint can be manually set as long as the invariant still
   holds. If the checkpoint has not been manually set, it will
   designate the block at level [head.last_allowed_fork_level].

 ****************************************************************

   Savepoint

   Block descriptor (hash x level) that designate the lowest block
   that has not been pruned:

   [is_stored(block) ^ has_metadata(block) => block.level >=
   savepoint.level]

   On Full and Rolling history mode, the savepoint will be
   periodically updated at each store merge which happens when:

   [pred(head).last_allowed_fork_level < head.last_allowed_fork_level]

   On Archive history mode: [savepoint = genesis]

 ****************************************************************

   Caboose

   Block descriptor (hash x level) that designate the lowest block
   that we stored but, if the history mode is *not* Archive, it might
   not contain its metadata depending on the current savepoint:

   [is_stored(block) => block.level >= caboose.level]

   On Archive and Full history mode: [caboose = genesis]

   On Rolling history mode, the caboose will be periodically updated
   at each store merge (see above) such that:

   [caboose.level =
       min (savepoint.level,
            lowest_full_block.level - lowest_full_block.max_op_ttl)]

   with [lowest_full_block] the lowest level block that has metadata.

 ****************************************************************

*)

open Store_types

type t

type store = t

type chain_store

type testchain

module Block : sig
  type t

  type block = t

  type metadata = {
    message : string option;
    max_operations_ttl : int;
    last_allowed_fork_level : Int32.t;
    block_metadata : Bytes.t;
    operations_metadata : Bytes.t list list;
  }

  (** Unsafe *)
  val repr : t -> Block_repr.t

  (** Unsafe *)
  val of_repr : Block_repr.t -> t

  (** Unsafe *)
  val repr_metadata : metadata -> Block_repr.metadata

  (** Unsafe *)
  val of_repr_metadata : Block_repr.metadata -> metadata

  val equal : block -> block -> bool

  val is_known_valid : chain_store -> Block_hash.t -> bool Lwt.t

  val is_known_invalid : chain_store -> Block_hash.t -> bool Lwt.t

  val is_known : chain_store -> Block_hash.t -> bool Lwt.t

  val is_genesis : chain_store -> Block_hash.t -> bool

  val block_validity :
    chain_store -> Block_hash.t -> Block_locator.validity Lwt.t

  val read_block :
    chain_store -> ?distance:int -> Block_hash.t -> block tzresult Lwt.t

  val read_block_opt :
    chain_store -> ?distance:int -> Block_hash.t -> block option Lwt.t

  val read_block_by_level : chain_store -> int32 -> block tzresult Lwt.t

  val read_block_by_level_opt : chain_store -> int32 -> block option Lwt.t

  val read_block_metadata :
    ?distance:int -> chain_store -> Block_hash.t -> metadata option Lwt.t

  val get_block_metadata : chain_store -> block -> metadata tzresult Lwt.t

  val get_block_metadata_opt : chain_store -> block -> metadata option Lwt.t

  val read_predecessor : chain_store -> block -> block tzresult Lwt.t

  val read_predecessor_opt : chain_store -> block -> block option Lwt.t

  val read_ancestor_hash :
    chain_store -> distance:int -> Block_hash.t -> Block_hash.t option Lwt.t

  val read_predecessor_of_hash_opt :
    chain_store -> Block_hash.t -> block option Lwt.t

  val read_predecessor_of_hash :
    chain_store -> Block_hash.t -> block tzresult Lwt.t

  val store_block :
    chain_store ->
    block_header:Block_header.t ->
    block_header_metadata:bytes ->
    operations:Operation.t list list ->
    operations_metadata:bytes list list ->
    context_hash:Context_hash.t ->
    message:string option ->
    max_operations_ttl:int ->
    last_allowed_fork_level:Int32.t ->
    block option tzresult Lwt.t

  val store_block_metadata : chain_store -> (t * metadata) list -> unit Lwt.t

  val context_exn : chain_store -> block -> Context.t Lwt.t

  val context_opt : chain_store -> block -> Context.t option Lwt.t

  val context : chain_store -> block -> Context.t tzresult Lwt.t

  val context_exists : chain_store -> block -> bool Lwt.t

  val protocol_hash : chain_store -> block -> Protocol_hash.t tzresult Lwt.t

  val protocol_hash_exn : chain_store -> block -> Protocol_hash.t Lwt.t

  val compute_locator :
    chain_store ->
    ?size:int ->
    block ->
    Block_locator.seed ->
    Block_locator.t Lwt.t

  (** [filter_known_suffix chain_store locator] trims the locator by
      removing steps that are already present in [chain_store].

      It either returns:
      - [Some (header, hist)] when we find a valid block, where [hist]
        is the unknown prefix, ending with the first valid block found.
      - [Some (header, hist)] when we don't find any block known valid nor invalid
        and the node runs in full or rolling mode. In this case
        [(h, hist)] is the given [locator].
      - [None] when the node runs in archive history mode and
        we find an invalid block or no valid block in the [locator].
      - [None] when the node runs in full or rolling mode and we find
        an invalid block in the [locator]. *)
  val filter_known_suffix :
    chain_store -> Block_locator.t -> Block_locator.t option Lwt.t

  val read_invalid_block_opt :
    chain_store -> Block_hash.t -> invalid_block option Lwt.t

  val read_invalid_blocks : chain_store -> invalid_block Block_hash.Map.t Lwt.t

  val mark_invalid :
    chain_store ->
    Block_hash.t ->
    level:int32 ->
    error list ->
    unit tzresult Lwt.t

  val unmark_invalid : chain_store -> Block_hash.t -> unit Lwt.t

  val descriptor : block -> block_descriptor

  val hash : block -> Block_hash.t

  val header : block -> Block_header.t

  val operations : block -> Operation.t list list

  val shell_header : block -> Block_header.shell_header

  val level : block -> int32

  val proto_level : block -> int

  val predecessor : block -> Block_hash.t

  val timestamp : block -> Time.Protocol.t

  val validation_passes : block -> int

  val fitness : block -> Fitness.t

  val context_hash : block -> Context_hash.t

  val protocol_data : block -> bytes

  val message : metadata -> string option

  val max_operations_ttl : metadata -> int

  val last_allowed_fork_level : metadata -> int32

  val block_metadata : metadata -> bytes

  val operations_metadata : metadata -> bytes list list

  val operations_path :
    block -> int -> Operation.t list * Operation_list_list_hash.path

  val operations_hashes_path :
    block -> int -> Operation_hash.t list * Operation_list_list_hash.path

  val all_operation_hashes : block -> Operation_hash.t list list
end

module Chain : sig
  type nonrec chain_store = chain_store

  type t = chain_store

  val global_store : chain_store -> store

  val chain_id : chain_store -> Chain_id.t

  val history_mode : chain_store -> History_mode.t

  (* Must be used with care. Invariant are node handled here *)
  val set_history_mode : chain_store -> History_mode.t -> unit Lwt.t

  val genesis : chain_store -> Genesis.t

  val genesis_block : chain_store -> Block.t Lwt.t

  val create_genesis_block : genesis:Genesis.t -> Context_hash.t -> Block.t

  val expiration : chain_store -> Time.Protocol.t option

  val current_head : chain_store -> Block.t Lwt.t

  val checkpoint : chain_store -> block_descriptor Lwt.t

  val savepoint : chain_store -> block_descriptor Lwt.t

  val set_savepoint : chain_store -> block_descriptor -> unit tzresult Lwt.t

  val caboose : chain_store -> block_descriptor Lwt.t

  val set_caboose : chain_store -> block_descriptor -> unit tzresult Lwt.t

  val mempool : chain_store -> Mempool.t Lwt.t

  val set_mempool :
    chain_store -> head:Block_hash.t -> Mempool.t -> unit tzresult Lwt.t

  val live_blocks :
    chain_store -> (Block_hash.Set.t * Operation_hash.Set.t) Lwt.t

  val compute_live_blocks :
    chain_store ->
    block:Block.t ->
    (Block_hash.Set.t * Operation_hash.Set.t) tzresult Lwt.t

  val re_store :
    chain_store ->
    head:Block.t ->
    checkpoint:block_descriptor ->
    savepoint:block_descriptor ->
    caboose:block_descriptor ->
    unit tzresult Lwt.t

  (* Returns the previous head *)
  val set_head : chain_store -> Block.t -> Block.t tzresult Lwt.t

  val known_heads : chain_store -> int32 Block_hash.Map.t Lwt.t

  val is_ancestor :
    t -> head:block_descriptor -> ancestor:block_descriptor -> bool Lwt.t

  val is_in_chain : t -> block_descriptor -> bool Lwt.t

  val is_valid_for_checkpoint :
    chain_store ->
    checkpoint:block_descriptor ->
    block_descriptor ->
    bool Lwt.t

  val is_acceptable_block : chain_store -> block_descriptor -> bool Lwt.t

  val best_known_head_for_checkpoint :
    chain_store -> checkpoint:block_descriptor -> Block.t Lwt.t

  val set_checkpoint : chain_store -> block_descriptor -> unit tzresult Lwt.t

  val testchain_status :
    chain_store -> Block.t -> (Test_chain_status.t * Block_hash.t option) Lwt.t

  val testchain : chain_store -> testchain option Lwt.t

  val testchain_forked_block : testchain -> Block_hash.t

  val testchain_store : testchain -> chain_store

  val load_testchain :
    chain_store -> chain_id:Chain_id.t -> testchain option tzresult Lwt.t

  val fork_testchain :
    chain_store ->
    testchain_id:Chain_id.t ->
    forked_block:Block.t ->
    genesis_hash:Block_hash.t ->
    genesis_header:Block_header.t ->
    test_protocol:Protocol_hash.t ->
    expiration:Time.Protocol.t ->
    testchain tzresult Lwt.t

  val shutdown_testchain : chain_store -> unit tzresult Lwt.t

  val set_protocol_level :
    chain_store -> int -> Block.block * Protocol_hash.t -> unit tzresult Lwt.t

  val may_update_protocol_level :
    chain_store -> int -> Block.block * Protocol_hash.t -> unit tzresult Lwt.t

  val get_commit_info :
    Context.index -> Block_header.t -> commit_info tzresult Lwt.t

  val find_protocol_level :
    chain_store ->
    int ->
    (block_descriptor * Protocol_hash.t * commit_info) option Lwt.t

  val all_protocol_levels :
    chain_store ->
    (block_descriptor * Protocol_hash.t * commit_info) Protocol_levels.t Lwt.t

  val watcher : chain_store -> Block.t Lwt_stream.t * Lwt_watcher.stopper

  val get_rpc_directory :
    chain_store ->
    Block.t ->
    (chain_store * Block.t) RPC_directory.t option Lwt.t

  val set_rpc_directory :
    chain_store ->
    Protocol_hash.t ->
    (chain_store * Block.t) RPC_directory.t ->
    unit Lwt.t
end

module Protocol : sig
  val is_protocol_stored : store -> Protocol_hash.t -> bool

  val all_stored_protocols : store -> Protocol_hash.Set.t

  val read_protocol : store -> Protocol_hash.t -> Protocol.t option Lwt.t

  (* Returns Some if correctly stored or None if already stored *)
  val store_protocol :
    store -> Protocol_hash.t -> Protocol.t -> Protocol_hash.t option Lwt.t

  (* Returns Some if correctly stored or None if already stored *)
  val store_raw_protocol :
    store -> Protocol_hash.t -> bytes -> Protocol_hash.t option Lwt.t

  val protocol_watcher :
    t -> Protocol_hash.t Lwt_stream.t * Lwt_watcher.stopper
end

module Chain_traversal : sig
  val path : chain_store -> Block.t -> Block.t -> Block.t list option Lwt.t

  val common_ancestor :
    chain_store -> Block.t -> Block.t -> Block.t option Lwt.t

  val new_blocks :
    chain_store ->
    from_block:Block.t ->
    to_block:Block.t ->
    (Block.t * Block.t list) Lwt.t
end

val init :
  ?patch_context:(Context.t -> Context.t tzresult Lwt.t) ->
  ?commit_genesis:(chain_id:Chain_id.t -> Context_hash.t tzresult Lwt.t) ->
  ?history_mode:History_mode.t ->
  ?readonly:bool ->
  store_dir:string ->
  context_dir:string ->
  allow_testchains:bool ->
  Genesis.t ->
  t tzresult Lwt.t

val main_chain_store : store -> chain_store

val directory : store -> string

val context_index : store -> Context.index

val allow_testchains : store -> bool

val get_chain_store : store -> Chain_id.t -> chain_store tzresult Lwt.t

val get_chain_store_opt : store -> Chain_id.t -> chain_store option Lwt.t

val all_chain_stores : store -> chain_store list Lwt.t

val global_block_watcher :
  t -> (chain_store * Block.t) Lwt_stream.t * Lwt_watcher.stopper

val close_store : store -> unit tzresult Lwt.t

val open_for_snapshot_export :
  store_dir:string ->
  context_dir:string ->
  Genesis.t ->
  locked_f:(chain_store * Context.index -> 'a tzresult Lwt.t) ->
  'a tzresult Lwt.t

val restore_from_snapshot :
  ?notify:(unit -> unit Lwt.t) ->
  store_dir:string ->
  context_index:Context.index ->
  genesis:Genesis.t ->
  genesis_context_hash:Context_hash.t ->
  floating_blocks_stream:Block_repr.block Lwt_stream.t ->
  new_head_with_metadata:Block_repr.block ->
  protocol_levels:(block_descriptor * Protocol_hash.t * commit_info)
                  Protocol_levels.t ->
  history_mode:History_mode.t ->
  unit tzresult Lwt.t

val restore_from_legacy_snapshot :
  ?notify:(unit -> unit Lwt.t) ->
  store_dir:string ->
  context_index:Context.index ->
  genesis:Genesis.t ->
  genesis_context_hash:Context_hash.t ->
  floating_blocks_stream:Block_repr.block Lwt_stream.t ->
  new_head_with_metadata:Block_repr.block ->
  partial_protocol_levels:(int32 * Protocol_hash.t * commit_info) list ->
  history_mode:History_mode.t ->
  unit tzresult Lwt.t

val cement_blocks_chunk :
  chain_store -> Block_repr.t list -> write_metadata:bool -> unit Lwt.t

(**/*)

(****************** For testing purposes only *****************)

val unsafe_get_block_store : chain_store -> Block_store.block_store
