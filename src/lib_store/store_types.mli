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

(** {1 Global types used in the store library} *)

(** The type used to describe a block pointer i.e. its hash and level. *)
type block_descriptor = Block_hash.t * int32

(** Encoding for {block_descriptor}. *)
val block_descriptor_encoding : (Block_hash.t * int32) Data_encoding.t

(** The type used to store an invalid block's value. We only retain
    the level and the errors encountered during validation.
    These values should be indexed by the block's hash. *)
type invalid_block = {level : int32; errors : Error_monad.error list}

(** Encoding for {invalid_block}. *)
val invalid_block_encoding : invalid_block Data_encoding.t

(** Module {Protocol_levels} represents an association map of protocol
   levels to corresponding blocks which supposedly activate new
   protocols. *)
module Protocol_levels : sig
  include Map.S with type key = int

  (** The type representing a subset of the commit informations. These
      are used to easily check that a given [Context_hash.t],
      with the associated context not present on disk, is consistent.
      It is used to verify that an announced protocol is indeed the one
      that was commited on disk. Fields are:
      - [author] is the commit's author;
      - [message] is the commit's message;
      - [test_chain_status] is the status of the test chain at
        commit time;
      - [data_merkle_root] is the merkle root of the context's data
        main node;
      - [parents_contexts] are the context hashes of this commit's
        parents.

      This structure should be populated with the result of
      {Tezos_storage.Context.retrieve_commit_info} and the consistency
      check is done by
      {Tezos_storage.Context.check_protocol_commit_consistency}
  *)
  type commit_info = {
    author : string;
    message : string;
    test_chain_status : Test_chain_status.t;
    data_merkle_root : Context_hash.t;
    parents_contexts : Context_hash.t list;
  }

  (** Encoding for {commit_info}. *)
  val commit_info_encoding : commit_info Data_encoding.t

  (** The type for activation blocks.

      {b WARNING.} Commit informations are optional to allow
      retro-compatibility: the LMDB legacy store does not contain such
      informations and thus populating the protocol levels' map while
      upgrading would prevent us from storing an activation block which
      is used to retrieve the [protocol] to load and therefore being
      unable to decode stored blocks. In the future, when a sufficient
      number of  nodes have fully migrated, we can stitch the missing
      commit informations by hard-coding them and allowing use to remove
      the option. *)
  type activation_block = {
    block : block_descriptor;
    protocol : Protocol_hash.t;
    commit_info : commit_info option;
  }

  (** Encoding for the protocol level's association map. *)
  val encoding : activation_block t Data_encoding.t
end
