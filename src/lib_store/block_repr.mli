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

(** Block representation effectively stored on disk and its accessors. *)

(** {1 Type definitions and encodings} *)

(** The type for the effective [contents] of a block is its header and the
    [operations] it contains *)
type contents = {header : Block_header.t; operations : Operation.t list list}

(** The type for a block's [metadata] stored on disk. This representation is
    tightly linked to {!Tezos_validation.Block_validation.result} which
    also has a strong dependency to
    {!Tezos_protocol_environment.validation_result}.

    Some fields exposed by {!Tezos_validation.Block_validation.result}
    are unnecessary hence the lack of direct link. *)
type metadata = {
  message : string option;
  max_operations_ttl : int;
  last_allowed_fork_level : Int32.t;
  block_metadata : Bytes.t;
  operations_metadata : Bytes.t list list;
}

(** The type for a [block] stored on disk.

    The [hash] of the block is
    also stored to improve efficiency by not forcing the user to hash
    the header.  This also allows to store fake hashes (e.g. sandbox's
    genesis blocks) but should be prevented by the API.

    The [metadata] might not be present. The mutability flag allows
    users to re-use the same structure to store freshly loaded
    metadata.
*)
type block = {
  hash : Block_hash.t;
  contents : contents;
  mutable metadata : metadata option;
}

type t = block

(** Encoding for {!contents}. *)
val contents_encoding : contents Data_encoding.t

(** Encoding for {!metadata}. *)
val metadata_encoding : metadata Data_encoding.t

(** Encoding for {!t} (and {!block}).

    {b Important} An encoded block is prefixed by 4 bytes of the
    length of the data before the data. This is the case with
    [Data_encoding.dynamic_size ~kind:`Uint30] encodings. This will
    be expected to be present to improve the store efficiency.
*)
val encoding : t Data_encoding.t

(** [pp_json] pretty-print a block as JSON. *)
val pp_json : Format.formatter -> t -> unit

(** {1 Accessors} *)

(** [hash block] returns the stored [block]'s hash. It is not
    guarenteed to be the same one as
    [Block_header.hash (header block)]. *)
val hash : t -> Block_hash.t

(** [operations block] returns the list of list of operations
    contained in the [block]. *)
val operations : t -> Operation.t list list

(** {2 Block header accessors} *)

val header : t -> Block_header.t

val shell_header : t -> Block_header.shell_header

val level : t -> Int32.t

val proto_level : t -> int

val predecessor : t -> Tezos_crypto.Block_hash.t

val timestamp : t -> Tezos_base.Time.Protocol.t

val validation_passes : t -> int

val fitness : t -> Tezos_base.Fitness.t

val context : t -> Tezos_crypto.Context_hash.t

val protocol_data : t -> Bytes.t

(** {2 Metadata accesors} *)

val metadata : t -> metadata option

val message : metadata -> string option

val max_operations_ttl : metadata -> int

val last_allowed_fork_level : metadata -> Int32.t

val block_metadata : metadata -> Bytes.t

val operations_metadata : metadata -> Bytes.t list list

(** {1 Utility functions} *)

(** [check_block_consistency ~genesis_hash ?pred_block block] checks
    that the stored data is consistent:

    - Does the [hash] stored equals the result of [Block_header.hash]
      of its header and, if not, is this the [genesis_hash] ?
    - Is the [block] a successor of [pred_block] with regards to its
      level and its predecessor's hash ?
*)
val check_block_consistency :
  ?genesis_hash:Block_hash.t -> ?pred_block:t -> t -> unit tzresult Lwt.t

(** [read_next_block fd] reads from [fd] and decode the next block
    found in the descriptor moving the [fd]'s offset in doing so. This
    returns the binary length of the encoded block along with the
    decoded block.  *)
val read_next_block : Lwt_unix.file_descr -> (t * int) Lwt.t

(** Same as [read_next_block fd] but returns None if there was an error. *)
val read_next_block_opt : Lwt_unix.file_descr -> (t * int) option Lwt.t
