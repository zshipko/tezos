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

(** Persistent data manager.

    Every data read/write operation is protected by a mutex preventing
    concurrent data-races. *)

(** The type for the persistent data. *)
type 'a t

(** [read data] accesses the data (cached). *)
val read : 'a t -> 'a Lwt.t

(** [write data value] overwrites the previous [data] with the new
    [value]. *)
val write : 'a t -> 'a -> unit Lwt.t

(** [write_file ~file encoding value] raw writes the [file] with the
    [value]'s [encoding].

    {b Warning} this function should not be used in a normal
    context. Favour the usage of [write]. *)
val write_file : file:string -> 'a Data_encoding.t -> 'a -> unit Lwt.t

(** [update_with data f] {b atomically} updates [data] with the result
    of the application of [f]. Concurrent accesses to the data will
    block until the value is updated.

    {b Warning} Calling read/write in [f] will result in a deadlock. *)
val update_with : 'a t -> ('a -> 'a Lwt.t) -> unit Lwt.t

(** [load ~file encoding] reads from [file] the data and decode it
    using [encoding]. *)
val load : file:string -> 'a Data_encoding.t -> 'a t Lwt.t

(** [init ~file encoding ~initial_data] creates or load an on-disk
    data. If the file already exists, then the data is read from the
    file. Otherwise, [initial_data] is used. *)
val init : file:string -> 'a Data_encoding.t -> initial_data:'a -> 'a t Lwt.t
