(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
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

open Error_monad

(** [default_net_timeout] is the default timeout used by functions in
   this library which admit a timeout value, i.e. [read_bytes],
   [Socket.connect], [Socket.recv]. *)
val default_net_timeout : Ptime.Span.t ref

(** [read_bytes ?timeout ?file_offset ?pos ?len fd buf] reads
    [len-pos] bytes from [fd] into [bytes]. If [file_offset] is given,
    {!Lwt_unix.pread} will be used instead of {!Lwt_unix.read}.

    @raise Lwt_unix.Timeout if the operation failed to finish within a
    [timeout] time span. *)
val read_bytes :
  ?timeout:Ptime.Span.t ->
  ?file_offset:int ->
  ?pos:int ->
  ?len:int ->
  Lwt_unix.file_descr ->
  bytes ->
  unit Lwt.t

val write_string :
  ?pos:int -> ?len:int -> Lwt_unix.file_descr -> string -> unit Lwt.t

(** [write_bytes ?file_offset ?pos ?len fd buf] write [len-pos] bytes
    from [bytes] to [fd]. If [file_offset] is given, {!Lwt_unix.pwrite}
    will be used instead of {!Lwt_unix.write}.

    @raise Lwt_unix.Timeout if the operation failed to finish within a
    [timeout] time span. *)
val write_bytes :
  ?file_offset:int ->
  ?pos:int ->
  ?len:int ->
  Lwt_unix.file_descr ->
  Bytes.t ->
  unit Lwt.t

val remove_dir : string -> unit Lwt.t

val create_dir : ?perm:int -> string -> unit Lwt.t

(** [copy_dir ?perm src dst] copies the content of directory [src] in
    a fresh directory [dst] created with [perm] (0o755 by default).

    @raise [Unix_error (ENOTDIR, _, _)] if the given file is not a
    directory. *)
val copy_dir : ?perm:int -> string -> string -> unit Lwt.t

val read_file : string -> string Lwt.t

val copy_file : src:string -> dst:string -> unit Lwt.t

val create_file : ?perm:int -> string -> string -> unit Lwt.t

val with_tempdir : string -> (string -> 'a Lwt.t) -> 'a Lwt.t

val safe_close : Lwt_unix.file_descr -> unit tzresult Lwt.t

val getaddrinfo :
  passive:bool ->
  node:string ->
  service:string ->
  (Ipaddr.V6.t * int) list Lwt.t

(** [getpass ()] reads a password from stdio while setting-up the
    terminal to not display the password being typed. *)
val getpass : unit -> string

module Json : sig
  (** Loads a JSON file in memory *)
  val read_file : string -> Data_encoding.json tzresult Lwt.t

  (** (Over)write a JSON file from in memory data *)
  val write_file : string -> Data_encoding.json -> unit tzresult Lwt.t
end

val retry :
  ?log:('error -> unit Lwt.t) ->
  ?n:int ->
  ?sleep:float ->
  (unit -> ('a, 'error) result Lwt.t) ->
  ('a, 'error) result Lwt.t

(** [display_progress ?every ?out ~pp_print_step f] calls
    [pp_print_step] when the first argument of [f] is called and
    increment the number of step which will be given to
    [pp_print_step].

    If [every] is passed, it is instead displayed whenever [nb_step
    mod every = 0] (defaults to 1). Each time [pp_print_step] is
    called, the previous output will be erased and replaced by the new
    output.

    [pp_print_step] must only write on a single-line with no carriage
    return. *)
val display_progress :
  ?every:int ->
  ?out:Lwt_unix.file_descr ->
  pp_print_step:(Format.formatter -> int -> unit) ->
  ((unit -> unit Lwt.t) -> 'a Lwt.t) ->
  'a Lwt.t
