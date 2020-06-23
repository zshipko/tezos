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

open Store_errors

type 'a t = {
  mutable cache : 'a;
  file : string;
  encoding : 'a Data_encoding.t;
  scheduler : Lwt_idle_waiter.t;
}

let read_file encoding file =
  Lwt.try_bind
    (fun () -> Lwt_utils_unix.read_file file)
    (fun str ->
      let bytes = Bytes.unsafe_of_string str in
      Lwt.return (Data_encoding.Binary.of_bytes_opt encoding bytes))
    (fun _ -> Lwt.return_none)

let read v = Lwt_idle_waiter.task v.scheduler (fun () -> Lwt.return v.cache)

let write_file ~file encoding data =
  let bytes = Data_encoding.Binary.to_bytes_exn encoding data in
  let tmp_filename = file ^ "_tmp" in
  (* Write in a new temporary file then swap the files to avoid
     inconsistent states after loading *)
  Lwt_unix.openfile
    tmp_filename
    [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC]
    0o644
  >>= fun fd ->
  Lwt_utils_unix.write_bytes fd bytes
  >>= fun () ->
  Lwt_utils_unix.safe_close fd >>= fun _ -> Lwt_unix.rename tmp_filename file

let write v data =
  Lwt_idle_waiter.force_idle v.scheduler (fun () ->
      if v.cache = data then Lwt.return_unit
      else (
        v.cache <- data ;
        write_file ~file:v.file v.encoding data ))

let create ~file encoding data =
  let file = file in
  let scheduler = Lwt_idle_waiter.create () in
  write_file ~file encoding data
  >>= fun () -> Lwt.return {cache = data; file; encoding; scheduler}

let update_with v f =
  Lwt_idle_waiter.force_idle v.scheduler (fun () ->
      f v.cache
      >>= fun new_data ->
      if v.cache = new_data then Lwt.return_unit
      else (
        v.cache <- new_data ;
        write_file ~file:v.file v.encoding new_data ))

let load ~file encoding =
  read_file encoding file
  >>= function
  | Some cache ->
      let scheduler = Lwt_idle_waiter.create () in
      return {cache; file; encoding; scheduler}
  | None ->
      fail (Missing_stored_data file)

let init ~file encoding ~initial_data =
  Lwt_unix.file_exists file
  >>= function
  | true ->
      load ~file encoding
  | false ->
      create ~file encoding initial_data >>= return
