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

type chain_config = {
  history_mode : History_mode.t;
  genesis : Genesis.t;
  expiration : Time.Protocol.t option;
}

type t = chain_config

let initial_encoding : chain_config Data_encoding.With_version.t =
  let open Data_encoding in
  With_version.first_version
    (conv
       (fun {history_mode; genesis; expiration} ->
         (history_mode, genesis, expiration))
       (fun (history_mode, genesis, expiration) ->
         {history_mode; genesis; expiration})
       (obj3
          (req "history_mode" History_mode.encoding)
          (req "genesis" Genesis.encoding)
          (varopt "expiration" Time.Protocol.encoding)))

let encoding : chain_config Data_encoding.t =
  Data_encoding.With_version.encoding ~name:"store.config" initial_encoding

let write ~chain_dir chain_config =
  (* FIXME handle I/O errors *)
  let config_json = Data_encoding.Json.construct encoding chain_config in
  Lwt_utils_unix.Json.write_file
    Naming.(chain_dir // chain_config_file)
    config_json

let load ~chain_dir =
  (* FIXME handle I/O errors *)
  Lwt_utils_unix.Json.read_file Naming.(chain_dir // chain_config_file)
  >>=? fun config -> return (Data_encoding.Json.destruct encoding config)
