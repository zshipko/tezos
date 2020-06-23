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

type floating_kind = Naming.floating_kind = RO | RW | RW_TMP | RO_TMP

type t = {
  floating_block_index : Floating_block_index.t;
  filename : string;
  fd : Lwt_unix.file_descr;
  kind : floating_kind;
  scheduler : Lwt_idle_waiter.t;
}

let floating_blocks_log_size = 100_000

open Floating_block_index.Block_info

let kind {kind; _} = kind

let mem floating_store hash =
  Lwt_idle_waiter.task floating_store.scheduler (fun () ->
      Lwt.return
        (Floating_block_index.mem floating_store.floating_block_index hash))

let find_predecessors floating_store hash =
  Lwt_idle_waiter.task floating_store.scheduler (fun () ->
      try
        let {predecessors; _} =
          Floating_block_index.find floating_store.floating_block_index hash
        in
        Lwt.return_some predecessors
      with Not_found -> Lwt.return_none)

let read_block floating_store hash =
  Lwt_idle_waiter.task floating_store.scheduler (fun () ->
      try
        let {offset; _} =
          Floating_block_index.find floating_store.floating_block_index hash
        in
        (* Read dynamic length prefix *)
        let length_bytes = Bytes.create 4 in
        Lwt_utils_unix.read_bytes
          ~file_offset:offset
          ~pos:0
          ~len:4
          floating_store.fd
          length_bytes
        >>= fun () ->
        let length = Data_encoding.(Binary.of_bytes_exn int31 length_bytes) in
        let block_bytes = Bytes.extend length_bytes 0 length in
        Lwt_utils_unix.read_bytes
          ~file_offset:(offset + 4)
          ~pos:4
          ~len:length
          floating_store.fd
          block_bytes
        >>= fun () ->
        let block =
          Data_encoding.Binary.of_bytes_exn Block_repr.encoding block_bytes
        in
        Lwt.return_some block
      with Not_found -> Lwt.return_none)

let locked_write_block floating_store ~offset ~block ~predecessors =
  ( match Data_encoding.Binary.to_bytes_opt Block_repr.encoding block with
  | None ->
      fail (Cannot_encode_block block.Block_repr.hash)
  | Some bytes ->
      return bytes )
  >>=? fun block_bytes ->
  let block_length = Bytes.length block_bytes in
  Lwt_utils_unix.write_bytes
    ~pos:0
    ~len:block_length
    floating_store.fd
    block_bytes
  >>= fun () ->
  Floating_block_index.replace
    floating_store.floating_block_index
    block.Block_repr.hash
    {offset; predecessors} ;
  return block_length

let append_block floating_store predecessors (block : Block_repr.t) =
  Lwt_idle_waiter.force_idle floating_store.scheduler (fun () ->
      Lwt_unix.lseek floating_store.fd 0 Unix.SEEK_END
      >>= fun offset ->
      locked_write_block floating_store ~offset ~block ~predecessors
      >>= fun _written_len ->
      Floating_block_index.flush floating_store.floating_block_index ;
      Lwt.return_unit)

let append_all floating_store
    (blocks : (Block_hash.t list * Block_repr.t) list) =
  Lwt_idle_waiter.force_idle floating_store.scheduler (fun () ->
      Lwt_unix.lseek floating_store.fd 0 Unix.SEEK_END
      >>= fun eof_offset ->
      Error_monad.fold_left_s
        (fun offset (predecessors, block) ->
          locked_write_block floating_store ~offset ~block ~predecessors
          >>=? fun written_len -> return (offset + written_len))
        eof_offset
        blocks
      >>=? fun _last_offset ->
      Floating_block_index.flush floating_store.floating_block_index ;
      return_unit)

let iter_s_raw_fd f fd =
  Lwt_unix.lseek fd 0 Unix.SEEK_END
  >>= fun eof_offset ->
  Lwt_unix.lseek fd 0 Unix.SEEK_SET
  >>= fun _ ->
  let rec loop nb_bytes_left =
    if nb_bytes_left = 0 then return_unit
    else
      Block_repr.read_next_block_opt fd
      >>= function
      | None ->
          return_unit
      | Some (block, length) ->
          f block >>=? fun () -> loop (nb_bytes_left - length)
  in
  loop eof_offset

let iter_with_pred_s_raw_fd f fd block_index =
  iter_s_raw_fd
    (fun block ->
      let {predecessors; _} =
        Floating_block_index.find block_index block.hash
      in
      f (block, predecessors))
    fd

let folder f floating_store =
  Lwt_idle_waiter.task floating_store.scheduler (fun () ->
      (* We open a new fd *)
      let (flags, perms) = ([Unix.O_CREAT; O_RDONLY; O_CLOEXEC], 0o444) in
      Lwt_unix.openfile floating_store.filename flags perms
      >>= fun fd ->
      Lwt.finalize
        (fun () -> f fd)
        (fun () -> Lwt_utils_unix.safe_close fd >>= fun _ -> Lwt.return_unit))

let fold_left_s f e floating_store =
  folder
    (fun fd ->
      let acc = ref e in
      iter_s_raw_fd
        (fun block ->
          f !acc block
          >>=? fun new_acc ->
          acc := new_acc ;
          return_unit)
        fd
      >>=? fun () -> return !acc)
    floating_store

let fold_left_with_pred_s f e floating_store =
  folder
    (fun fd ->
      let acc = ref e in
      iter_with_pred_s_raw_fd
        (fun (b, preds) ->
          f !acc (b, preds)
          >>=? fun new_acc ->
          acc := new_acc ;
          return_unit)
        fd
        floating_store.floating_block_index
      >>=? fun () -> return !acc)
    floating_store

(* Iter sequentially on every blocks in the file *)
let iter_s f floating_store = fold_left_s (fun () e -> f e) () floating_store

let iter_with_pred_s f floating_store =
  fold_left_with_pred_s (fun () e -> f e) () floating_store

let init ~chain_dir ~readonly kind =
  let (flag, perms) =
    (* Only RO is readonly: when we open RO_TMP, we actually write in it. *)
    if kind = Naming.RO && readonly then (Unix.O_RDONLY, 0o444)
    else (Unix.O_RDWR, 0o644)
  in
  let filename = Naming.(chain_dir // floating_blocks kind) in
  Lwt_unix.openfile filename [Unix.O_CREAT; O_CLOEXEC; flag] perms
  >>= fun fd ->
  let floating_block_index =
    Floating_block_index.v
      ~log_size:floating_blocks_log_size
      ~readonly
      Naming.(chain_dir // floating_block_index kind)
  in
  let scheduler = Lwt_idle_waiter.create () in
  Lwt.return {floating_block_index; fd; filename; kind; scheduler}

let close {floating_block_index; fd; scheduler; _} =
  Lwt_idle_waiter.force_idle scheduler (fun () ->
      Floating_block_index.close floating_block_index ;
      Lwt_utils_unix.safe_close fd >>= fun _ -> Lwt.return_unit)
