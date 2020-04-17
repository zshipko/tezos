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

(* Cemented files overlay:

   | <n> x <offset (4 bytes)> | <n> x <blocks> |

   <offset> is an absolute offset in the file.
   <blocks> are prefixed by 4 bytes of length

*)
(* On-disk index of block's hashes to level *)
module Cemented_block_level_index = Index_unix.Make (Block_key) (Block_level)

(* On-disk index of block's level to hash *)
module Cemented_block_hash_index = Index_unix.Make (Block_level) (Block_key)

type cemented_blocks_file = {
  start_level : int32;
  end_level : int32;
  filename : string;
}

type t = {
  cemented_blocks_dir : string;
  cemented_blocks_metadata_dir : string;
  cemented_block_level_index : Cemented_block_level_index.t;
  cemented_block_hash_index : Cemented_block_hash_index.t;
  mutable cemented_blocks_files : cemented_blocks_file array;
}

let cemented_blocks_dir {cemented_blocks_dir; _} = cemented_blocks_dir

let cemented_blocks_files {cemented_blocks_files; _} = cemented_blocks_files

let default_index_log_size = 100_000

let default_compression_level = 9

let create ~cemented_blocks_dir =
  Lwt_utils_unix.create_dir cemented_blocks_dir
  >>= fun () ->
  let cemented_blocks_metadata_dir =
    Naming.(cemented_blocks_dir // cemented_blocks_metadata_directory)
  in
  Lwt_utils_unix.create_dir cemented_blocks_metadata_dir
  >>= fun () ->
  let cemented_block_level_index =
    Cemented_block_level_index.v
      ~log_size:default_index_log_size
      Naming.(cemented_blocks_dir // cemented_block_level_index_directory)
  in
  let cemented_block_hash_index =
    Cemented_block_hash_index.v
      ~log_size:default_index_log_size
      Naming.(cemented_blocks_dir // cemented_block_hash_index_directory)
  in
  (* Empty table at first *)
  let cemented_blocks_files =
    Array.make 0 {start_level = -1l; end_level = -1l; filename = ""}
  in
  let cemented_store =
    {
      cemented_blocks_dir;
      cemented_blocks_metadata_dir;
      cemented_block_level_index;
      cemented_block_hash_index;
      cemented_blocks_files;
    }
  in
  Lwt.return cemented_store

let load_table ~cemented_blocks_dir =
  fail_unless
    ( Sys.file_exists cemented_blocks_dir
    && Sys.is_directory cemented_blocks_dir )
    (Exn
       (Invalid_argument
          (Format.sprintf
             "load_table: directory %s does not exist"
             cemented_blocks_dir)))
  >>=? fun () ->
  Lwt_unix.opendir cemented_blocks_dir
  >>= fun dir_handle ->
  let rec loop acc =
    Lwt.catch
      (fun () ->
        Lwt_unix.readdir dir_handle
        >>= fun filename ->
        let levels = String.split_on_char '_' filename in
        match levels with
        | [start_level; end_level] -> (
            let start_level_opt = Int32.of_string_opt start_level in
            let end_level_opt = Int32.of_string_opt end_level in
            match (start_level_opt, end_level_opt) with
            | (Some start_level, Some end_level) ->
                loop ({start_level; end_level; filename} :: acc)
            | _ ->
                loop acc )
        | _ ->
            loop acc)
      (function End_of_file -> Lwt.return acc | _exn -> loop acc)
  in
  Lwt.finalize (fun () -> loop []) (fun () -> Lwt_unix.closedir dir_handle)
  >>= fun cemented_files_list ->
  let cemented_files_array = Array.of_list cemented_files_list in
  Array.sort
    (fun {start_level; _} {start_level = start_level'; _} ->
      Compare.Int32.compare start_level start_level')
    cemented_files_array ;
  return cemented_files_array

let load ~cemented_blocks_dir =
  let cemented_block_level_index =
    Cemented_block_level_index.v
      ~log_size:default_index_log_size
      Naming.(cemented_blocks_dir // cemented_block_level_index_directory)
  in
  let cemented_block_hash_index =
    Cemented_block_hash_index.v
      ~log_size:default_index_log_size
      Naming.(cemented_blocks_dir // cemented_block_hash_index_directory)
  in
  let cemented_blocks_metadata_dir =
    Naming.(cemented_blocks_dir // cemented_blocks_metadata_directory)
  in
  load_table ~cemented_blocks_dir
  >>=? fun cemented_blocks_files ->
  let cemented_store =
    {
      cemented_blocks_dir;
      cemented_blocks_metadata_dir;
      cemented_block_level_index;
      cemented_block_hash_index;
      cemented_blocks_files;
    }
  in
  return cemented_store

let init ~cemented_blocks_dir =
  if Sys.file_exists cemented_blocks_dir then
    fail_unless
      (Sys.is_directory cemented_blocks_dir)
      (Exn
         (Failure
            (Format.sprintf
               "Cemented_block_store.init: file %s is not a directory"
               cemented_blocks_dir)))
    >>=? fun () -> load ~cemented_blocks_dir >>=? fun res -> return res
  else create ~cemented_blocks_dir >>= fun res -> return res

let close cemented_store =
  Cemented_block_level_index.close cemented_store.cemented_block_level_index ;
  Cemented_block_hash_index.close cemented_store.cemented_block_hash_index

let offset_length = 4 (* file offset *)

let offset_encoding = Data_encoding.int31

let find_block_file cemented_store block_level =
  try
    if Compare.Int32.(block_level < 0l) then None
    else
      let length = Array.length cemented_store.cemented_blocks_files in
      let last_interval =
        let {start_level; end_level; _} =
          cemented_store.cemented_blocks_files.(length - 1)
        in
        Int32.(succ (sub end_level start_level))
      in
      (* Heuristic: in most chains, the first cemented blocks are [0_1]
       then real cycles begin [2_4097], .. *)
      let heuristic_initial_pivot =
        match block_level with
        | 0l | 1l ->
            0
        | _ ->
            Compare.Int.min
              (length - 1)
              (1 + Int32.(to_int (div (sub block_level 2l) last_interval)))
      in
      (* Dichotomic based search. Do not repeat the heuristic *)
      let rec loop (inf, sup) pivot =
        if pivot < inf || pivot > sup || inf > sup then None
        else
          let ({start_level; end_level; _} as res) =
            cemented_store.cemented_blocks_files.(pivot)
          in
          if
            Compare.Int32.(
              block_level >= start_level && block_level <= end_level)
          then (* Found *)
            Some res
          else if Compare.Int32.(block_level > end_level) then
            (* Making sure the pivot is strictly increasing *)
            let new_pivot = pivot + max 1 ((sup - pivot) / 2) in
            loop (pivot, sup) new_pivot
          else
            (* Making sure the pivot is strictly decreasing *)
            let new_pivot = pivot - max 1 ((pivot - inf) / 2) in
            loop (inf, pivot) new_pivot
      in
      loop (0, length - 1) heuristic_initial_pivot
  with _ -> None

(* Hypothesis: at least one cemented file and the table is ordered *)
let compute_location cemented_store block_level =
  match find_block_file cemented_store block_level with
  | None ->
      None
  | Some {start_level; filename; _} ->
      Some (filename, Int32.(to_int (sub block_level start_level)))

let is_cemented cemented_store hash =
  try
    Cemented_block_level_index.mem
      cemented_store.cemented_block_level_index
      hash
  with Not_found -> false

let get_cemented_block_level cemented_store hash =
  try
    Some
      (Cemented_block_level_index.find
         cemented_store.cemented_block_level_index
         hash)
  with Not_found -> None

let get_cemented_block_hash cemented_store level =
  try
    Some
      (Cemented_block_hash_index.find
         cemented_store.cemented_block_hash_index
         level)
  with Not_found -> None

let read_block_metadata ?location cemented_store block_level =
  let location =
    match location with
    | Some _ ->
        location
    | None ->
        compute_location cemented_store block_level
  in
  match location with
  | None ->
      None
  | Some (filename, _block_number) -> (
      let metadata_file =
        Naming.(
          cemented_store.cemented_blocks_metadata_dir
          // cemented_metadata_file filename)
      in
      if not (Sys.file_exists metadata_file) then None
      else
        let in_file = Zip.open_in metadata_file in
        try
          let entry = Zip.find_entry in_file (Int32.to_string block_level) in
          let metadata = Zip.read_entry in_file entry in
          Zip.close_in in_file ;
          let metadata = Bytes.unsafe_of_string metadata in
          Some
            (Data_encoding.Binary.of_bytes_exn
               Block_repr.metadata_encoding
               metadata)
        with _ -> Zip.close_in in_file ; None )

let cement_blocks_metadata cemented_store blocks =
  let cemented_metadata_dir = cemented_store.cemented_blocks_metadata_dir in
  Lwt_unix.file_exists cemented_metadata_dir
  >>= (function
        | true ->
            Lwt.return_unit
        | false ->
            Lwt_utils_unix.create_dir cemented_metadata_dir)
  >>= fun () ->
  fail_unless
    (blocks <> [])
    (Exn (Invalid_argument "cement_blocks_metadata: empty list of blocks"))
  >>=? fun () ->
  find_block_file cemented_store (Block_repr.level (List.hd blocks))
  |> function
  | None ->
      failwith "cement_blocks_metadata: given blocks are not cemented"
  | Some {filename; _} ->
      let metadata_file =
        Naming.(cemented_metadata_dir // cemented_metadata_file filename)
      in
      if List.exists (fun block -> Block_repr.metadata block <> None) blocks
      then (
        let out_file = Zip.open_out metadata_file in
        List.iter
          (fun block ->
            let level = Block_repr.level block in
            match Block_repr.metadata block with
            | Some metadata ->
                let metadata_bytes =
                  Data_encoding.Binary.to_bytes_exn
                    Block_repr.metadata_encoding
                    metadata
                in
                Zip.add_entry
                  ~level:default_compression_level
                  (Bytes.unsafe_to_string metadata_bytes)
                  out_file
                  (Int32.to_string level)
            | None ->
                ())
          blocks ;
        Zip.close_out out_file ;
        return_unit )
      else return_unit

let read_block fd block_number =
  Lwt_unix.lseek fd (block_number * offset_length) Unix.SEEK_SET
  >>= fun _ofs ->
  let offset_buffer = Bytes.create offset_length in
  (* We read the (absolute) offset at the position in the offset array *)
  Lwt_utils_unix.read_bytes ~pos:0 ~len:offset_length fd offset_buffer
  >>= fun () ->
  let offset =
    Data_encoding.(Binary.of_bytes_opt offset_encoding offset_buffer)
    |> Option.unopt_assert ~loc:__POS__
  in
  Lwt_unix.lseek fd offset Unix.SEEK_SET
  >>= fun _ofs ->
  (* We move the cursor to the element's position *)
  Block_repr.read_next_block fd >>= fun (block, _len) -> Lwt.return block

let get_lowest_cemented_level cemented_store =
  let nb_cemented_blocks = Array.length cemented_store.cemented_blocks_files in
  if nb_cemented_blocks > 0 then
    Some cemented_store.cemented_blocks_files.(0).start_level
  else (* No cemented blocks*)
    None

let get_highest_cemented_level cemented_store =
  let nb_cemented_blocks = Array.length cemented_store.cemented_blocks_files in
  if nb_cemented_blocks > 0 then
    Some
      cemented_store.cemented_blocks_files.(nb_cemented_blocks - 1).end_level
  else (* No cemented blocks*)
    None

let get_cemented_block_by_level (cemented_store : t) ~read_metadata level =
  match get_highest_cemented_level cemented_store with
  | None ->
      Lwt.return_none
  | Some highest_cemented_level -> (
      if
        (* negative level or the highest cemented block level is lower
           than the given level *)
        Compare.Int32.(level < 0l || highest_cemented_level < level)
      then Lwt.return_none
      else
        match compute_location cemented_store level with
        | None ->
            Lwt.return_none
        | Some ((filename, block_number) as location) ->
            let file =
              Naming.(cemented_store.cemented_blocks_dir // filename)
            in
            Lwt_unix.openfile file [Unix.O_RDONLY] 0o444
            >>= fun fd ->
            read_block fd block_number
            >>= fun block ->
            Lwt_utils_unix.safe_close fd
            >>= fun () ->
            if read_metadata then
              let metadata =
                read_block_metadata ~location cemented_store level
              in
              Lwt.return_some {block with metadata}
            else Lwt.return_some block )

let read_block_metadata cemented_store block_level =
  read_block_metadata cemented_store block_level

let get_cemented_block_by_hash ~read_metadata (cemented_store : t) hash =
  match get_cemented_block_level cemented_store hash with
  | None ->
      Lwt.return_none
  | Some level ->
      get_cemented_block_by_level ~read_metadata cemented_store level

(* Hypothesis:
   - The block list is expected to be ordered by increasing
     level and no blocks are skipped
   - If the first block has metadata, metadata are written
     and all blocks are expected to have metadata
*)
let cement_blocks (cemented_store : t) ~write_metadata
    (blocks : Block_repr.t list) =
  let nb_blocks = List.length blocks in
  let preamble_length = nb_blocks * offset_length in
  ( if nb_blocks = 0 then
    Lwt.fail_invalid_arg "cement_blocks: empty list of blocks to cement"
  else Lwt.return_unit )
  >>= fun () ->
  let first_block = List.hd blocks in
  let first_block_level = Block_repr.level first_block in
  let last_block_level =
    Int32.(add first_block_level (of_int (nb_blocks - 1)))
  in
  ( match get_highest_cemented_level cemented_store with
  | None ->
      Lwt.return_unit
  | Some highest_cemented_block ->
      if Compare.Int32.(first_block_level <> Int32.succ highest_cemented_block)
      then
        Lwt.fail_invalid_arg
          "cement_blocks: previously cemented blocks have higher level than \
           the given blocks"
      else Lwt.return_unit )
  >>= fun () ->
  let filename = Format.sprintf "%ld_%ld" first_block_level last_block_level in
  let final_file = Naming.(cemented_store.cemented_blocks_dir // filename) in
  (* Manipulate temporary files and swap it when everything is written *)
  let file =
    Naming.((cemented_store.cemented_blocks_dir // "tmp_") ^ filename)
  in
  ( if Sys.file_exists final_file then
    Format.kasprintf Lwt.fail_with "cement_blocks: file %s already exists" file
  else Lwt.return_unit )
  >>= fun () ->
  Lwt_unix.openfile file Unix.[O_CREAT; O_TRUNC; O_RDWR] 0o644
  >>= fun fd ->
  (* Blit the offset preamble *)
  let offsets_buffer = Bytes.create preamble_length in
  Lwt_utils_unix.write_bytes ~pos:0 ~len:preamble_length fd offsets_buffer
  >>= fun () ->
  let first_offset = preamble_length in
  (* Cursor is now at the beginning of the element section *)
  Lwt_list.fold_left_s
    (fun (i, current_offset) block ->
      let block_bytes =
        Data_encoding.Binary.to_bytes_exn
          Block_repr.encoding
          (* Don't write metadata in this file *)
          {block with metadata = None}
      in
      let block_offset_bytes =
        Data_encoding.Binary.to_bytes_exn offset_encoding current_offset
      in
      (* We start by blitting the corresponding offset in the preamble part *)
      Bytes.blit
        block_offset_bytes
        0
        offsets_buffer
        (i * offset_length)
        offset_length ;
      let block_len = Bytes.length block_bytes in
      (* We write the block in the file *)
      Lwt_utils_unix.write_bytes ~pos:0 ~len:block_len fd block_bytes
      >>= fun () ->
      let level = Int32.(add first_block_level (of_int i)) in
      (* We also populate the indexes *)
      Cemented_block_level_index.replace
        cemented_store.cemented_block_level_index
        block.hash
        level ;
      Cemented_block_hash_index.replace
        cemented_store.cemented_block_hash_index
        level
        block.hash ;
      Lwt.return (succ i, current_offset + block_len))
    (0, first_offset)
    blocks
  >>= fun _ ->
  (* We now write the real offsets in the preamble *)
  Lwt_unix.lseek fd 0 Unix.SEEK_SET
  >>= fun _ofs ->
  Lwt_utils_unix.write_bytes ~pos:0 ~len:preamble_length fd offsets_buffer
  >>= fun () ->
  Lwt_utils_unix.safe_close fd
  >>= fun () ->
  (* TODO clear potential artifacts *)
  Lwt_unix.rename file final_file
  >>= fun () ->
  (* Flush the indexes *)
  Cemented_block_level_index.flush cemented_store.cemented_block_level_index ;
  Cemented_block_hash_index.flush cemented_store.cemented_block_hash_index ;
  (* Update table *)
  let cemented_block_interval =
    {start_level = first_block_level; end_level = last_block_level; filename}
  in
  cemented_store.cemented_blocks_files <-
    Array.append
      cemented_store.cemented_blocks_files
      [|cemented_block_interval|] ;
  (* Compress and write the metadatas *)
  if write_metadata then cement_blocks_metadata cemented_store blocks
  else return_unit

let trigger_gc (cemented_store : t) = function
  | History_mode.Archive ->
      Lwt.return_unit
  | Full {offset} ->
      let nb_files = Array.length cemented_store.cemented_blocks_files in
      if nb_files <= offset then Lwt.return_unit
      else
        let cemented_files =
          Array.to_list cemented_store.cemented_blocks_files
        in
        let file_to_prune = List.nth cemented_files (nb_files - 1 - offset) in
        (* Remove the corresponding metadata files *)
        let metadata_file =
          Naming.(
            cemented_store.cemented_blocks_metadata_dir
            // cemented_metadata_file file_to_prune.filename)
        in
        Lwt.catch
          (fun () -> Lwt_unix.unlink metadata_file)
          (fun _exn -> Lwt.return_unit)
  | Rolling {offset} ->
      let nb_files = Array.length cemented_store.cemented_blocks_files in
      if nb_files <= offset then Lwt.return_unit
      else
        let {end_level = last_level_to_purge; _} =
          cemented_store.cemented_blocks_files.(nb_files - offset - 1)
        in
        let cemented_files =
          Array.to_list cemented_store.cemented_blocks_files
        in
        (* Start by updating the indexes by filtering blocks that are
           below the offset *)
        Cemented_block_hash_index.filter
          cemented_store.cemented_block_hash_index
          (fun (level, _) -> Compare.Int32.(level > last_level_to_purge)) ;
        Cemented_block_level_index.filter
          cemented_store.cemented_block_level_index
          (fun (_, level) -> Compare.Int32.(level > last_level_to_purge)) ;
        let (files_to_remove, _files_to_keep) =
          List.split_n (nb_files - offset) cemented_files
        in
        (* Remove the rest of the files to prune *)
        Lwt_list.iter_s
          (fun {filename; _} ->
            let block_file =
              Naming.(cemented_store.cemented_blocks_dir // filename)
            in
            let metadata_file =
              Naming.(
                cemented_store.cemented_blocks_metadata_dir
                // cemented_metadata_file filename)
            in
            Lwt.catch
              (fun () -> Lwt_unix.unlink metadata_file)
              (fun _exn -> Lwt.return_unit)
            >>= fun () ->
            Lwt.catch
              (fun () -> Lwt_unix.unlink block_file)
              (fun _exn -> Lwt.return_unit))
          files_to_remove

let restore_indexes_consistency ?(post_step = fun () -> Lwt.return_unit)
    ?genesis_hash cemented_store =
  let table = cemented_store.cemented_blocks_files in
  let len = Array.length table in
  let rec check_contiguity i =
    if i = len || i = len - 1 then return_unit
    else
      fail_unless
        Compare.Int32.(
          Int32.succ table.(i).end_level = table.(i + 1).start_level)
        (Exn
           (Failure
              (Format.asprintf
                 "Inconsistent cemented store: missing cycle between %s and %s"
                 table.(i).filename
                 table.(i + 1).filename)))
      >>=? fun () -> check_contiguity (succ i)
  in
  check_contiguity 0
  >>=? fun () ->
  let table_list = Array.to_list table in
  iter_s
    (fun {start_level = inf; end_level = sup; filename} ->
      Lwt_unix.openfile
        Naming.(cemented_store.cemented_blocks_dir // filename)
        [Unix.O_RDONLY]
        0o444
      >>= fun fd ->
      (* nb blocks : (sup - inf) + 1 *)
      let nb_blocks = Int32.(to_int (succ (sub sup inf))) in
      (* Load the offset region *)
      let len_offset = nb_blocks * offset_length in
      let bytes = Bytes.create len_offset in
      Lwt_utils_unix.read_bytes ~len:len_offset fd bytes
      >>= fun () ->
      let offsets =
        Data_encoding.Binary.of_bytes_exn
          Data_encoding.(Variable.array ~max_length:nb_blocks int31)
          bytes
      in
      (* Cursor is now after the offset region *)
      let rec iter_blocks ?pred_block n =
        if n = nb_blocks then return_unit
        else
          Lwt_unix.lseek fd 0 Unix.SEEK_CUR
          >>= fun cur_offset ->
          fail_unless
            Compare.Int.(cur_offset = offsets.(n))
            (Exn
               (Failure
                  (Format.asprintf
                     "Inconsistent cemented store: bad offset found for block \
                      #%d in cycle %s"
                     n
                     filename)))
          >>=? fun () ->
          Block_repr.read_next_block fd
          >>= fun (block, _) ->
          fail_unless
            Compare.Int32.(Block_repr.level block = Int32.(add inf (of_int n)))
            (Exn
               (Failure
                  (Format.asprintf
                     "Inconsistent cemented store: bad level found for block \
                      %a - expected %ld got %ld"
                     Block_hash.pp
                     (Block_repr.hash block)
                     Int32.(add inf (of_int n))
                     (Block_repr.level block))))
          >>=? fun () ->
          Block_repr.check_block_consistency ?genesis_hash ?pred_block block
          >>=? fun () ->
          let level = Block_repr.level block in
          let hash = Block_repr.hash block in
          Cemented_block_level_index.replace
            cemented_store.cemented_block_level_index
            hash
            level ;
          Cemented_block_hash_index.replace
            cemented_store.cemented_block_hash_index
            level
            hash ;
          iter_blocks ~pred_block:block (succ n)
      in
      Lwt.finalize
        (fun () ->
          protect (fun () ->
              iter_blocks 0
              >>=? fun () -> post_step () >>= fun () -> return_unit))
        (fun () -> Lwt_utils_unix.safe_close fd))
    table_list
  >>=? fun () -> return_unit
