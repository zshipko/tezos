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

let default_parameters =
  Tezos_protocol_alpha_parameters.Default_parameters.(
    Alpha_utils.default_genesis_parameters |> json_of_parameters
    |> fun x -> Some x)

let make_legacy_store ?(sandbox_parameters = default_parameters)
    ?(user_activated_upgrades = []) ?(user_activated_protocol_overrides = [])
    ~legacy_store_builder_exe ~history_mode ~genesis_block store genesis
    (blocks : Store.Block.t list) ~output_dir =
  let legacy_dir = output_dir in
  let proc =
    let exe = legacy_store_builder_exe in
    if not (Sys.file_exists exe) then
      Format.ksprintf Stdlib.invalid_arg "File %s does not exist" exe ;
    Lwt_process.open_process_full (exe, [|Filename.basename exe; legacy_dir|])
  in
  let proc_reader =
    let rec loop () =
      External_validation.recv proc#stdout Data_encoding.string
      >>= fun l ->
      Format.printf "[Legacy store builder] %s@." l ;
      loop ()
    in
    Lwt.catch
      (fun () -> loop ())
      (function
        | End_of_file ->
            Format.printf "[Legacy store builder] Terminating@." ;
            Lwt.return_unit
        | exn ->
            Lwt.fail exn)
  in
  Lwt.async (fun () -> proc_reader) ;
  (* Initialize the legacy state *)
  External_validation.send
    proc#stdin
    (Data_encoding.tup2
       History_mode.Legacy.encoding
       External_validation.parameters_encoding)
    ( history_mode,
      {
        External_validation.context_root = Filename.concat legacy_dir "context";
        protocol_root = Filename.concat legacy_dir "protocol";
        sandbox_parameters;
        genesis;
        user_activated_upgrades;
        user_activated_protocol_overrides;
      } )
  >>= fun () ->
  (* Start sending blocks *)
  let chain_store = Store.main_chain_store store in
  let chain_id = Store.Chain.chain_id chain_store in
  Lwt_list.fold_left_s
    (fun pred_block block ->
      let block_header = Store.Block.header block in
      let operations = Store.Block.operations block in
      let max_operations_ttl =
        (Option.unopt_assert
           ~loc:__POS__
           (Store.Unsafe.repr_of_block pred_block).Block_repr.metadata)
          .max_operations_ttl
      in
      External_validation.send
        proc#stdin
        External_validation.request_encoding
        (Validate
           {
             chain_id;
             block_header;
             predecessor_block_header = Store.Block.header pred_block;
             operations;
             max_operations_ttl;
           })
      >>= fun () -> Lwt.return block)
    genesis_block
    blocks
  >>= fun _ ->
  External_validation.send
    proc#stdin
    External_validation.request_encoding
    Terminate
  >>= fun () ->
  Lwt.join [proc_reader]
  >>= fun () ->
  proc#status
  >>= fun status ->
  Assert.is_true ~msg:"legacy builder exited" (status = Unix.(WEXITED 0)) ;
  Lwt.return_unit

let ( // ) = Filename.concat

let init_store ~base_dir ~patch_context ~history_mode k =
  let store_dir = base_dir // "store" in
  let context_dir = base_dir // "context" in
  Store.init
    ~patch_context
    ~history_mode
    ~store_dir
    ~context_dir
    ~allow_testchains:false
    Test_utils.genesis
  >>=? fun store -> k (store_dir, context_dir) store

let store_builder ?(legacy_history_mode = History_mode.Legacy.Full)
    ?(nb_blocks = `Cycle 8) ~base_dir ~legacy_store_builder_exe () =
  let history_mode = History_mode.convert legacy_history_mode in
  let patch_context ctxt = Alpha_utils.default_patch_context ctxt in
  let k (_store_dir, _context_dir) store =
    let chain_store = Store.main_chain_store store in
    let genesis = Store.Chain.genesis chain_store in
    Store.Chain.genesis_block chain_store
    >>= fun genesis_block ->
    Lwt.finalize
      (fun () ->
        match nb_blocks with
        | `Cycle n ->
            Alpha_utils.bake_until_n_cycle_end chain_store n genesis_block
        | `Blocks n ->
            Alpha_utils.bake_n chain_store n genesis_block)
      (fun () ->
        let bs = Store.Unsafe.get_block_store chain_store in
        Block_store.await_merging bs >>= fun _ -> Lwt.return_unit)
    >>=? fun (blocks, _last_head) ->
    return (store, genesis, genesis_block, blocks)
  in
  init_store ~base_dir ~patch_context ~history_mode k
  >>=? fun (store, genesis, genesis_block, blocks) ->
  let legacy_store_dir = base_dir // "store_to_upgrade" in
  make_legacy_store
    ~legacy_store_builder_exe
    ~genesis_block
    ~history_mode:legacy_history_mode
    store
    genesis
    blocks
    ~output_dir:legacy_store_dir
  >>= fun () ->
  Legacy_state.init
    ~readonly:false
    ~history_mode:legacy_history_mode
    ~store_root:(legacy_store_dir // "store")
    ~context_root:(legacy_store_dir // "context")
    genesis
  >>=? fun (legacy_state, _, _, _) ->
  return (store, (legacy_store_dir, legacy_state), blocks)

type test = {
  name : string;
  speed : [`Quick | `Slow];
  legacy_history_mode : History_mode.Legacy.t;
  nb_blocks : [`Cycle of int | `Blocks of int];
  test :
    Store.t ->
    string * Legacy_state.t ->
    Store.Block.t list ->
    unit tzresult Lwt.t;
}

let wrap_test_legacy ?(keep_dir = false) test : string Alcotest_lwt.test_case =
  let {name; speed; legacy_history_mode; nb_blocks; test} = test in
  let prefix_dir = "tezos_indexed_store_test_" in
  let with_dir f =
    if not keep_dir then
      Lwt_utils_unix.with_tempdir prefix_dir (fun base_dir ->
          Lwt.catch
            (fun () -> f base_dir)
            (fun exn ->
              Lwt_utils_unix.remove_dir base_dir
              >>= fun () -> Lwt.return (Error_monad.error_exn exn)))
    else
      let base_dir = Filename.temp_file prefix_dir "" in
      Lwt_unix.unlink base_dir
      >>= fun () ->
      Lwt_unix.mkdir base_dir 0o700
      >>= fun () ->
      Format.printf "@\nPersisting dir %s for test.@." base_dir ;
      f base_dir
  in
  let test _ legacy_store_builder_exe =
    with_dir (fun base_dir ->
        store_builder
          ~base_dir
          ~legacy_history_mode
          ~nb_blocks
          ~legacy_store_builder_exe
          ()
        >>=? fun (store, (legacy_store_dir, legacy_state), blocks) ->
        Lwt.catch
          (fun () -> test store (legacy_store_dir, legacy_state) blocks)
          (fun exn ->
            Legacy_state.close legacy_state
            >>= fun () -> Store.close_store store >>= fun () -> Lwt.fail exn))
  in
  Alcotest_lwt.test_case name speed (fun x exe ->
      test x exe
      >>= function
      | Ok () ->
          Lwt.return_unit
      | Error errs ->
          Format.printf
            "@\nError while running test:@\n%a@."
            Error_monad.pp_print_error
            errs ;
          Lwt.fail Alcotest.Test_error)
