(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2018 Nomadic Labs. <nomadic@tezcore.com>                    *)
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

type error += Invalid_sandbox_file of string

let () =
  register_error_kind
    `Permanent
    ~id:"main.snapshots.invalid_sandbox_file"
    ~title:"Invalid sandbox file"
    ~description:"The provided sandbox file is not a valid sandbox JSON file."
    ~pp:(fun ppf s ->
      Format.fprintf ppf "The file '%s' is not a valid JSON sandbox file" s)
    Data_encoding.(obj1 (req "sandbox_file" string))
    (function Invalid_sandbox_file s -> Some s | _ -> None)
    (fun s -> Invalid_sandbox_file s)

(** Main *)

module Event = struct
  include Internal_event.Simple

  let cleaning_up_after_failure =
    Internal_event.Simple.declare_1
      ~section:["node"; "main"]
      ~name:"cleaning_up_after_failure"
      ~msg:"cleaning up directory \"{directory}\" after failure."
      ~level:Internal_event.Notice
      ("directory", Data_encoding.string)
end

module Term = struct
  type subcommand = Export | Import | Info

  let process subcommand args snapshot_path block rolling reconstruct
      sandbox_file import_legacy =
    (* FIXME check snapshot format *)
    let run =
      Internal_event_unix.init ()
      >>= fun () ->
      (* TODO FIXME *)
      Node_shared_arg.read_and_patch_config_file args
      >>=? fun node_config ->
      let data_dir = node_config.data_dir in
      let ({genesis; chain_name; _} : Node_config_file.blockchain_network) =
        node_config.blockchain_network
      in
      match subcommand with
      | Export ->
          let dir_cleaner () =
            Event.(emit cleaning_up_after_failure) snapshot_path
            >>= fun () -> Lwt_utils_unix.remove_dir snapshot_path
          in
          Node_data_version.ensure_data_dir data_dir
          >>=? fun () ->
          let context_dir = Node_data_version.context_dir data_dir in
          let store_dir = Node_data_version.store_dir data_dir in
          protect
            ~on_error:(fun err ->
              dir_cleaner () >>= fun () -> Lwt.return (Error err))
            (fun () ->
              Snapshots.export
                ~rolling
                ~store_dir
                ~context_dir
                ~chain_name
                ?block
                ~snapshot_dir:snapshot_path
                genesis)
      | Import ->
          let dir_cleaner () =
            Event.(emit cleaning_up_after_failure) data_dir
            >>= fun () ->
            Lwt_utils_unix.remove_dir (Node_data_version.store_dir data_dir)
            >>= fun () ->
            Lwt_utils_unix.remove_dir (Node_data_version.context_dir data_dir)
          in
          Node_config_file.write args.config_file node_config
          >>=? fun () ->
          Node_data_version.ensure_data_dir ~bare:true data_dir
          >>=? fun () ->
          Lwt_lock_file.create
            ~unlink_on_exit:true
            (Node_data_version.lock_file data_dir)
          >>=? fun () ->
          ( match
              (node_config.blockchain_network.genesis_parameters, sandbox_file)
            with
          | (None, None) ->
              return_none
          | (Some parameters, None) ->
              return_some (parameters.context_key, parameters.values)
          | (_, Some filename) -> (
              Lwt_utils_unix.Json.read_file filename
              >>= function
              | Error _err ->
                  fail (Invalid_sandbox_file filename)
              | Ok json ->
                  return_some ("sandbox_parameter", json) ) )
          >>=? fun sandbox_parameters ->
          let context_root = Node_data_version.context_dir data_dir in
          let store_root = Node_data_version.store_dir data_dir in
          let patch_context =
            Patch_context.patch_context genesis sandbox_parameters
          in
          protect
            ~on_error:(fun err ->
              dir_cleaner () >>= fun () -> Lwt.return (Error err))
            (fun () ->
              if import_legacy then
                Snapshots.import_legacy
                  ~patch_context
                  ?block
                  ~dst_store_dir:store_root
                  ~dst_context_dir:context_root
                  ~chain_name:node_config.blockchain_network.chain_name
                  ~user_activated_upgrades:
                    node_config.blockchain_network.user_activated_upgrades
                  ~user_activated_protocol_overrides:
                    node_config.blockchain_network
                      .user_activated_protocol_overrides
                  ~snapshot_file:snapshot_path
                  genesis
              else
                Snapshots.import
                  ~patch_context
                  ?block
                  ~snapshot_dir:snapshot_path
                  ~dst_store_dir:store_root
                  ~dst_context_dir:context_root
                  ~user_activated_upgrades:
                    node_config.blockchain_network.user_activated_upgrades
                  ~user_activated_protocol_overrides:
                    node_config.blockchain_network
                      .user_activated_protocol_overrides
                  genesis)
          >>=? fun () ->
          if reconstruct then
            Reconstruction.reconstruct
              ~patch_context
              ~store_root
              ~context_root
              ~genesis
              ~user_activated_upgrades:
                node_config.blockchain_network.user_activated_upgrades
              ~user_activated_protocol_overrides:
                node_config.blockchain_network
                  .user_activated_protocol_overrides
          else return_unit
      | Info ->
          Snapshots.snapshot_info ~snapshot_dir:snapshot_path
          >>= fun () -> return_unit
    in
    match Lwt_main.run run with
    | Ok () ->
        `Ok ()
    | Error err ->
        `Error (false, Format.asprintf "%a" pp_print_error err)

  open Cmdliner.Arg

  let subcommand_arg =
    let parser = function
      | "export" ->
          `Ok Export
      | "import" ->
          `Ok Import
      | "info" ->
          `Ok Info
      | s ->
          `Error ("invalid argument: " ^ s)
    and printer ppf = function
      | Export ->
          Format.fprintf ppf "export"
      | Import ->
          Format.fprintf ppf "import"
      | Info ->
          Format.fprintf ppf "info"
    in
    let doc =
      "Operation to perform. Possible values: $(b,export), $(b,import)."
    in
    required
    & pos 0 (some (parser, printer)) None
    & info [] ~docv:"OPERATION" ~doc

  let file_arg = required & pos 1 (some string) None & info [] ~docv:"FILE"

  let block =
    let open Cmdliner.Arg in
    let doc =
      "The block to export/import. When exporting, either the block_hash, the \
       level or an alias (such as $(i,caboose), $(i,checkpoint), \
       $(i,savepoint) or $(i,head) in combination with ~ and + operators) can \
       be used. When importing, only the block hash you are expected to \
       restore is allowed."
    in
    value
    & opt (some string) None
    & info ~docv:"<block_hash, level, alias>" ~doc ["block"]

  let export_rolling =
    let open Cmdliner in
    let doc =
      "Force export command to dump a minimal snapshot based on the rolling \
       mode."
    in
    Arg.(
      value & flag
      & info ~docs:Node_shared_arg.Manpage.misc_section ~doc ["rolling"])

  let reconstruct =
    let open Cmdliner in
    let doc =
      "Start a storage reconstruction from a full mode snapshot to an archive \
       storage. This operation can be quite long."
    in
    Arg.(
      value & flag
      & info ~docs:Node_shared_arg.Manpage.misc_section ~doc ["reconstruct"])

  let sandbox =
    let open Cmdliner in
    let doc =
      "Run the snapshot import in sandbox mode. P2P to non-localhost \
       addresses are disabled, and constants of the economic protocol can be \
       altered with an optional JSON file. $(b,IMPORTANT): Using sandbox mode \
       affects the node state and subsequent runs of Tezos node must also use \
       sandbox mode. In order to run the node in normal mode afterwards, a \
       full reset must be performed (by removing the node's data directory)."
    in
    Arg.(
      value
      & opt (some non_dir_file) None
      & info
          ~docs:Node_shared_arg.Manpage.misc_section
          ~doc
          ~docv:"FILE.json"
          ["sandbox"])

  let import_legacy =
    let open Cmdliner in
    let doc = "Force import command to process a lagacy snapshot file." in
    Arg.(
      value & flag
      & info ~docs:Node_shared_arg.Manpage.misc_section ~doc ["legacy"])

  let term =
    let open Cmdliner.Term in
    ret
      ( const process $ subcommand_arg $ Node_shared_arg.Term.args $ file_arg
      $ block $ export_rolling $ reconstruct $ sandbox $ import_legacy )
end

module Manpage = struct
  let command_description =
    "The $(b,snapshot) command is meant to export and import snapshots files."

  let description =
    [ `S "DESCRIPTION";
      `P (command_description ^ " Several operations are possible: ");
      `P
        "$(b,export) allows to export a snapshot of the current node state \
         into a file.";
      `P "$(b,import) allows to import a snapshot from a given file.";
      `P "$(b,info) displays information about the snapshot file." ]

  let options = [`S "OPTIONS"]

  let examples =
    [ `S "EXAMPLES";
      `I
        ( "$(b,Export a snapshot using the rolling mode)",
          "$(mname) snapshot export latest.rolling --rolling" );
      `I
        ( "$(b,Export a snapshot up to and including the given block hash)",
          "$(mname) snapshot export file.full --block <BLOCK_HASH>" );
      `I
        ( "$(b,Export a snapshot up to and including the 10th predecessor of \
           the current head)",
          "$(mname) snapshot export file.full --block head~10" );
      `I
        ( "$(b,Import a snapshot located in file.full)",
          "$(mname) snapshot import file.full" );
      `I
        ( "$(b,Import a snapshot and ensure that the imported data targets \
           the given block hash (recommended))",
          "$(mname) snapshot import file.full --block <BLOCK_HASH>" );
      `I
        ( "$(b,Import a full mode snapshot and then reconstruct the whole \
           storage to obtain an archive mode storage)",
          "$(mname) snapshot import file.full --reconstruct" ) ]

  let man = description @ options @ examples @ Node_shared_arg.Manpage.bugs

  let info = Cmdliner.Term.info ~doc:"Manage snapshots" ~man "snapshot"
end

let cmd = (Term.term, Manpage.info)
