(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2020 Tarides <contact@tarides.com>                          *)
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

let term_name = "storage"

module Term = struct
  open Cmdliner

  (* [Cmdliner] terms are not nestable, so we implement an ad-hoc mechanism for
     delegating to one of several "subcommand"s by parsing a single positional
     argument and then calling [Term.eval] again with the remaining
     arguments. *)

  type subcommand = {
    name : string;
    description : string;
    term : (unit -> unit) Term.t;
  }

  let terms =
    let open Context.Checks in
    [ {
      name = "check-self-contained";
      description =
        "Check that the upper layer of the store is self-contained.";
      term = Pack.Check_self_contained.term;
    };
      {
        name = "integrity-check-index";
        description = "Search the store for integrity faults and corruption.";
        term = Index.Integrity_check.term;
      };
      {
        name = "stat-index";
        description = "Print high-level statistics about the index store.";
        term = Index.Stat.term;
      };
      {
        name = "stat-pack";
        description = "Print high-level statistics about the pack file.";
        term = Pack.Stat.term;
      };
      {
        name = "reconstruct-index";
        description = "Reconstruct index from pack file.";
        term = Pack.Reconstruct_index.term;
      } ]

  let default_config_file () =
    return Node_config_file.default_config

  let read_data_dir data_dir =
    default_config_file ()
    >>=? fun cfg ->
    let data_dir = Option.value ~default:cfg.data_dir data_dir in
    return data_dir

  let get_genesis genesis_path =
    Lwt_stream.to_string (Lwt_io.chars_of_file genesis_path) >|= fun s ->
    Data_encoding.Binary.of_bytes_exn Genesis.encoding (Bytes.unsafe_of_string s)

  let get_head data_dir =
    (read_data_dir data_dir >>= fun root ->
     let root = Result.get_ok root in
     let ( // ) = Filename.concat in
     let store_dir = root // "store" in
     let context_dir = root // "context" in
     get_genesis (root // "genesis")  >>= fun genesis ->
     Store.init
       ~store_dir
       ~context_dir
       ~allow_testchains:true
       genesis  >>= fun store ->
     let store = Result.get_ok store in
     let chain_store = Store.main_chain_store store in
     Store.Chain.current_head chain_store)
    |> Lwt_main.run

  let dispatch_subcommand data_dir =
    function
    | None ->
      `Help (`Auto, Some term_name)
    | Some n -> (
        match List.find_opt (fun {name; _} -> name = n) terms with
        | None ->
          let msg =
            let pp_ul = Fmt.(list ~sep:cut (const string "- " ++ string)) in
            terms
            |> List.map (fun {name; _} -> name)
            |> Fmt.str
              "@[<v 0>Unrecognized command: %s@,\
               @,\
               Available commands:@,\
               %a@,\
               @]"
              n
              pp_ul
          in
          `Error (false, msg)
        | Some command -> (
            let (binary_name, argv) =
              (* Get remaining arguments for subcommand evaluation *)
              ( Sys.argv.(0),
                Array.init
                  (Array.length Sys.argv - 2)
                  (function 0 -> Sys.argv.(0) | i -> Sys.argv.(i + 2)) )
            in
            let noop_formatter =
              Format.make_formatter (fun _ _ _ -> ()) (fun () -> ())
            in
            let argv = if command.name = "check-self-contained" then
                Array.append argv  [| "--heads"; Store.Block.hash @@ get_head data_dir |> Block_hash.to_string |]
              else argv in
            Term.eval
              ~argv
              ~err:noop_formatter (* Defaults refer to non-existent help *)
              ~catch:false (* Will be caught by parent [Term.eval_choice] *)
              ( command.term,
                Term.info (binary_name ^ " " ^ term_name ^ " " ^ command.name) )
            |> function
            | `Ok f ->
              `Ok (f ())
            | `Help | `Version ->
              (* Parent term evaluation intercepts [--help] and [--version] *)
              assert false
            | `Error _ -> (
                (* We want to display the usage information for the selected
                   subcommand, but [Cmdliner] will only do this at evaluation
                   time *)
                Term.eval
                  ~argv:[|""; "--help=plain"|]
                  ( command.term,
                    Term.info (binary_name ^ " " ^ term_name ^ " " ^ command.name)
                  )
                |> function `Help -> `Ok () | _ -> assert false ) ) )

  let term =
    let subcommand =
      (* NOTE: [Cmdliner] doesn't have a wildcard argument or mechanism for
         deferring the parsing of arguments, so this term must explicitly
         support any options required by the subcommands *)
      Arg.(value @@ pos_all string [] (info ~docv:"COMMAND" []))
      |> Term.(app (const List.hd_opt))
    in
    Term.(ret (const dispatch_subcommand $ Node_shared_arg.Term.data_dir $ subcommand ))
end

module Manpage = struct
  let command_description =
    "The $(b,storage) command provides tools for introspecting and debugging \
     the storage layer."

  let commands =
    [ `S Cmdliner.Manpage.s_commands;
      `P "The following subcommands are available:";
      `Blocks
        (List.map
           (fun Term.{name; description; _} ->
              `I (Printf.sprintf " $(b,%s)" name, description))
           Term.terms);
      `P
        "$(b,WARNING): this API is experimental and may change in future \
         versions." ]

  let man = commands @ Node_shared_arg.Manpage.bugs

  let info =
    Cmdliner.Term.info
      ~doc:"Query the storage layer (EXPERIMENTAL)"
      ~man
      term_name
end

let cmd = (Term.term, Manpage.info)
