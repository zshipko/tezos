(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2019-2021 Nomadic Labs, <contact@nomadic-labs.com>          *)
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

include Internal_event.Simple

let section = ["make-it-self-contained_command"]

let chain_status_event =
  declare_5
    ~section
    ~name:"chain_status_event"
    ~msg:
      "chain status: \n\
       current head {head_level} ({hbh}, {hch}) \n\
       checkpoint {checkpoint_level} ({cbh})"
    ~level:Notice
    ("head_level", Data_encoding.int32)
    ~pp2:Block_hash.pp
    ("hbh", Block_hash.encoding)
    ~pp3:Context_hash.pp
    ("hch", Context_hash.encoding)
    ("checkpoint_level", Data_encoding.int32)
    ~pp5:Block_hash.pp
    ("cbh", Block_hash.encoding)

let start_selfcontained_event =
  declare_3
    ~section
    ~name:"start_selfcontained_event"
    ~msg:
      "start making the upper store self contained using current head \
       ({level}, {hash}, {context_hash})"
    ~level:Notice
    ("level", Data_encoding.int32)
    ("hash", Block_hash.encoding)
    ("context_hash", Context_hash.encoding)
    ~pp2:Block_hash.pp
    ~pp3:Context_hash.pp

let end_selfcontained_event =
  declare_1
    ~section
    ~name:"end_selfcontained_event"
    ~msg:"the upper layer is now self contained after {timespan}"
    ~level:Notice
    ~pp1:Time.System.Span.pp_hum
    ("timespan", Time.System.Span.encoding)

let result_event =
  declare_1
    ~section
    ~name:"result_event"
    ~msg:"upper status: {msg}"
    ~level:Notice
    ("msg", Data_encoding.string)

let start_is_selfcontained_event =
  declare_0
    ~section
    ~name:"start_is_selfcontained_event"
    ~msg:"start checking that the upper store is self contained"
    ~level:Notice
    ()

module Term = struct
  let process check_self_contained args =
    let run =
      Internal_event_unix.init ()
      >>= fun () ->
      Node_shared_arg.read_and_patch_config_file args
      >>=? fun node_config ->
      let data_dir = node_config.data_dir in
      let ({genesis; _} : Node_config_file.blockchain_network) =
        node_config.blockchain_network
      in
      let (_validator_environment
            : Block_validator_process.validator_environment) =
        {
          user_activated_upgrades =
            node_config.blockchain_network.user_activated_upgrades;
          user_activated_protocol_overrides =
            node_config.blockchain_network.user_activated_protocol_overrides;
        }
      in
      let context_dir = Node_data_version.context_dir data_dir in
      let store_dir = Node_data_version.store_dir data_dir in
      Store.init
        ~readonly:false
        ~store_dir
        ~context_dir
        ~allow_testchains:false
        genesis
      >>=? fun store ->
      let context = Store.context_index store in
      let main_chain_store = Store.main_chain_store store in
      Store.Chain.current_head main_chain_store
      >>= fun current_head ->
      Store.Chain.checkpoint main_chain_store
      >>= fun (checkpoint_hash, checkpoint_level) ->
      let head_level = Store.Block.level current_head in
      let head_hash = Store.Block.hash current_head in
      let head_context_hash = Store.Block.context_hash current_head in
      emit
        chain_status_event
        ( head_level,
          head_hash,
          head_context_hash,
          checkpoint_level,
          checkpoint_hash )
      >>= fun () ->
      emit start_selfcontained_event (head_level, head_hash, head_context_hash)
      >>= fun () ->
      let now = Systime_os.now () in
      Context.wip_self_contained context head_context_hash
      >>= fun () ->
      let timespan =
        let then_ = Systime_os.now () in
        Ptime.diff then_ now
      in
      emit end_selfcontained_event timespan
      >>= fun () ->
      (* Make sure it worked if the corresponding flag is given *)
      ( if check_self_contained then
        emit start_is_selfcontained_event ()
        >>= fun () ->
        Context.wip_is_self_contained context head_context_hash
        >>= function
        | Ok (`Msg msg) ->
            emit result_event msg
        | Error (`Msg err) ->
            Lwt.fail_with err
      else Lwt.return_unit )
      >>= fun () -> Store.close_store store >>= fun () -> return_unit
    in
    match Lwt_main.run @@ Lwt_exit.wrap_and_exit run with
    | Ok () ->
        `Ok ()
    | Error err ->
        `Error (false, Format.asprintf "%a" pp_print_error err)

  let sandbox =
    let open Cmdliner in
    let doc =
      "TODO DESCR"
      (* "Run the bench command in sandbox mode. P2P to non-localhost addresses \
       *  are disabled, and constants of the economic protocol can be altered \
       *  with an optional JSON file. $(b,IMPORTANT): Using sandbox mode affects \
       *  the node state and subsequent runs of Tezos node must also use sandbox \
       *  mode. In order to run the node in normal mode afterwards, a full reset \
       *  must be performed (by removing the node's data directory)." *)
    in
    Arg.(
      value
      & opt (some non_dir_file) None
      & info
          ~docs:Node_shared_arg.Manpage.misc_section
          ~doc
          ~docv:"FILE.json"
          ["sandbox"])

  let make_it_self_contained =
    let open Cmdliner in
    let doc =
      "WIP: Make the current head of the block store as a selfcontained upper \
       store. This utility is meant to be used to transform a non layered \
       store to a valid layered one where the all context was manually moved \
       into the `context/lower` folder. "
    in
    Arg.(
      value & flag
      & info
          ~docs:Node_shared_arg.Manpage.misc_section
          ~doc
          ["make-it-self-contained"])

  let check_self_contained =
    let open Cmdliner in
    let doc =
      "If the flag is present, it checks that the commit hash on witch the \
       store ran the `self_contained` function is actually self contained, \
       using the dedicated `is_self_contained` function."
    in
    Arg.(
      value & flag
      & info ~docs:Node_shared_arg.Manpage.misc_section ~doc ["check"])

  let term =
    let open Cmdliner.Term in
    ret (const process $ check_self_contained $ Node_shared_arg.Term.args)
end

module Manpage = struct
  let command_description = "TODO DESCR"

  let description = [`S "DESCRIPTION"; `P command_description]

  let options = []

  let examples =
    [ `S "EXAMPLES";
      `I ("$(b,example )", "./tezos-node make-it-self-contained ") ]

  let man = description @ options @ examples @ Node_shared_arg.Manpage.bugs

  let info =
    Cmdliner.Term.info
      ~doc:"Manage upgrade toward layered store"
      ~man
      "make-it-self-contained"
end

let cmd = (Term.term, Manpage.info)
