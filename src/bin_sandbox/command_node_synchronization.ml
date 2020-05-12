open Flextesa
open Internal_pervasives

let default_attempts = 5

let starting_level = 10

let number_of_lonely_bakes = 100

let hm_to_string = function
  | `Archive ->
      "archive"
  | `Full i ->
      sprintf "full%s" (if i <> 0 then " " ^ Int.to_string i else "")
  | `Rolling i ->
      sprintf "rolling%s" (if i <> 0 then " " ^ Int.to_string i else "")

let cpt = ref 0

let fresh_id () = Int.incr cpt ; !cpt

let initial_port = ref 15000

let fresh_port () = Int.incr initial_port ; !initial_port

let run_test state ~node_exec ~client_exec ~primary_history_mode
    ~secondary_history_mode ~should_sync =
  Helpers.clear_root state
  >>= fun () ->
  Interactive_test.Pauser.generic
    state
    EF.[af "Ready to start"; af "Root path deleted."]
  >>= fun () ->
  let block_interval = 1 in
  let default_protocol = Tezos_protocol.default () in
  let baker_list = default_protocol.bootstrap_accounts in
  let protocol =
    {
      default_protocol with
      timestamp_delay = Some (-3600);
      expected_pow = 0;
      time_between_blocks = [block_interval; 0];
    }
  in
  let (p2p_primary_port, rpc_primary_port) = (fresh_port (), fresh_port ()) in
  let (p2p_secondary_port, rpc_secondary_port) =
    (fresh_port (), fresh_port ())
  in
  let id = fresh_id () in
  Fmt.pr
    "%d %d %d %d"
    rpc_primary_port
    p2p_primary_port
    rpc_secondary_port
    p2p_secondary_port ;
  let primary_node =
    Tezos_node.make
      ~protocol
      ~exec:node_exec
      ("primary_node_" ^ Int.to_string id)
      ~history_mode:primary_history_mode
      ~expected_connections:2
      ~rpc_port:rpc_primary_port
      ~p2p_port:p2p_primary_port
      [p2p_secondary_port]
  in
  let secondary_node =
    Tezos_node.make
      ~protocol
      ~exec:node_exec
      ~history_mode:secondary_history_mode
      ("secondary_node_" ^ Int.to_string id)
      ~expected_connections:2
      ~rpc_port:rpc_secondary_port
      ~p2p_port:p2p_secondary_port
      [p2p_primary_port]
  in
  let all_nodes = [primary_node; secondary_node] in
  Helpers.dump_connections state all_nodes
  >>= fun () ->
  Interactive_test.Pauser.add_commands
    state
    Interactive_test.Commands.(
      all_defaults state ~nodes:all_nodes
      @ [secret_keys state ~protocol; Log_recorder.Operations.show_all state]) ;
  let primary_client = Tezos_client.of_node ~exec:client_exec primary_node in
  Interactive_test.Pauser.generic
    state
    EF.
      [ af "Starting primary node in %s" (hm_to_string primary_history_mode);
        af
          "Starting secondary node in %s"
          (hm_to_string secondary_history_mode);
        af "Expecting nodes to sync after lonely baking run: %b" should_sync ]
  >>= fun () ->
  Test_scenario.Network.(start_up state ~client_exec (make all_nodes))
  >>= fun _ ->
  let (baker_account, _) = List.hd_exn baker_list in
  let baker =
    Tezos_client.Keyed.make
      primary_client
      ~key_name:(Tezos_protocol.Account.name baker_account)
      ~secret_key:(Tezos_protocol.Account.private_key baker_account)
  in
  Tezos_client.Keyed.initialize state baker
  >>= fun _ ->
  Loop.n_times (starting_level - 1) (fun i ->
      Tezos_client.Keyed.bake
        state
        baker
        (sprintf "bakery run: [%d/%d]" i starting_level))
  >>= fun () ->
  Test_scenario.Queries.wait_for_all_levels_to_be
    state
    ~attempts:default_attempts
    ~seconds:8.
    all_nodes
    (`Equal_to starting_level)
  >>= fun () ->
  Helpers.kill_node state secondary_node
  >>= fun () ->
  Loop.n_times number_of_lonely_bakes (fun i ->
      Tezos_client.Keyed.bake
        state
        baker
        (sprintf "lonely bakery run: [%d/%d]" i number_of_lonely_bakes))
  >>= fun () ->
  Tezos_client.rpc
    state
    ~client:primary_client
    `Get
    ~path:"/chains/main/checkpoint"
  >>= fun json ->
  ( match primary_history_mode with
  | `Archive ->
      return ()
  | `Rolling _ ->
      let caboose_level = Jqo.(get_int @@ field ~k:"caboose" json) in
      if not (caboose_level > starting_level) then
        fail
          (`Scenario_error
            "Caboose level is lower or equal to the starting level")
      else return ()
  | `Full _ ->
      let save_point_level = Jqo.(get_int @@ field ~k:"savepoint" json) in
      if not (save_point_level > starting_level) then
        fail
          (`Scenario_error
            "Save point level is lower or equal to the starting level")
      else return () )
  >>= fun () ->
  Helpers.restart_node ~client_exec state secondary_node
  >>= fun () ->
  Lwt.bind
    (Test_scenario.Queries.wait_for_all_levels_to_be
       state
       ~attempts:default_attempts
       ~seconds:8.
       all_nodes
       (`Equal_to (starting_level + number_of_lonely_bakes)))
    (function
      | {result = Ok _; _} when should_sync ->
          return true
      | {result = Error (`Waiting_for (_, `Time_out)); _} when not should_sync
        ->
          return false
      | _ ->
          fail
            (`Scenario_error
              "Unexpected answer when waiting for nodes synchronization"))
  >>= fun are_sync ->
  ( match (should_sync, are_sync) with
  | (false, true) ->
      fail (`Scenario_error "Nodes are not expected to be synchronized")
  | (true, false) ->
      fail (`Scenario_error "Nodes are expected to be synchronized")
  | _ ->
      return () )
  >>= fun () ->
  let identity_file = Tezos_node.identity_file state primary_node in
  System.read_file state identity_file
  >>= fun identity_contents ->
  let identity_json = Ezjsonm.from_string identity_contents in
  let primary_node_peer_id =
    Ezjsonm.value_to_string @@ Jqo.field ~k:"peer_id" identity_json
  in
  let secondary_client =
    Tezos_client.of_node ~exec:client_exec secondary_node
  in
  Tezos_client.rpc
    state
    ~client:secondary_client
    `Get
    ~path:"/network/connections/"
  >>= fun connections_json ->
  let are_nodes_connected =
    Jqo.list_exists
      ~f:(fun connection ->
        let peer_id =
          Ezjsonm.value_to_string @@ Jqo.field ~k:"peer_id" connection
        in
        String.equal primary_node_peer_id peer_id)
      connections_json
  in
  ( match (should_sync, are_nodes_connected) with
  | (true, false) ->
      fail (`Scenario_error "Expecting nodes to be connected")
  | (false, true) ->
      fail (`Scenario_error "Expecting nodes to not be connected")
  | _ ->
      return () )
  >>= fun () ->
  Stdlib.Scanf.sscanf primary_node_peer_id "%S" (fun primary_node_peer_id ->
      Tezos_client.rpc
        state
        ~client:secondary_client
        `Get
        ~path:(sprintf "/network/peers/%s/banned" primary_node_peer_id))
  >>= fun json ->
  let is_banned = Ezjsonm.get_bool json in
  if is_banned then fail (`Scenario_error "Node should not be banned")
  else return ()

let run state ~node_exec ~client_exec () =
  let combinations =
    [ (`Archive, `Full 0, true);
      (`Archive, `Rolling 0, true);
      (`Full 0, `Rolling 0, true);
      (`Rolling 0, `Archive, false);
      (`Rolling 0, `Full 0, false) ]
  in
  List_sequential.iter
    ~f:(fun (primary_history_mode, secondary_history_mode, should_sync) ->
      let test =
        run_test
          state
          ~node_exec
          ~client_exec
          ~primary_history_mode
          ~secondary_history_mode
          ~should_sync
      in
      Asynchronous_result.transform_error
        ~f:(function
          | `Scenario_error msg ->
              `Scenario_error
                (Printf.sprintf
                   "%s -> %s (expected_sync: %b): %s"
                   (hm_to_string primary_history_mode)
                   (hm_to_string secondary_history_mode)
                   should_sync
                   msg)
          | err ->
              err)
        test)
    combinations

let cmd () =
  let open Cmdliner in
  let open Term in
  let pp_error = Test_command_line.Common_errors.pp in
  let base_state =
    Test_command_line.Command_making_state.make
      ~application_name:"Flextesa"
      ~command_name:"node-synchronization"
      ()
  in
  Test_command_line.Run_command.make
    ~pp_error
    ( pure (fun node_exec client_exec state ->
          ( state,
            Interactive_test.Pauser.run_test
              ~pp_error
              state
              (run state ~node_exec ~client_exec) ))
    $ Tezos_executable.cli_term base_state `Node "tezos"
    $ Tezos_executable.cli_term base_state `Client "tezos"
    $ Test_command_line.cli_state ~name:"history_mode_synchronization" () )
    (let doc =
       sprintf
         "Synchronization of two sandboxed nodes after a lonely baking run of \
          %d blocks."
         number_of_lonely_bakes
     in
     let man : Manpage.block list =
       [ `S "NODE SYNCHRONIZATION";
         `P
           "This command builds a combination of networks of two \
            interconnected nodes. The first one solo-baking before connecting \
            to the second and check that they may or may not bootstrap the \
            second node depending the history mode on both of them." ]
     in
     info ~man ~doc "node-synchronization")
