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

let rec read_partial_context context path depth =
  (* non tail-recursive *)
  if depth = 0 then Lwt.return Block_services.Cut
  else
    (* try to read as file *)
    Context.get context path
    >>= function
    | Some v ->
        Lwt.return (Block_services.Key v)
    | None ->
        (* try to read as directory *)
        Context.fold context path ~init:[] ~f:(fun k acc ->
            match k with
            | `Key k | `Dir k ->
                read_partial_context context k (depth - 1)
                >>= fun v ->
                let k = List.nth k (List.length k - 1) in
                Lwt.return ((k, v) :: acc))
        >>= fun l -> Lwt.return (Block_services.Dir (List.rev l))

let build_raw_header_rpc_directory (module Proto : Block_services.PROTO) =
  let dir :
      (Store.chain_store * Block_hash.t * Block_header.t) RPC_directory.t ref =
    ref RPC_directory.empty
  in
  let register0 s f =
    dir :=
      RPC_directory.register !dir (RPC_service.subst0 s) (fun block p q ->
          f block p q)
  in
  let module Block_services = Block_services.Make (Proto) (Proto) in
  let module S = Block_services.S in
  register0 S.hash (fun (_, hash, _) () () -> return hash) ;
  (* block header *)
  register0 S.header (fun (chain_store, hash, header) () () ->
      let protocol_data =
        Data_encoding.Binary.of_bytes_exn
          Proto.block_header_data_encoding
          header.protocol_data
      in
      return
        {
          Block_services.hash;
          chain_id = Store.Chain.chain_id chain_store;
          shell = header.shell;
          protocol_data;
        }) ;
  register0 S.raw_header (fun (_, _, header) () () ->
      return (Data_encoding.Binary.to_bytes_exn Block_header.encoding header)) ;
  register0 S.Header.shell_header (fun (_, _, header) () () ->
      return header.shell) ;
  register0 S.Header.protocol_data (fun (_, _, header) () () ->
      return
        (Data_encoding.Binary.of_bytes_exn
           Proto.block_header_data_encoding
           header.protocol_data)) ;
  register0 S.Header.raw_protocol_data (fun (_, _, header) () () ->
      return header.protocol_data) ;
  (* helpers *)
  register0 S.Helpers.Forge.block_header (fun _block () header ->
      return (Data_encoding.Binary.to_bytes_exn Block_header.encoding header)) ;
  (* protocols *)
  register0 S.protocols (fun (chain_store, _hash, header) () () ->
      Store.Chain.find_protocol_level chain_store header.shell.proto_level
      >>= fun next_proto ->
      let (_, next_protocol_hash, _) = Option.unopt_exn Not_found next_proto in
      Store.Block.read_block_opt chain_store header.shell.predecessor
      >>= function
      | None ->
          return
            {
              Tezos_shell_services.Block_services.current_protocol =
                next_protocol_hash;
              next_protocol = next_protocol_hash;
            }
      | Some pred_block ->
          let pred_header = Store.Block.header pred_block in
          Store.Chain.find_protocol_level
            chain_store
            pred_header.shell.proto_level
          >>= fun current_protocol ->
          let (_, protocol_hash, _) =
            Option.unopt_exn Not_found current_protocol
          in
          return
            {
              Tezos_shell_services.Block_services.current_protocol =
                protocol_hash;
              next_protocol = next_protocol_hash;
            }) ;
  !dir

let build_raw_rpc_directory ~user_activated_upgrades
    ~user_activated_protocol_overrides (module Proto : Block_services.PROTO)
    (module Next_proto : Registered_protocol.T) =
  let dir : (Store.chain_store * Store.Block.block) RPC_directory.t ref =
    ref RPC_directory.empty
  in
  let merge d = dir := RPC_directory.merge d !dir in
  let register0 s f =
    dir :=
      RPC_directory.register !dir (RPC_service.subst0 s) (fun block p q ->
          f block p q)
  in
  let register1 s f =
    dir :=
      RPC_directory.register !dir (RPC_service.subst1 s) (fun (block, a) p q ->
          f block a p q)
  in
  let register2 s f =
    dir :=
      RPC_directory.register
        !dir
        (RPC_service.subst2 s)
        (fun ((block, a), b) p q -> f block a b p q)
  in
  let module Block_services = Block_services.Make (Proto) (Next_proto) in
  let module S = Block_services.S in
  register0 S.live_blocks (fun (chain_store, block) () () ->
      Store.Chain.compute_live_blocks chain_store ~block
      >>=? fun (live_blocks, _) -> return live_blocks) ;
  (* block metadata *)
  let metadata chain_store block =
    Store.Block.get_block_metadata chain_store block
    >>=? fun metadata ->
    let protocol_data =
      Data_encoding.Binary.of_bytes_exn
        Proto.block_header_metadata_encoding
        (Store.Block.block_metadata metadata)
    in
    Store.Chain.testchain_status chain_store block
    >>= fun (test_chain_status, _) ->
    let max_operations_ttl = Store.Block.max_operations_ttl metadata in
    return
      {
        Block_services.protocol_data;
        test_chain_status;
        max_operations_ttl;
        max_operation_data_length = Next_proto.max_operation_data_length;
        max_block_header_length = Next_proto.max_block_length;
        operation_list_quota =
          List.map
            (fun {Tezos_protocol_environment.max_size; max_op} ->
              {Tezos_shell_services.Block_services.max_size; max_op})
            Next_proto.validation_passes;
      }
  in
  register0 S.metadata (fun (chain_store, block) () () ->
      metadata chain_store block) ;
  (* operations *)
  let convert chain_id (op : Operation.t) metadata : Block_services.operation =
    let protocol_data =
      Data_encoding.Binary.of_bytes_exn Proto.operation_data_encoding op.proto
    in
    let receipt =
      Data_encoding.Binary.of_bytes_exn
        Proto.operation_receipt_encoding
        metadata
    in
    {
      Block_services.chain_id;
      hash = Operation.hash op;
      shell = op.shell;
      protocol_data;
      receipt;
    }
  in
  let operations chain_store block =
    let ops = Store.Block.operations block in
    Store.Block.get_block_metadata chain_store block
    >>=? fun metadata ->
    let ops_metadata = Store.Block.operations_metadata metadata in
    let chain_id = Store.Chain.chain_id chain_store in
    return (List.map2 (List.map2 (convert chain_id)) ops ops_metadata)
  in
  register0 S.Operations.operations (fun (chain_store, block) () () ->
      operations chain_store block) ;
  register1
    S.Operations.operations_in_pass
    (fun (chain_store, block) i () () ->
      let chain_id = Store.Chain.chain_id chain_store in
      try
        let (ops, _path) = Store.Block.operations_path block i in
        Store.Block.get_block_metadata_opt chain_store block
        >>= function
        | None ->
            Lwt.fail Not_found
        | Some metadata ->
            let opss_metadata = Store.Block.operations_metadata metadata in
            let ops_metadata = List.nth opss_metadata i in
            return (List.map2 (convert chain_id) ops ops_metadata)
      with _ -> Lwt.fail Not_found) ;
  register2 S.Operations.operation (fun (chain_store, block) i j () () ->
      let chain_id = Store.Chain.chain_id chain_store in
      ( try
          let (ops, _path) = Store.Block.operations_path block i in
          Store.Block.get_block_metadata_opt chain_store block
          >>= function
          | None ->
              Lwt.fail Not_found
          | Some metadata ->
              let opss_metadata = Store.Block.operations_metadata metadata in
              let ops_metadata = List.nth opss_metadata i in
              return (List.nth ops j, List.nth ops_metadata j)
        with _ -> Lwt.fail Not_found )
      >>=? fun (op, md) -> return (convert chain_id op md)) ;
  (* operation_hashes *)
  register0 S.Operation_hashes.operation_hashes (fun (_, block) () () ->
      return (Store.Block.all_operation_hashes block)) ;
  register1
    S.Operation_hashes.operation_hashes_in_pass
    (fun (_, block) i () () ->
      return (Store.Block.operations_hashes_path block i |> fst)) ;
  register2 S.Operation_hashes.operation_hash (fun (_, block) i j () () ->
      ( try
          let (ops, _) = Store.Block.operations_hashes_path block i in
          Lwt.return (List.nth ops j)
        with _ -> Lwt.fail Not_found )
      >>= fun op -> return op) ;
  (* context *)
  register1 S.Context.read (fun (chain_store, block) path q () ->
      let depth = Option.unopt ~default:max_int q#depth in
      fail_unless
        (depth >= 0)
        (Tezos_shell_services.Block_services.Invalid_depth_arg depth)
      >>=? fun () ->
      Store.Block.context chain_store block
      >>=? fun context ->
      Context.mem context path
      >>= fun mem ->
      Context.dir_mem context path
      >>= fun dir_mem ->
      if not (mem || dir_mem) then Lwt.fail Not_found
      else read_partial_context context path depth >>= fun dir -> return dir) ;
  (* info *)
  register0 S.info (fun (chain_store, block) () () ->
      let chain_id = Store.Chain.chain_id chain_store in
      let hash = Store.Block.hash block in
      let header = Store.Block.header block in
      let shell = header.shell in
      let protocol_data =
        Data_encoding.Binary.of_bytes_exn
          Proto.block_header_data_encoding
          header.protocol_data
      in
      metadata chain_store block
      >>=? fun metadata ->
      (* FIXME: handle pruned metadata *)
      operations chain_store block
      >>=? fun operations ->
      return
        {
          Block_services.hash;
          chain_id;
          header = {shell; protocol_data};
          metadata;
          operations;
        }) ;
  (* helpers *)
  register0 S.Helpers.Preapply.block (fun (chain_store, block) q p ->
      let timestamp =
        match q#timestamp with
        | None ->
            Time.System.to_protocol (Systime_os.now ())
        | Some time ->
            time
      in
      let protocol_data =
        Data_encoding.Binary.to_bytes_exn
          Next_proto.block_header_data_encoding
          p.protocol_data
      in
      let operations =
        List.map
          (fun operations ->
            let operations =
              if q#sort_operations then
                List.sort Next_proto.compare_operations operations
              else operations
            in
            List.map
              (fun op ->
                let proto =
                  Data_encoding.Binary.to_bytes_exn
                    Next_proto.operation_data_encoding
                    op.Next_proto.protocol_data
                in
                {Operation.shell = op.shell; proto})
              operations)
          p.operations
      in
      Prevalidation.preapply
        chain_store
        ~user_activated_upgrades
        ~user_activated_protocol_overrides
        ~predecessor:block
        ~timestamp
        ~protocol_data
        operations) ;
  register0 S.Helpers.Preapply.operations (fun (chain_store, block) () ops ->
      Store.Block.context chain_store block
      >>=? fun ctxt ->
      let predecessor = Store.Block.hash block in
      let header = Store.Block.shell_header block in
      let predecessor_context = Shell_context.wrap_disk_context ctxt in
      Next_proto.begin_construction
        ~chain_id:(Store.Chain.chain_id chain_store)
        ~predecessor_context
        ~predecessor_timestamp:header.timestamp
        ~predecessor_level:header.level
        ~predecessor_fitness:header.fitness
        ~predecessor
        ~timestamp:(Time.System.to_protocol (Systime_os.now ()))
        ()
      >>=? fun state ->
      fold_left_s
        (fun (state, acc) op ->
          Next_proto.apply_operation state op
          >>=? fun (state, result) ->
          return (state, (op.protocol_data, result) :: acc))
        (state, [])
        ops
      >>=? fun (state, acc) ->
      Next_proto.finalize_block state >>=? fun _ -> return (List.rev acc)) ;
  register1 S.Helpers.complete (fun (chain_store, block) prefix () () ->
      Store.Block.context chain_store block
      >>=? fun ctxt ->
      Base58.complete prefix
      >>= fun l1 ->
      let ctxt = Shell_context.wrap_disk_context ctxt in
      Next_proto.complete_b58prefix ctxt prefix >>= fun l2 -> return (l1 @ l2)) ;
  (* merge protocol rpcs... *)
  merge
    (RPC_directory.map
       (fun (chain_store, block) ->
         let hash = Store.Block.hash block in
         let header = Store.Block.header block in
         Lwt.return (chain_store, hash, header))
       (build_raw_header_rpc_directory (module Proto))) ;
  merge
    (RPC_directory.map
       (fun (chain_store, block) ->
         Store.Block.context_exn chain_store block
         >|= fun context ->
         let context = Shell_context.wrap_disk_context context in
         {
           Tezos_protocol_environment.block_hash = Store.Block.hash block;
           block_header = Store.Block.shell_header block;
           context;
         })
       Next_proto.rpc_services) ;
  !dir

let get_protocol hash =
  match Registered_protocol.get hash with
  | None ->
      raise Not_found
  | Some protocol ->
      protocol

let get_directory ~user_activated_upgrades ~user_activated_protocol_overrides
    chain_store block =
  Store.Chain.get_rpc_directory chain_store block
  >>= function
  | Some dir ->
      Lwt.return dir
  | None -> (
      Store.Block.protocol_hash_exn chain_store block
      >>= fun next_protocol_hash ->
      let (module Next_proto) = get_protocol next_protocol_hash in
      let build_fake_rpc_directory () =
        build_raw_rpc_directory
          ~user_activated_upgrades
          ~user_activated_protocol_overrides
          (module Block_services.Fake_protocol)
          (module Next_proto)
      in
      if Store.Block.is_genesis chain_store (Store.Block.hash block) then
        Lwt.return (build_fake_rpc_directory ())
      else
        Store.Block.read_predecessor_opt chain_store block
        >>= (function
              | None ->
                  (* No predecessors (e.g. pruned caboose), return the
                     current protocol *)
                  Lwt.return (module Next_proto : Registered_protocol.T)
              | Some pred ->
                  Store.Chain.savepoint chain_store
                  >>= fun (_, savepoint_level) ->
                  ( if Compare.Int32.(Store.Block.level pred < savepoint_level)
                  then
                    Store.Chain.find_protocol_level
                      chain_store
                      (Store.Block.proto_level pred)
                    >>= fun predecessor_protocol ->
                    let (_, protocol_hash, _) =
                      Option.unopt_exn Not_found predecessor_protocol
                    in
                    Lwt.return protocol_hash
                  else Store.Block.protocol_hash_exn chain_store pred )
                  >>= fun protocol_hash ->
                  Lwt.return (get_protocol protocol_hash))
        >>= fun (module Proto) ->
        Store.Chain.get_rpc_directory chain_store block
        >>= function
        | Some dir ->
            Lwt.return dir
        | None ->
            let dir =
              build_raw_rpc_directory
                ~user_activated_upgrades
                ~user_activated_protocol_overrides
                (module Proto)
                (module Next_proto)
            in
            Store.Chain.set_rpc_directory chain_store Proto.hash dir
            >>= fun () -> Lwt.return dir )

let get_block chain_store = function
  | `Genesis ->
      Store.Chain.genesis_block chain_store
      >>= fun block -> Lwt.return_some block
  | `Head n ->
      Store.Chain.current_head chain_store
      >>= fun current_head ->
      if n < 0 then Lwt.return_none
      else if n = 0 then Lwt.return_some current_head
      else
        Store.Block.read_block_opt
          chain_store
          ~distance:n
          (Store.Block.hash current_head)
  (* TODO ~below_save_point:true *)
  | (`Alias (_, n) | `Hash (_, n)) as b ->
      ( match b with
      | `Alias (`Checkpoint, _) ->
          Store.Chain.checkpoint chain_store
          >>= fun (checkpoint_hash, _) -> Lwt.return checkpoint_hash
      | `Alias (`Savepoint, _) ->
          Store.Chain.savepoint chain_store
          >>= fun (savepoint_hash, _) -> Lwt.return savepoint_hash
      | `Alias (`Caboose, _) ->
          Store.Chain.caboose chain_store
          >>= fun (caboose_hash, _) -> Lwt.return caboose_hash
      | `Hash (h, _) ->
          Lwt.return h )
      >>= fun hash ->
      if n < 0 then
        Store.Block.read_block_opt chain_store hash
        >>= function
        | None ->
            Lwt.fail Not_found
        | Some block ->
            Store.Chain.current_head chain_store
            >>= fun current_head ->
            let head_level = Store.Block.level current_head in
            let block_level = Store.Block.level block in
            let distance =
              Int32.(to_int (sub head_level (sub block_level (of_int n))))
            in
            if distance < 0 then Lwt.return_none
            else
              Store.Block.read_block_opt
                chain_store
                ~distance
                (Store.Block.hash current_head)
      else if n = 0 then Store.Block.read_block_opt chain_store hash
      else Store.Block.read_block_opt chain_store ~distance:n hash
  | `Level i ->
      if Compare.Int32.(i < 0l) then Lwt.fail Not_found
      else Store.Block.read_block_by_level_opt chain_store i

let build_rpc_directory ~user_activated_upgrades
    ~user_activated_protocol_overrides chain_store block =
  get_block chain_store block
  >>= function
  | None ->
      Lwt.fail Not_found
  | Some b ->
      get_directory
        ~user_activated_upgrades
        ~user_activated_protocol_overrides
        chain_store
        b
      >>= fun dir ->
      Lwt.return (RPC_directory.map (fun _ -> Lwt.return (chain_store, b)) dir)
