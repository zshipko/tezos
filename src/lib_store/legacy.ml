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

(* Hardcoded networks data*)
module Hardcoded = struct
  type network = {name : Distributed_db_version.Name.t; cycle_length : int}

  let proj (name, cycle_length) = {name; cycle_length}

  (* Hardcoded cycle length *)
  let supported_networks =
    let open Distributed_db_version.Name in
    List.map
      proj
      [ (of_string "TEZOS_MAINNET", 4096);
        (of_string "TEZOS_ALPHANET_CARTHAGE_2019-11-28T13:02:13Z", 2048);
        (of_string "TEZOS", 8) ]

  let cycle_length ~chain_name =
    List.find_map
      (fun {name; cycle_length} ->
        if chain_name = name then Some cycle_length else None)
      supported_networks
    |> Option.unopt_assert ~loc:__POS__

  let check_network ~chain_name =
    if
      not (List.exists (fun {name; _} -> chain_name = name) supported_networks)
    then
      failwith
        "Cannot perform operation for chain_name %a. Only %a are supported."
        Distributed_db_version.Name.pp
        chain_name
        (Format.pp_print_list
           ~pp_sep:(fun ppf () -> Format.fprintf ppf ", ")
           (fun ppf {name; _} ->
             Format.fprintf ppf "%a" Distributed_db_version.Name.pp name))
        supported_networks
    else return_unit

  (* Aims to tell if the checkpoint set and a cimentage is needed*)
  let may_update_checkpoint ~cycle_length nb_blocks = nb_blocks = cycle_length
end

(* Legacy store conversion *)

type error += Failed_to_convert_protocol of Protocol_hash.t

let () =
  register_error_kind
    `Permanent
    ~id:"failed_to_convert_protocol"
    ~title:"Failed to convert protocol"
    ~description:"Failed to convert protocol from legacy store."
    ~pp:(fun ppf p ->
      Format.fprintf
        ppf
        "Failed to convert protocol %a from legacy store."
        Protocol_hash.pp
        p)
    Data_encoding.(obj1 (req "protocol_hash" Protocol_hash.encoding))
    (function Failed_to_convert_protocol p -> Some p | _ -> None)
    (fun p -> Failed_to_convert_protocol p)

module Event = struct
  include Internal_event.Simple

  let section = ["node"; "legacy"; "upgrade"]

  let level = Internal_event.Notice

  let restoring_after_failure =
    Internal_event.Simple.declare_1
      ~level
      ~section
      ~name:"restoring_after_failure"
      ~msg:
        "Cleaning directory {directory} because of failure: restoring the \
         store backup."
      ("directory", Data_encoding.string)

  let advertise_upgrade_mode =
    Internal_event.Simple.declare_1
      ~level
      ~section
      ~name:"advertise_upgrade_mode"
      ~msg:"Upgrading storage in {history_mode}."
      ("history_mode", Data_encoding.string)
end

let make_block_repr ~read_metadata ~write_metadata lmdb_block_store hash =
  Legacy_store.Block.Operations.bindings (lmdb_block_store, hash)
  >>= fun ops ->
  Legacy_store.Block.Operations_metadata.bindings (lmdb_block_store, hash)
  >>= fun ops_met ->
  let operations = List.map snd ops in
  let operations_metadata = List.map snd ops_met in
  match read_metadata with
  | true ->
      Legacy_store.Block.Contents.read (lmdb_block_store, hash)
      >>=? fun block_contents ->
      let ({ header;
             message;
             max_operations_ttl;
             last_allowed_fork_level;
             metadata;
             _ }
            : Legacy_store.Block.contents) =
        block_contents
      in
      let contents = ({header; operations} : Block_repr.contents) in
      let metadata =
        if write_metadata then
          Some
            ( {
                message;
                max_operations_ttl;
                last_allowed_fork_level;
                block_metadata = metadata;
                operations_metadata;
              }
              : Block_repr.metadata )
        else None
      in
      return ({hash; contents; metadata} : Block_repr.t)
  | false ->
      assert (read_metadata = write_metadata) ;
      Legacy_store.Block.Pruned_contents.read (lmdb_block_store, hash)
      >>=? fun header ->
      let contents =
        ({header = header.header; operations} : Block_repr.contents)
      in
      return ({hash; contents; metadata = None} : Block_repr.t)

(* Note that the blocks are processed from head to genesis.
   Thus, the proto levels are in an decreasing order *)
let may_update_protocol_table lmdb_block_store chain_store prev_block
    proto_level =
  if
    (not (proto_level = Block_repr.proto_level prev_block))
    || Block_repr.level prev_block = 2l
  then
    Legacy_store.Chain.Protocol_info.bindings lmdb_block_store
    >>= fun protocol_table ->
    let (proto_hash, _transition_level)
          : Legacy_store.Chain.Protocol_info.value =
      List.assoc (Block_repr.proto_level prev_block) protocol_table
    in
    Store.Chain.set_protocol_level
      chain_store
      (Block_repr.proto_level prev_block)
      (Store.Block.of_repr prev_block, proto_hash)
  else return_unit

let import_floating lmdb_block_store chain_store ?(read_metadata = true)
    ?(write_metadata = read_metadata) block_hash limit =
  (* TODO: Make it tail rec ? if yes, we must be able to store a block
   without searching for its predecessors*)
  let block_store = Store.unsafe_get_block_store chain_store in
  make_block_repr ~read_metadata ~write_metadata lmdb_block_store block_hash
  >>=? fun block ->
  let nb_floating_blocks =
    Int32.(to_int (succ (sub (Block_repr.level block) limit)))
  in
  Lwt_utils_unix.display_progress
    ~pp_print_step:(fun fmt i ->
      Format.fprintf
        fmt
        "Converting floating blocks: %d/%d"
        i
        nb_floating_blocks)
    (fun notify ->
      let rec aux ~prev_block block =
        (* At protocol change, update the protocol_table*)
        may_update_protocol_table
          lmdb_block_store
          chain_store
          prev_block
          (Block_repr.proto_level block)
        >>=? fun () ->
        if Block_repr.level block = limit then
          notify ()
          >>= fun () ->
          Block_store.store_block block_store block >>= fun () -> return_unit
        else
          notify ()
          >>= fun () ->
          make_block_repr
            ~read_metadata
            ~write_metadata
            lmdb_block_store
            (Block_repr.predecessor block)
          >>=? fun predecessor_block ->
          aux ~prev_block:block predecessor_block
          >>=? fun savepoint ->
          Block_store.store_block block_store block
          >>= fun () -> return savepoint
      in
      aux ~prev_block:block block)

let import_cemented ?(display_msg = "") lmdb_chain_store chain_store
    cycle_length ?(with_metadata = true) ~start_block ~end_limit =
  (* We assume that the limits are "on cycles" *)
  assert (
    Int32.(to_int (sub (Block_repr.level start_block) 1l)) mod cycle_length = 0
  ) ;
  let nb_cemented_cycles =
    let cycles =
      Int32.(to_int (sub (Block_repr.level start_block) end_limit))
      / cycle_length
    in
    if end_limit = 1l then succ cycles else cycles
  in
  Lwt_utils_unix.display_progress
    ~pp_print_step:(fun fmt i ->
      Format.fprintf
        fmt
        "Converting cemented blocks%s: %d/%d"
        display_msg
        i
        nb_cemented_cycles)
    (fun notify ->
      let rec aux ~prev_block (last_checkpoint : Block_header.t) acc hash =
        make_block_repr
          ~read_metadata:with_metadata
          ~write_metadata:with_metadata
          lmdb_chain_store
          hash
        >>=? fun block ->
        (* At protocol change, update the protocol_table*)
        (* let prev_proto_level = Block_repr.proto_level block_repr in *)
        may_update_protocol_table
          lmdb_chain_store
          chain_store
          prev_block
          (Block_repr.proto_level block)
        >>=? fun () ->
        let new_acc = block :: acc in
        if Block_repr.level block = end_limit then
          (* Special case to cement [genesis;1] *)
          (*Reading genesis which is not pruned in lmdb *)
          if end_limit = 1l then (
            assert (List.length acc = 0) ;
            make_block_repr
              ~read_metadata:true
              ~write_metadata:false
              lmdb_chain_store
              (Block_repr.predecessor block)
            >>=? fun genesis ->
            Store.cement_blocks_chunk
              ~check_consistency:false
              chain_store
              [genesis; block]
              ~write_metadata:with_metadata
            >>=? fun () -> notify () >>= fun () -> return_unit )
          else (
            assert (List.length acc = 0) ;
            return_unit )
        else if
          Hardcoded.may_update_checkpoint ~cycle_length (List.length new_acc)
        then
          Store.cement_blocks_chunk
            ~check_consistency:false
            chain_store
            new_acc
            ~write_metadata:with_metadata
          >>=? fun () ->
          notify ()
          >>= fun () ->
          aux
            ~prev_block:block
            (Block_repr.header block)
            []
            (Block_repr.predecessor block)
        else
          aux
            ~prev_block:block
            last_checkpoint
            (block :: acc)
            (Block_repr.predecessor block)
      in
      aux
        ~prev_block:start_block
        (Block_repr.header start_block)
        [start_block]
        (Block_repr.predecessor start_block))

(* … FIXME *)
let dirty_read_i lmdb_chain_store hash target_level =
  let rec aux hash =
    Legacy_store.Block.Pruned_contents.read_opt (lmdb_chain_store, hash)
    >>= (function
          | Some {header} ->
              return header
          | None ->
              Legacy_store.Block.Contents.read (lmdb_chain_store, hash)
              >>=? fun {header; _} -> return header)
    >>=? fun header ->
    let level = header.Block_header.shell.level in
    if level = target_level then return header
    else
      let pred = header.shell.predecessor in
      aux pred
  in
  aux hash

let archive_import lmdb_chain_store chain_store cycle_length checkpoint
    current_head_hash =
  let checkpoint_hash = Block_header.hash checkpoint in
  (* FIXME on doit pas partir du checkpoint mais du caboose *)
  (* On doit màj le cp quand même *)
  make_block_repr
    ~read_metadata:true
    ~write_metadata:true
    lmdb_chain_store
    checkpoint_hash
  >>=? fun start_block ->
  import_cemented
    ~display_msg:" (with metadata)"
    lmdb_chain_store
    chain_store
    cycle_length
    ~with_metadata:true
    ~start_block
    ~end_limit:1l
  >>=? fun () ->
  import_floating
    lmdb_chain_store
    chain_store
    current_head_hash
    (Int32.succ checkpoint.shell.level)
  >>=? fun () ->
  let new_checkpoint = (checkpoint_hash, checkpoint.shell.level) in
  let genesis = Store.Chain.genesis chain_store in
  let new_caboose = (genesis.block, 0l) in
  let new_savepoint = new_caboose in
  return (new_checkpoint, new_savepoint, new_caboose)

(* As the lmdb store is not compatible with a Full 5, it is upgraded as
   a Full 0. It will converge to a Full 5 afterward.*)
let full_import lmdb_chain_store chain_store cycle_length checkpoint
    current_head_hash =
  let checkpoint_hash = Block_header.hash checkpoint in
  (* As checkpoint = savepoint in lmbd, we cannot set a consistent savepoint *)
  (* savepoint = checkpoint - max_op_ttl(checkpoint)*)
  make_block_repr
    ~read_metadata:true
    ~write_metadata:true
    lmdb_chain_store
    checkpoint_hash
  >>=? fun checkpoint_block_repr ->
  let checkpoint_maxopttl =
    match Block_repr.metadata checkpoint_block_repr with
    | None ->
        assert false
    | Some m ->
        Block_repr.max_operations_ttl m
  in
  let savepoint_level =
    Int32.(sub checkpoint.shell.level (of_int checkpoint_maxopttl))
  in
  dirty_read_i lmdb_chain_store current_head_hash savepoint_level
  >>=? fun savepoint_header ->
  let checkpoint_level = checkpoint.shell.level in
  let lowest_cemented_level =
    Int32.(
      sub
        checkpoint_level
        (of_int (History_mode.default_offset * cycle_length)))
  in
  dirty_read_i lmdb_chain_store current_head_hash lowest_cemented_level
  >>=? fun lowest_cemented_header ->
  make_block_repr
    ~read_metadata:false
    ~write_metadata:false
    lmdb_chain_store
    (Block_header.hash lowest_cemented_header)
  >>=? fun lowest_cemented_block_repr ->
  (* First, import cemented [1l;lowest_cemented] without metadata*)
  import_cemented
    lmdb_chain_store
    chain_store
    cycle_length
    ~with_metadata:false
    ~start_block:lowest_cemented_block_repr
    ~end_limit:1l
  >>=? fun () ->
  (* Then, import cemented ]lowest_cemented;checkpoint] with metadata*)
  import_cemented
    ~display_msg:" (with metadata)"
    lmdb_chain_store
    chain_store
    cycle_length
    ~with_metadata:false
    ~start_block:checkpoint_block_repr
    ~end_limit:lowest_cemented_level
  >>=? fun () ->
  import_floating
    lmdb_chain_store
    chain_store
    current_head_hash
    (Int32.succ checkpoint.shell.level)
  >>=? fun () ->
  let new_checkpoint = (checkpoint_hash, checkpoint_level) in
  let genesis = Store.Chain.genesis chain_store in
  let new_caboose = (genesis.block, 0l) in
  let new_savepoint =
    (Block_header.hash savepoint_header, savepoint_header.shell.level)
  in
  return (new_checkpoint, new_savepoint, new_caboose)

(* As the lmdb store is not compatible with a Rolling 5, it is upgraded as
   a Rolling 0. It will converge to a Rolling 5 afterward.*)
let rolling_import lmdb_chain_store chain_store checkpoint current_head_hash =
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  Legacy_store.Chain_data.Save_point.read lmdb_chain_data
  >>=? fun (lmdb_savepoint_level, lmdb_savepoint_hash) ->
  Legacy_store.Chain_data.Caboose.read lmdb_chain_data
  >>=? fun (lmdb_caboose_level, lmdb_caboose_hash) ->
  (* Importing blocks [lmdb_caboose] in floating without metadata *)
  import_floating
    lmdb_chain_store
    chain_store
    ~read_metadata:false
    ~write_metadata:false
    lmdb_caboose_hash
    lmdb_caboose_level
  >>=? fun () ->
  make_block_repr
    ~read_metadata:true
    ~write_metadata:false
    lmdb_chain_store
    lmdb_savepoint_hash
  >>=? fun lmdb_savepoint_header ->
  (* Importing blocks [lmdb_caboose;lmdb_savepoint[ in floating without metadata *)
  import_floating
    lmdb_chain_store
    chain_store
    ~read_metadata:false
    (Block_repr.predecessor lmdb_savepoint_header)
    (Int32.succ lmdb_caboose_level)
  >>=? fun () ->
  (* Importing blocks [lmdb_savepoint] in floating without metadata *)
  import_floating
    lmdb_chain_store
    chain_store
    ~read_metadata:true
    ~write_metadata:false
    lmdb_savepoint_hash
    lmdb_savepoint_level
  >>=? fun () ->
  let checkpoint_hash = Block_header.hash checkpoint in
  let checkpoint_level = checkpoint.Block_header.shell.level in
  (* Importing blocks ]savepoint;current_head] in floating with metadata *)
  import_floating
    lmdb_chain_store
    ~read_metadata:true
    chain_store
    current_head_hash
    (Int32.succ lmdb_savepoint_level)
  >>=? fun () ->
  let new_caboose = (lmdb_caboose_hash, lmdb_caboose_level) in
  let new_savepoint = new_caboose in
  let new_checkpoint = (checkpoint_hash, checkpoint_level) in
  return (new_checkpoint, new_savepoint, new_caboose)

let infer_checkpoint lmdb_chain_data cycle_length =
  (* When upgrading from a full or rolling node, the checkpoint may not be set
   on a "real checkpoint".*)
  Legacy_store.Chain_data.Checkpoint.read lmdb_chain_data
  >>=? fun checkpoint ->
  let checkpoint_level = Int32.to_int checkpoint.shell.level in
  let modulo = (checkpoint_level - 1) mod cycle_length in
  if modulo = 0 then return checkpoint
  else
    failwith
      "The storage to upgrade is using a wrong checkpoint (%a at level %d). \
       Please use a storage based on a checkpoint defined by the protocol."
      Block_hash.pp
      (Block_header.hash checkpoint)
      checkpoint_level

let import_blocks lmdb_chain_store chain_store cycle_length checkpoint
    history_mode =
  (* TODO: add progression or something*)
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  Legacy_store.Chain_data.Current_head.read lmdb_chain_data
  >>=? fun current_head_hash ->
  ( match (history_mode : History_mode.t) with
  | Archive ->
      archive_import
        lmdb_chain_store
        chain_store
        cycle_length
        checkpoint
        current_head_hash
  | Full _ ->
      full_import
        lmdb_chain_store
        chain_store
        cycle_length
        checkpoint
        current_head_hash
  | Rolling _ ->
      rolling_import lmdb_chain_store chain_store checkpoint current_head_hash
  )
  >>=? fun (new_checkpoint, new_savepoint, new_caboose) ->
  return (new_checkpoint, new_savepoint, new_caboose)

let import_protocols history_mode lmdb_store lmdb_chain_store store =
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  match (history_mode : History_mode.t) with
  | Archive | Full _ ->
      Legacy_store.Protocol.Contents.bindings lmdb_store
      >>= fun proto_list ->
      Error_monad.iter_s
        (fun (h, p) ->
          Store.Protocol.store_protocol store h p
          >>= function
          | Some expected_hash ->
              fail_unless
                (Protocol_hash.equal expected_hash h)
                (Failed_to_convert_protocol h)
          | None ->
              fail (Failed_to_convert_protocol h) >>=? fun () -> return_unit)
        proto_list
  | Rolling _ ->
      Legacy_store.Chain_data.Current_head.read lmdb_chain_data
      >>=? fun current_head_hash ->
      Legacy_store.Block.Contents.read (lmdb_chain_store, current_head_hash)
      >>=? fun current_head ->
      Legacy_store.Chain_data.Caboose.read lmdb_chain_data
      >>=? fun (_lmdb_caboose_level, lmdb_caboose_hash) ->
      (* We store the oldest known protocol and we assume that its
         transition_header is the caboose *)
      Legacy_store.Block.Pruned_contents.read
        (lmdb_chain_store, lmdb_caboose_hash)
      >>=? fun {header = transition_header; _} ->
      let protocol_level = current_head.header.shell.proto_level in
      Legacy_store.Chain.Protocol_info.read lmdb_chain_store protocol_level
      >>=? fun protocol_info ->
      let protocol_hash = fst protocol_info in
      let chain_store = Store.main_chain_store store in
      let transition_hash = Block_header.hash transition_header in
      make_block_repr
        ~read_metadata:false
        ~write_metadata:false
        lmdb_chain_store
        transition_hash
      >>=? fun transition_block ->
      Store.Chain.set_protocol_level
        chain_store
        protocol_level
        (Store.Block.of_repr transition_block, protocol_hash)

let update_stored_data lmdb_chain_data chain_store ~new_checkpoint
    ~new_savepoint ~new_caboose =
  Legacy_store.Chain_data.Current_head.read lmdb_chain_data
  >>=? fun current_head ->
  Store.Block.read_block chain_store current_head
  >>=? fun current_head ->
  Store.Chain.re_store
    chain_store
    ~head:current_head
    ~checkpoint:new_checkpoint
    ~savepoint:new_savepoint
    ~caboose:new_caboose

let new_store_name = "store"

let lmdb_store_name = "lmdb_store_to_remove"

let upgrade_cleaner data_dir =
  Event.(emit restoring_after_failure) data_dir
  >>= fun () ->
  let new_store = Naming.(data_dir // new_store_name) in
  let lmdb_backup = Naming.(data_dir // lmdb_store_name) in
  Lwt_utils_unix.remove_dir new_store
  >>= fun () ->
  Unix.rename lmdb_backup new_store ;
  Lwt.return_unit

let upgrade_0_0_4 ~data_dir genesis patch_context
    ~(chain_name : Distributed_db_version.Name.t) =
  Hardcoded.check_network ~chain_name
  >>=? fun () ->
  let ( // ) = Filename.concat in
  let old_lmdb_path = data_dir // new_store_name in
  let new_lmdb_path = data_dir // lmdb_store_name in
  Unix.rename old_lmdb_path new_lmdb_path ;
  Lwt.try_bind
    (fun () ->
      Legacy_store.init ~readonly:false new_lmdb_path
      >>=? fun lmdb_store ->
      let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
      let lmdb_chain_store = Legacy_store.Chain.get lmdb_store chain_id in
      let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
      let store_root = data_dir // "store" in
      let context_root = data_dir // "context" in
      Legacy_store.Configuration.History_mode.read lmdb_store
      >>=? fun legacy_history_mode ->
      let history_mode = History_mode.convert legacy_history_mode in
      Event.(
        emit
          advertise_upgrade_mode
          (Format.asprintf "%a" History_mode.pp history_mode))
      >>= fun () ->
      Store.init
        ?patch_context
        ~store_dir:store_root
        ~context_dir:context_root
        ~history_mode
        ~allow_testchains:true
        genesis
      >>=? fun store ->
      let cycle_length = Hardcoded.cycle_length ~chain_name in
      infer_checkpoint lmdb_chain_data cycle_length
      >>=? fun checkpoint ->
      let chain_store = Store.main_chain_store store in
      import_protocols history_mode lmdb_store lmdb_chain_store store
      >>=? fun () ->
      import_blocks
        lmdb_chain_store
        chain_store
        cycle_length
        checkpoint
        history_mode
      >>=? fun (new_checkpoint, new_savepoint, new_caboose) ->
      update_stored_data
        lmdb_chain_data
        chain_store
        ~new_checkpoint
        ~new_savepoint
        ~new_caboose
      >>=? fun () ->
      Legacy_store.close lmdb_store ;
      Store.close_store store >>= fun () -> return_unit)
    (function
      | Ok () ->
          return
            (Format.sprintf
               "You can now safely remove the old store located at: %s"
               new_lmdb_path)
      | Error errors ->
          upgrade_cleaner data_dir >>= fun () -> Lwt.return (Error errors))
    (fun exn -> upgrade_cleaner data_dir >>= fun () -> Lwt.fail exn)
