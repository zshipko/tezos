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

(* Hardcoded networks data *)
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

  (* Tells if the setting the checkpoint requires to cement blocks *)
  let may_update_checkpoint ~cycle_length nb_blocks = nb_blocks = cycle_length
end

(* Legacy store conversion *)

type error += Failed_to_convert_protocol of Protocol_hash.t

(* TODO: Better way to handle errors ? *)
type error += Failed_to_upgrade of string

let () =
  register_error_kind
    `Permanent
    ~id:"failed_to_convert_protocol"
    ~title:"Failed to convert protocol"
    ~description:"Failed to convert protocol from legacy store."
    ~pp:(fun ppf ->
      Format.fprintf
        ppf
        "Failed to convert protocol %a from legacy store."
        Protocol_hash.pp)
    Data_encoding.(obj1 (req "protocol_hash" Protocol_hash.encoding))
    (function Failed_to_convert_protocol p -> Some p | _ -> None)
    (fun p -> Failed_to_convert_protocol p) ;
  register_error_kind
    `Permanent
    ~id:"failed_to_upgrade"
    ~title:"Failed to upgrade"
    ~description:"Failed to upgrade the store."
    ~pp:(fun ppf -> Format.fprintf ppf "Failed to upgrade the store: %s.")
    Data_encoding.(obj1 (req "msg" string))
    (function Failed_to_upgrade s -> Some s | _ -> None)
    (fun s -> Failed_to_upgrade s)

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
    Internal_event.Simple.declare_2
      ~level
      ~section
      ~name:"advertise_upgrade_mode"
      ~msg:"Upgrading storage from \"{old_hm}\" to {new_hm}."
      ("old_hm", Data_encoding.string)
      ("new_hm", Data_encoding.string)
end

(* Aims to build a Store.Block.block_repr from reading the data from a
   legacy store *)
let rec make_block_repr ?(flex = false) ~read_metadata ~write_metadata
    lmdb_block_store hash =
  Legacy_store.Block.Operations.bindings (lmdb_block_store, hash)
  >>= fun ops ->
  Legacy_store.Block.Operations_metadata.bindings (lmdb_block_store, hash)
  >>= fun ops_met ->
  let operations = List.map snd ops in
  let operations_metadata = List.map snd ops_met in
  match read_metadata with
  | true -> (
      Legacy_store.Block.Contents.read_opt (lmdb_block_store, hash)
      >>= function
      | Some block_contents ->
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
      | None ->
          if flex then
            make_block_repr
              ~read_metadata:false
              ~write_metadata:false
              lmdb_block_store
              hash
          else failwith "Failed to read block contents: %a." Block_hash.pp hash
      )
  | false -> (
      assert (read_metadata = write_metadata) ;
      Legacy_store.Block.Pruned_contents.read_opt (lmdb_block_store, hash)
      >>= function
      | Some header ->
          let contents =
            ({header = header.header; operations} : Block_repr.contents)
          in
          return ({hash; contents; metadata = None} : Block_repr.t)
      | None ->
          if flex then
            make_block_repr
              ~read_metadata:true
              ~write_metadata:true
              lmdb_block_store
              hash
          else failwith "Failed to read pruned block: %a." Block_hash.pp hash )

(* Updates the protocol table. Inserts entry when the proto_level of
   [prev_block] differs from [proto_level] (which is the protocole
   level of the successor of [prev_block]). *)
let may_update_protocol_table lmdb_block_store chain_store prev_block
    proto_level =
  if not (proto_level = Block_repr.proto_level prev_block) then
    Legacy_store.Chain.Protocol_info.bindings lmdb_block_store
    >>= fun protocol_table ->
    let (proto_hash, _transition_level)
          : Legacy_store.Chain.Protocol_info.value =
      List.assoc (Block_repr.proto_level prev_block) protocol_table
    in
    Store.Chain.may_update_protocol_level
      chain_store
      ~protocol_level:(Block_repr.proto_level prev_block)
      (Store.Unsafe.block_of_repr prev_block, proto_hash)
  else return_unit

let read_i lmdb_store block_hash i =
  Legacy_store.Block.Pruned_contents.known (lmdb_store, block_hash)
  >>= fun below_savepoint ->
  ( if below_savepoint then
    Legacy_store.Block.Pruned_contents.read (lmdb_store, block_hash)
    >>=? fun {header; _} -> return header
  else
    Legacy_store.Block.Contents.read (lmdb_store, block_hash)
    >>=? fun {header; _} -> return header )
  >>=? fun block ->
  let target = Int32.(to_int (sub block.shell.level i)) in
  Legacy_state.predecessor_n_raw lmdb_store block_hash target
  >>= function
  | Some h ->
      Legacy_store.Block.Pruned_contents.known (lmdb_store, h)
      >>= fun below_savepoint ->
      if below_savepoint then
        Legacy_store.Block.Pruned_contents.read (lmdb_store, h)
        >>=? fun {header; _} -> return header
      else
        Legacy_store.Block.Contents.read (lmdb_store, h)
        >>=? fun {header; _} -> return header
  | None ->
      failwith "Failed to find block at level %ld" i

(* Reads, from the legacy lmdb store, the blocks from [block_hash] to [limit]
   and store them in the floating store *)
let import_floating ?(flex = false) lmdb_block_store chain_store ~read_metadata
    ~write_metadata end_block_hash limit =
  let block_store = Store.Unsafe.get_block_store chain_store in
  read_i lmdb_block_store end_block_hash limit
  >>=? fun start_header ->
  make_block_repr
    ~flex
    ~read_metadata
    ~write_metadata
    lmdb_block_store
    (Block_header.hash start_header)
  >>=? fun start_block ->
  make_block_repr
    ~flex
    ~read_metadata
    ~write_metadata
    lmdb_block_store
    start_header.shell.predecessor
  >>=? fun pred_block ->
  make_block_repr
    ~flex
    ~read_metadata
    ~write_metadata
    lmdb_block_store
    end_block_hash
  >>=? fun end_block ->
  let end_limit = Block_repr.level end_block in
  let nb_floating_blocks =
    Int32.(to_int (succ (sub (Block_repr.level end_block) limit)))
  in
  Lwt_utils_unix.display_progress
    ~pp_print_step:(fun fmt i ->
      Format.fprintf
        fmt
        "Converting floating blocks: %d/%d"
        i
        nb_floating_blocks)
    (fun notify ->
      let rec aux ~pred_block block =
        Block_store.store_block block_store block
        >>= fun () ->
        let level = Block_repr.level block in
        if level >= end_limit then return_unit
        else
          (* At protocol change, update the protocol_table *)
          may_update_protocol_table
            lmdb_block_store
            chain_store
            pred_block
            (Block_repr.proto_level block)
          >>=? fun () ->
          read_i lmdb_block_store end_block_hash (Int32.succ level)
          >>=? fun next_block ->
          make_block_repr
            ~flex
            ~read_metadata
            ~write_metadata
            lmdb_block_store
            (Block_header.hash next_block)
          >>=? fun next_block_repr ->
          notify () >>= fun () -> aux ~pred_block:block next_block_repr
      in
      aux ~pred_block start_block)

(* Reads, from the legacy lmdb store, the blocks from [start_block] to
   [end_limit] and store them in the cemented block store *)
let import_cemented ?(display_msg = "") lmdb_chain_store chain_store
    cycle_length ?(with_metadata = true) ~start_block ~end_limit =
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
        may_update_protocol_table
          lmdb_chain_store
          chain_store
          prev_block
          (Block_repr.proto_level block)
        >>=? fun () ->
        let new_acc = block :: acc in
        if Block_repr.level block = end_limit then
          (* Reading genesis which is not pruned in lmdb *)
          if end_limit = 1l then
            make_block_repr
              ~read_metadata:true
              ~write_metadata:false
              lmdb_chain_store
              (Block_repr.predecessor block)
            >>=? fun genesis ->
            may_update_protocol_table
              lmdb_chain_store
              chain_store
              block
              (Block_repr.proto_level genesis)
            >>=? fun () ->
            let block_store = Store.Unsafe.get_block_store chain_store in
            Block_store.cement_blocks
              ~check_consistency:false
              block_store
              (genesis :: new_acc)
              ~write_metadata:with_metadata
            >>=? fun () -> notify () >>= fun () -> return_unit
          else (
            assert (List.length acc = 0) ;
            return_unit )
        else if
          Hardcoded.may_update_checkpoint ~cycle_length (List.length new_acc)
        then
          let block_store = Store.Unsafe.get_block_store chain_store in
          Block_store.cement_blocks
            ~check_consistency:false
            block_store
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

let archive_import lmdb_chain_store chain_store cycle_length
    (checkpoint, checkpoint_level) current_head_hash =
  let checkpoint_hash = Block_header.hash checkpoint in
  ( if checkpoint_level = 0l then
    (* Only the floating store should be imported *)
    import_floating
      ~read_metadata:true
      ~write_metadata:true
      lmdb_chain_store
      chain_store
      current_head_hash
      (Int32.succ checkpoint.shell.level)
  else
    make_block_repr
      ~read_metadata:true
      ~write_metadata:true
      lmdb_chain_store
      checkpoint_hash
    >>=? fun checkpoint_block ->
    import_cemented
      ~display_msg:" (with metadata)"
      lmdb_chain_store
      chain_store
      cycle_length
      ~with_metadata:true
      ~start_block:checkpoint_block
      ~end_limit:1l
    >>=? fun () ->
    import_floating
      ~read_metadata:true
      ~write_metadata:true
      lmdb_chain_store
      chain_store
      current_head_hash
      (Int32.succ checkpoint.shell.level) )
  >>=? fun () ->
  let new_checkpoint = (checkpoint_hash, checkpoint_level) in
  let genesis = Store.Chain.genesis chain_store in
  let new_caboose = (genesis.block, 0l) in
  let new_savepoint = new_caboose in
  return (new_checkpoint, new_savepoint, new_caboose)

(* As the lmdb store is not compatible with a Full 5, it is upgraded as
   a Full 0. It will converge to a Full 5 afterward. *)
let full_import lmdb_chain_store chain_store cycle_length
    (checkpoint, checkpoint_level) current_head_hash =
  let checkpoint_hash = Block_header.hash checkpoint in
  ( if checkpoint_level = 0l then
    (* Only the floating store should be imported *)
    import_floating
      ~flex:true
      ~read_metadata:false
      ~write_metadata:false
      lmdb_chain_store
      chain_store
      current_head_hash
      1l
  else
    make_block_repr
      ~flex:true
      ~read_metadata:true
      ~write_metadata:true
      lmdb_chain_store
      checkpoint_hash
    >>=? fun checkpoint_block_repr ->
    (* First, import cemented [1l;checkpoint] without metadata *)
    import_cemented
      lmdb_chain_store
      chain_store
      cycle_length
      ~with_metadata:false
      ~start_block:checkpoint_block_repr
      ~end_limit:1l
    >>=? fun () ->
    (* Then, import floating ]checkpoint;head] with metadata *)
    import_floating
      ~flex:true
      ~read_metadata:true
      ~write_metadata:true
      lmdb_chain_store
      chain_store
      current_head_hash
      (Int32.succ checkpoint.shell.level)
    >>=? fun () -> return_unit )
  >>=? fun () ->
  let new_checkpoint = (checkpoint_hash, checkpoint_level) in
  let genesis = Store.Chain.genesis chain_store in
  let new_caboose = (genesis.block, 0l) in
  let new_savepoint = new_checkpoint in
  return (new_checkpoint, new_savepoint, new_caboose)

(* As the lmdb store is not compatible with a Rolling 5, it is upgraded as
   a Rolling 0. It will converge to a Rolling 5 afterward. *)
let rolling_import lmdb_chain_store chain_store (checkpoint, checkpoint_level)
    current_head_hash =
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  let block_store = Store.Unsafe.get_block_store chain_store in
  Legacy_store.Chain_data.Save_point.read lmdb_chain_data
  >>=? fun (lmdb_savepoint_level, lmdb_savepoint_hash) ->
  Legacy_store.Chain_data.Caboose.read lmdb_chain_data
  >>=? fun (lmdb_caboose_level, _lmdb_caboose_hash) ->
  ( if checkpoint_level = 0l then
    (* Only the floating store should be imported *)
    import_floating
      ~flex:true
      ~read_metadata:true
      ~write_metadata:true
      lmdb_chain_store
      chain_store
      current_head_hash
      1l
  else
    make_block_repr
      ~flex:true
      ~read_metadata:true
      ~write_metadata:true
      lmdb_chain_store
      lmdb_savepoint_hash
    >>=? fun lmdb_savepoint_header ->
    (* Importing floating ]lmdb_caboose;lmdb_savepoint[ without metadata .
     The caboose is excluded as the lmdb store do not stores it. *)
    import_floating
      ~flex:true
      lmdb_chain_store
      chain_store
      ~read_metadata:false
      ~write_metadata:false
      (Block_repr.predecessor lmdb_savepoint_header)
      (Int32.succ lmdb_caboose_level)
    >>=? fun () ->
    (* Import the [savepoint] in floating without metadata *)
    Block_store.store_block block_store lmdb_savepoint_header
    >>= fun () ->
    (* Importing blocks ]savepoint;current_head] in floating with metadata *)
    import_floating
      ~flex:true
      lmdb_chain_store
      ~read_metadata:true
      ~write_metadata:true
      chain_store
      current_head_hash
      (Int32.succ lmdb_savepoint_level) )
  >>=? fun () ->
  read_i lmdb_chain_store current_head_hash (Int32.succ lmdb_caboose_level)
  >>=? fun new_caboose_header ->
  let new_caboose =
    if checkpoint_level = 0l then
      let genesis = Store.Chain.genesis chain_store in
      (genesis.block, 0l)
    else (Block_header.hash new_caboose_header, new_caboose_header.shell.level)
  in
  let checkpoint_hash = Block_header.hash checkpoint in
  let new_checkpoint = (checkpoint_hash, checkpoint_level) in
  let new_savepoint = new_checkpoint in
  return (new_checkpoint, new_savepoint, new_caboose)

let import_blocks lmdb_chain_store chain_store cycle_length checkpoint
    history_mode =
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

let import_protocols history_mode lmdb_store lmdb_chain_store store chain_id =
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  match (history_mode : History_mode.t) with
  | Archive | Full _ ->
      Legacy_store.Protocol.Contents.bindings lmdb_store
      >>= fun proto_list ->
      Error_monad.iter_s
        (fun (h, p) ->
          Store.Protocol.store store h p
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
      >>=? fun (lmdb_caboose_level, _lmdb_caboose_hash) ->
      (* We store the oldest known protocol and we assume that its
         transition_header is the caboose.
       * In LMDB, caboose is not stored! Thus, new_caboose = succ lmdb_caboose*)
      let lmdb_chain_store = Legacy_store.Chain.get lmdb_store chain_id in
      read_i lmdb_chain_store current_head_hash (Int32.succ lmdb_caboose_level)
      >>=? fun transition_header ->
      let protocol_level = current_head.header.shell.proto_level in
      Legacy_store.Chain.Protocol_info.read lmdb_chain_store protocol_level
      >>=? fun protocol_info ->
      let protocol_hash = fst protocol_info in
      let chain_store = Store.main_chain_store store in
      let transition_hash = Block_header.hash transition_header in
      ( if current_head.last_allowed_fork_level > transition_header.shell.level
      then
        make_block_repr
          ~read_metadata:false
          ~write_metadata:false
          lmdb_chain_store
          transition_hash
      else
        make_block_repr
          ~read_metadata:true
          ~write_metadata:false
          lmdb_chain_store
          transition_hash )
      >>=? fun transition_block ->
      Store.Chain.may_update_protocol_level
        chain_store
        ~protocol_level
        (Store.Unsafe.block_of_repr transition_block, protocol_hash)

let import_alternate_heads lmdb_chain_store lmdb_chain_data =
  Legacy_store.Chain_data.Checkpoint.read lmdb_chain_data
  >>=? fun checkpoint ->
  Legacy_store.Chain_data.Known_heads.elements lmdb_chain_data
  >>= fun bindings ->
  Error_monad.fold_left_s
    (fun map hash ->
      Legacy_store.Block.Contents.read (lmdb_chain_store, hash)
      >>=? fun {header; _} ->
      let level = header.shell.level in
      (* Ensure that alternate heads are valid *)
      if level <= checkpoint.shell.level then return map
      else return (Block_hash.Map.add hash level map))
    Block_hash.Map.empty
    bindings

let import_invalid_blocks lmdb_chain_store =
  Legacy_store.Block.Invalid_block.fold
    lmdb_chain_store
    ~init:Block_hash.Map.empty
    ~f:(fun hash ({level; errors} : Legacy_store.Block.invalid_block) map ->
      Lwt.return
        (Block_hash.Map.add
           hash
           ({level; errors} : Store_types.invalid_block)
           map))

let import_forked_chains lmdb_chain_store =
  Legacy_store.Forking_block_hash.fold
    lmdb_chain_store
    ~init:Chain_id.Map.empty
    ~f:(fun id hash map -> Lwt.return (Chain_id.Map.add id hash map))

let update_stored_data lmdb_chain_store old_store new_store ~new_checkpoint
    ~new_savepoint ~new_caboose genesis =
  let chain_store = Store.main_chain_store new_store in
  let store_dir = Store.directory new_store in
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  Legacy_store.Chain_data.Current_head.read lmdb_chain_data
  >>=? fun lmdb_head ->
  import_alternate_heads lmdb_chain_store lmdb_chain_data
  >>=? fun alternate_heads ->
  import_invalid_blocks lmdb_chain_store
  >>= fun invalid_blocks ->
  import_forked_chains old_store
  >>= fun forked_chains ->
  Store.Unsafe.restore_from_legacy_upgrade
    ~store_dir
    ~genesis
    alternate_heads
    invalid_blocks
    forked_chains
  >>=? fun () ->
  Store.Block.read_block chain_store lmdb_head
  >>=? fun new_head ->
  Store.Unsafe.set_head chain_store new_head
  >>=? fun () ->
  Store.Unsafe.set_checkpoint chain_store new_checkpoint
  >>=? fun () ->
  Store.Unsafe.set_savepoint chain_store new_savepoint
  >>=? fun () -> Store.Unsafe.set_caboose chain_store new_caboose

let infer_checkpoint old_store chain_id _cycle_length =
  (* When upgrading from a full or rolling node, the checkpoint may
     not be set on a "protocol defined checkpoint". We substitute it
     by using, as a checkpoint, the last allowed fork level of the
     current head. *)
  let lmdb_chain_store = Legacy_store.Chain.get old_store chain_id in
  let lmdb_chain_data = Legacy_store.Chain_data.get lmdb_chain_store in
  Legacy_store.Chain_data.Current_head.read lmdb_chain_data
  >>=? fun head_hash ->
  Legacy_store.Block.Contents.read (lmdb_chain_store, head_hash)
  >>=? fun head_contents ->
  fail_when
    (head_contents.header.shell.level = 0l)
    (Failed_to_upgrade "Nothing to do")
  >>=? fun () ->
  let lafl = head_contents.last_allowed_fork_level in
  read_i lmdb_chain_store head_hash lafl
  >>=? fun lafl_header -> return (lafl_header, lafl)

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

let raw_upgrade chain_name ~new_store ~old_store history_mode genesis =
  let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
  let lmdb_chain_store = Legacy_store.Chain.get old_store chain_id in
  let cycle_length = Hardcoded.cycle_length ~chain_name in
  infer_checkpoint old_store chain_id cycle_length
  >>=? fun checkpoint ->
  let chain_store = Store.main_chain_store new_store in
  import_protocols history_mode old_store lmdb_chain_store new_store chain_id
  >>=? fun () ->
  import_blocks
    lmdb_chain_store
    chain_store
    cycle_length
    checkpoint
    history_mode
  >>=? fun (new_checkpoint, new_savepoint, new_caboose) ->
  update_stored_data
    lmdb_chain_store
    old_store
    new_store
    ~new_checkpoint
    ~new_savepoint
    ~new_caboose
    genesis

let upgrade_0_0_4 ~data_dir ~patch_context
    ~(chain_name : Distributed_db_version.Name.t) genesis =
  Hardcoded.check_network ~chain_name
  >>=? fun () ->
  let ( // ) = Filename.concat in
  let old_lmdb_path = data_dir // new_store_name in
  let new_lmdb_path = data_dir // lmdb_store_name in
  ( try
      Unix.rename old_lmdb_path new_lmdb_path ;
      return_unit
    with _ ->
      fail
        (Failed_to_upgrade
           (Format.sprintf
              "Failed to locate the store to upgrade. Make sure that the give \
               path (%s) is correct."
              data_dir)) )
  >>=? fun () ->
  Lwt.try_bind
    (fun () ->
      Legacy_store.init ~readonly:false new_lmdb_path
      >>=? fun lmdb_store ->
      let store_root = data_dir // "store" in
      let context_root = data_dir // "context" in
      Legacy_store.Configuration.History_mode.read lmdb_store
      >>=? fun legacy_history_mode ->
      let history_mode = History_mode.convert legacy_history_mode in
      Event.(
        emit
          advertise_upgrade_mode
          ( Format.asprintf "%a" History_mode.Legacy.pp legacy_history_mode,
            Format.asprintf "%a" History_mode.pp history_mode ))
      >>= fun () ->
      Store.init
        ?patch_context
        ~store_dir:store_root
        ~context_dir:context_root
        ~history_mode
        ~allow_testchains:true
        genesis
      >>=? fun store ->
      raw_upgrade
        chain_name
        ~new_store:store
        ~old_store:lmdb_store
        history_mode
        genesis
      >>=? fun () ->
      Legacy_store.close lmdb_store ;
      Store.close_store store >>= fun () -> return_unit)
    (function
      | Ok () ->
          return
            (Format.sprintf
               "you can now safely remove the old store located at: %s"
               new_lmdb_path)
      | Error errors ->
          upgrade_cleaner data_dir >>= fun () -> Lwt.return (Error errors))
    (fun exn -> upgrade_cleaner data_dir >>= fun () -> Lwt.fail exn)
