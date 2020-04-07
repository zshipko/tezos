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

module Legacy_store = struct
  (* type t = Raw_store.t *)

  (* type global_store = t *)

  (**************************************************************************
   * Configuration setup we need to save in order to avoid wrong changes.
   **************************************************************************)

  module Configuration = struct
    module History_mode =
      Store_helpers.Make_single_store
        (Raw_store)
        (struct
          let name = ["history_mode"]
        end)
        (Store_helpers.Make_value (History_mode.Legacy))
  end

  (**************************************************************************
   * Net store under "chain/"
   **************************************************************************)

  module Chain = struct
    (* type store = global_store * Chain_id.t *)

    let get s id = (s, id)

    module Indexed_store =
      Store_helpers.Make_indexed_substore
        (Store_helpers.Make_substore
           (Raw_store)
           (struct
             let name = ["chain"]
           end))
           (Chain_id)

    (* let destroy = Indexed_store.remove_all *)

    (* let list t =
     *   Indexed_store.fold_indexes t ~init:[] ~f:(fun h acc ->
     *       Lwt.return (h :: acc)) *)

    module Genesis_hash =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["genesis"; "hash"]
        end)
        (Store_helpers.Make_value (Block_hash))

    module Genesis_time =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["genesis"; "time"]
        end)
        (Store_helpers.Make_value (Time.Protocol))

    module Genesis_protocol =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["genesis"; "protocol"]
        end)
        (Store_helpers.Make_value (Protocol_hash))

    module Genesis_test_protocol =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["genesis"; "test_protocol"]
        end)
        (Store_helpers.Make_value (Protocol_hash))

    module Expiration =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["expiration"]
        end)
        (Store_helpers.Make_value (Time.Protocol))

    module Allow_forked_chain = Indexed_store.Make_set (struct
      let name = ["allow_forked_chain"]
    end)

    module Protocol_index =
      Store_helpers.Make_indexed_substore
        (Store_helpers.Make_substore
           (Indexed_store.Store)
           (struct
             let name = ["protocol"]
           end))
           (Store_helpers.Integer_index)

    module Protocol_info =
      Protocol_index.Make_map
        (struct
          let name = ["info"]
        end)
        (Store_helpers.Make_value (struct
          type t = Protocol_hash.t * Int32.t

          let encoding =
            let open Data_encoding in
            tup2 Protocol_hash.encoding int32
        end))
  end

  (**************************************************************************
   * Temporary test chain forking block store under "forking_block_hash/"
   **************************************************************************)

  module Forking_block_hash =
    Store_helpers.Make_map
      (Store_helpers.Make_substore
         (Raw_store)
         (struct
           let name = ["forking_block_hash"]
         end))
         (Chain_id)
      (Store_helpers.Make_value (Block_hash))

  (**************************************************************************
   * Block_header store under "chain/<id>/blocks/"
   **************************************************************************)

  module Block = struct
    (* type store = Chain.store *)

    (* let get x = x *)

    module Indexed_store =
      Store_helpers.Make_indexed_substore
        (Store_helpers.Make_substore
           (Chain.Indexed_store.Store)
           (struct
             let name = ["blocks"]
           end))
           (Block_hash)

    type contents = {
      header : Block_header.t;
      message : string option;
      max_operations_ttl : int;
      last_allowed_fork_level : Int32.t;
      context : Context_hash.t;
      metadata : Bytes.t;
    }

    module Contents =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["contents"]
        end)
        (Store_helpers.Make_value (struct
          type t = contents

          let encoding =
            let open Data_encoding in
            conv
              (fun { header;
                     message;
                     max_operations_ttl;
                     last_allowed_fork_level;
                     context;
                     metadata } ->
                ( message,
                  max_operations_ttl,
                  last_allowed_fork_level,
                  context,
                  metadata,
                  header ))
              (fun ( message,
                     max_operations_ttl,
                     last_allowed_fork_level,
                     context,
                     metadata,
                     header ) ->
                {
                  header;
                  message;
                  max_operations_ttl;
                  last_allowed_fork_level;
                  context;
                  metadata;
                })
              (obj6
                 (opt "message" string)
                 (req "max_operations_ttl" uint16)
                 (req "last_allowed_fork_level" int32)
                 (req "context" Context_hash.encoding)
                 (req "metadata" bytes)
                 (req "header" Block_header.encoding))
        end))

    type pruned_contents = {header : Block_header.t}

    module Pruned_contents =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["pruned_contents"]
        end)
        (Store_helpers.Make_value (struct
          type t = pruned_contents

          let encoding =
            let open Data_encoding in
            conv
              (fun {header} -> header)
              (fun header -> {header})
              (obj1 (req "header" Block_header.encoding))
        end))

    module Operations_index =
      Store_helpers.Make_indexed_substore
        (Store_helpers.Make_substore
           (Indexed_store.Store)
           (struct
             let name = ["operations"]
           end))
           (Store_helpers.Integer_index)

    module Operation_hashes =
      Operations_index.Make_map
        (struct
          let name = ["hashes"]
        end)
        (Store_helpers.Make_value (struct
          type t = Operation_hash.t list

          let encoding = Data_encoding.list Operation_hash.encoding
        end))

    module Operations =
      Operations_index.Make_map
        (struct
          let name = ["contents"]
        end)
        (Store_helpers.Make_value (struct
          type t = Operation.t list

          let encoding = Data_encoding.(list (dynamic_size Operation.encoding))
        end))

    module Operations_metadata =
      Operations_index.Make_map
        (struct
          let name = ["metadata"]
        end)
        (Store_helpers.Make_value (struct
          type t = Bytes.t list

          let encoding = Data_encoding.(list bytes)
        end))

    type invalid_block = {level : int32; errors : Error_monad.error list}

    module Invalid_block =
      Store_helpers.Make_map
        (Store_helpers.Make_substore
           (Chain.Indexed_store.Store)
           (struct
             let name = ["invalid_blocks"]
           end))
           (Block_hash)
        (Store_helpers.Make_value (struct
          type t = invalid_block

          let encoding =
            let open Data_encoding in
            conv
              (fun {level; errors} -> (level, errors))
              (fun (level, errors) -> {level; errors})
              (tup2 int32 (list Error_monad.error_encoding))
        end))

    let register s =
      Base58.register_resolver Block_hash.b58check_encoding (fun str ->
          let pstr = Block_hash.prefix_path str in
          Chain.Indexed_store.fold_indexes s ~init:[] ~f:(fun chain acc ->
              Indexed_store.resolve_index (s, chain) pstr
              >>= fun l -> Lwt.return (List.rev_append l acc)))

    module Predecessors =
      Store_helpers.Make_map
        (Store_helpers.Make_substore
           (Indexed_store.Store)
           (struct
             let name = ["predecessors"]
           end))
           (Store_helpers.Integer_index)
        (Store_helpers.Make_value (Block_hash))
  end

  (**************************************************************************
   * Blockchain data
   **************************************************************************)

  module Chain_data = struct
    (* type store = Chain.store *)

    let get s = s

    module Known_heads =
      Store_helpers.Make_buffered_set
        (Store_helpers.Make_substore
           (Chain.Indexed_store.Store)
           (struct
             let name = ["known_heads"]
           end))
           (Block_hash)
        (Block_hash.Set)

    module Current_head =
      Store_helpers.Make_single_store
        (Chain.Indexed_store.Store)
        (struct
          let name = ["current_head"]
        end)
        (Store_helpers.Make_value (Block_hash))

    module In_main_branch =
      Store_helpers.Make_single_store
        (Block.Indexed_store.Store)
        (struct
          let name = ["in_chain"]
        end)
        (Store_helpers.Make_value (Block_hash))

    (* successor *)

    module Checkpoint =
      Store_helpers.Make_single_store
        (Chain.Indexed_store.Store)
        (struct
          let name = ["checkpoint"]
        end)
        (Store_helpers.Make_value (Block_header))

    module Save_point =
      Store_helpers.Make_single_store
        (Chain.Indexed_store.Store)
        (struct
          let name = ["save_point"]
        end)
        (Store_helpers.Make_value (struct
          type t = Int32.t * Block_hash.t

          let encoding =
            let open Data_encoding in
            tup2 int32 Block_hash.encoding
        end))

    module Caboose =
      Store_helpers.Make_single_store
        (Chain.Indexed_store.Store)
        (struct
          let name = ["caboose"]
        end)
        (Store_helpers.Make_value (struct
          type t = Int32.t * Block_hash.t

          let encoding =
            let open Data_encoding in
            tup2 int32 Block_hash.encoding
        end))
  end

  (**************************************************************************
   * Protocol store under "protocols/"
   **************************************************************************)

  module Protocol = struct
    (* type store = global_store *)

    (* let get x = x *)

    module Indexed_store =
      Store_helpers.Make_indexed_substore
        (Store_helpers.Make_substore
           (Raw_store)
           (struct
             let name = ["protocols"]
           end))
           (Protocol_hash)

    module Contents =
      Indexed_store.Make_map
        (struct
          let name = ["contents"]
        end)
        (Store_helpers.Make_value (Protocol))

    module RawContents =
      Store_helpers.Make_single_store
        (Indexed_store.Store)
        (struct
          let name = ["contents"]
        end)
        (Store_helpers.Raw_value)

    let register s =
      Base58.register_resolver Protocol_hash.b58check_encoding (fun str ->
          let pstr = Protocol_hash.prefix_path str in
          Indexed_store.resolve_index s pstr)
  end

  let init ?readonly ?mapsize dir =
    Raw_store.init ?readonly ?mapsize dir
    >>=? fun s -> Block.register s ; Protocol.register s ; return s

  let close = Raw_store.close

  (* let open_with_atomic_rw = Raw_store.open_with_atomic_rw
   *
   * let with_atomic_rw = Raw_store.with_atomic_rw *)
end

(* Hardcoded networks data*)
module Hardcoded = struct
  type network = {name : string; cycle_length : int}

  let proj (name, cycle_length) = {name; cycle_length}

  (* Hardcoded cycle length *)
  let supported_networks =
    List.map
      proj
      [ ("TEZOS_MAINNET", 4096);
        ("TEZOS_ALPHANET_CARTHAGE_2019-11-28T13:02:13Z", 2048);
        ("TEZOS", 8) ]

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
        "Cannot perform operation for chain_name %s. Only %a are supported."
        chain_name
        (Format.pp_print_list
           ~pp_sep:(fun ppf () -> Format.fprintf ppf ", ")
           (fun ppf {name; _} -> Format.fprintf ppf "%s" name))
        supported_networks
    else return_unit

  (* Aims to tell if the checkpoint set and a cimentage is needed*)
  let may_update_checkpoint ~cycle_length nb_blocks = nb_blocks = cycle_length
end

(* Legacy store convertion*)
open Legacy_store

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

  let restoring_after_failure =
    Internal_event.Simple.declare_1
      ~level:Internal_event.Notice
      ~section
      ~name:"restoring_after_failure"
      ~msg:
        "Cleaning directory {directory} because of failure: restoring the \
         store backup."
      ("directory", Data_encoding.string)
end

let make_block_repr ~with_metadata lmdb_block_store hash =
  Block.Operations.bindings (lmdb_block_store, hash)
  >>= fun ops ->
  Block.Operations_metadata.bindings (lmdb_block_store, hash)
  >>= fun ops_met ->
  let operations = List.map snd ops in
  let operations_metadata = List.map snd ops_met in
  match with_metadata with
  | true ->
      Block.Contents.read (lmdb_block_store, hash)
      >>=? fun block_contents ->
      let ({ header;
             message;
             max_operations_ttl;
             last_allowed_fork_level;
             metadata;
             _ }
            : Block.contents) =
        block_contents
      in
      let contents = ({header; operations} : Block_repr.contents) in
      let metadata =
        Some
          ( {
              message;
              max_operations_ttl;
              last_allowed_fork_level;
              block_metadata = metadata;
              operations_metadata;
            }
            : Block_repr.metadata )
      in
      return ({hash; contents; metadata} : Block_repr.t)
  | false ->
      Block.Pruned_contents.read (lmdb_block_store, hash)
      >>=? fun header ->
      let contents =
        ({header = header.header; operations} : Block_repr.contents)
      in
      return ({hash; contents; metadata = None} : Block_repr.t)

let may_update_protocol_table lmdb_block_store chain_store block
    prev_proto_level =
  if
    (not (prev_proto_level = Block_repr.proto_level block))
    || Block_repr.level block = 1l
  then
    Chain.Protocol_info.bindings lmdb_block_store
    >>= fun protocol_table ->
    let (proto_hash, _transition_level) : Chain.Protocol_info.value =
      List.assoc (Block_repr.proto_level block) protocol_table
    in
    Store.Chain.set_protocol_level
      chain_store
      prev_proto_level
      (Store.Block.of_repr block, proto_hash)
  else return_unit

let import_floating lmdb_block_store chain_store ?(with_metadata = true)
    block_hash limit =
  (* TODO: Make it tail rec ? If so, we must be able to store a block
   without searching for its predecessors*)
  let block_store = Store.unsafe_get_block_store chain_store in
  let rec aux hash =
    make_block_repr ~with_metadata lmdb_block_store hash
    >>=? fun block_repr ->
    (* At protocol change, update the protocol_table*)
    let prev_proto_level = Block_repr.proto_level block_repr in
    may_update_protocol_table
      lmdb_block_store
      chain_store
      block_repr
      prev_proto_level
    >>=? fun () ->
    if Block_repr.level block_repr = limit then
      Block_store.store_block block_store block_repr >>= fun () -> return_unit
    else
      aux (Block_repr.predecessor block_repr)
      >>=? fun savepoint ->
      Block_store.store_block block_store block_repr
      >>= fun () -> return savepoint
  in
  aux block_hash

let new_import_cemented lmdb_chain_store chain_store cycle_length
    ~with_metadata ~start_block =
  (* We assume that the limits are "on cycles" *)
  assert (
    Int32.(to_int (sub (Block_repr.level start_block) 1l)) mod cycle_length = 0
  ) ;
  let rec aux (last_checkpoint : Block_header.t) acc hash =
    make_block_repr ~with_metadata lmdb_chain_store hash
    >>=? fun block_repr ->
    (* At protocol change, update the protocol_table*)
    let prev_proto_level = Block_repr.proto_level block_repr in
    may_update_protocol_table
      lmdb_chain_store
      chain_store
      block_repr
      prev_proto_level
    >>=? fun () ->
    let new_acc = block_repr :: acc in
    if Block_repr.level block_repr = 1l then
      (* Special case to cement [genesis;1] *)
      (*Reading genesis which is not pruned in lmdb *)
      make_block_repr
        ~with_metadata:true
        lmdb_chain_store
        (Block_repr.predecessor block_repr)
      >>=? fun genesis ->
      Store.cement_blocks_chunk
        chain_store
        [genesis; block_repr]
        ~write_metadata:with_metadata
      >>= fun () -> return_unit
    else if Hardcoded.may_update_checkpoint ~cycle_length (List.length new_acc)
    then
      Store.cement_blocks_chunk
        chain_store
        new_acc
        ~write_metadata:with_metadata
      >>= fun () ->
      aux (Block_repr.header block_repr) [] (Block_repr.predecessor block_repr)
    else
      aux
        last_checkpoint
        (block_repr :: acc)
        (Block_repr.predecessor block_repr)
  in
  aux
    (Block_repr.header start_block)
    [start_block]
    (Block_repr.predecessor start_block)

let dirty_read_i lmdb_chain_store hash target_level =
  let rec aux hash =
    Block.Pruned_contents.read_opt (lmdb_chain_store, hash)
    >>= (function
          | Some {header} ->
              return header
          | None ->
              Block.Contents.read (lmdb_chain_store, hash)
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
  make_block_repr ~with_metadata:true lmdb_chain_store checkpoint_hash
  >>=? fun start_block ->
  new_import_cemented
    lmdb_chain_store
    chain_store
    cycle_length
    ~with_metadata:true
    ~start_block
  >>=? fun () ->
  import_floating
    lmdb_chain_store
    chain_store
    current_head_hash
    (Int32.add checkpoint.shell.level 1l)
  >>=? fun () ->
  let cp = (checkpoint_hash, checkpoint.shell.level) in
  let genesis = Store.Chain.genesis chain_store in
  let cb = (genesis.block, 0l) in
  let sp = cb in
  return (cp, sp, cb)

let full_import lmdb_chain_store chain_store cycle_length checkpoint
    current_head_hash =
  let checkpoint_hash = Block_header.hash checkpoint in
  make_block_repr ~with_metadata:true lmdb_chain_store checkpoint_hash
  >>=? fun start_block ->
  new_import_cemented
    lmdb_chain_store
    chain_store
    cycle_length
    ~with_metadata:false
    ~start_block
  >>=? fun () ->
  import_floating
    lmdb_chain_store
    chain_store
    current_head_hash
    (Int32.add checkpoint.shell.level 1l)
  >>=? fun () ->
  let cp = (checkpoint_hash, checkpoint.shell.level) in
  let genesis = Store.Chain.genesis chain_store in
  let cb = (genesis.block, 0l) in
  (* As checkpoint = savepoint in lmbd, we cannot set a consistent savepoint *)
  dirty_read_i
    lmdb_chain_store
    current_head_hash
    (Int32.add 1l checkpoint.shell.level)
  >>=? fun sp_header ->
  let sp = (Block_header.hash sp_header, sp_header.shell.level) in
  return (cp, sp, cb)

let rolling_import lmdb_chain_store chain_store cycle_length checkpoint
    current_head_hash =
  let lmdb_chain_data = Chain_data.get lmdb_chain_store in
  (*TODO: in rolling, everything is in floating. Check that the
      max_op_ttl blocks before checkpoint are not dropped ?*)
  (* low_limit delimits where we stop reading pruned blocks*)
  Chain_data.Save_point.read lmdb_chain_data
  >>=? fun (savepoint_level, savepoint_hash) ->
  (* Assume caboose = savepoint - History_mode.default_offset * cylce_size *)
  let caboose_level =
    Int32.(
      sub
        (add checkpoint.Block_header.shell.level 1l)
        (of_int (History_mode.default_offset * cycle_length)))
  in
  dirty_read_i lmdb_chain_store current_head_hash caboose_level
  >>=? fun caboose_header ->
  (* Read the savepoint as non-pruned*)
  make_block_repr ~with_metadata:true lmdb_chain_store savepoint_hash
  >>=? fun savepoint ->
  (* Importing [caboose-(max_op_ttl(caboose));savepoint[ in floating *)
  (* We take (caboose - lmdb_caboose) blocks for the max_op_ttl are it is the
   only data available … *)
  Chain_data.Caboose.read lmdb_chain_data
  >>=? fun (lmdb_caboose_level, _) ->
  import_floating
    lmdb_chain_store
    chain_store
    ~with_metadata:false
    (Block_repr.predecessor savepoint)
    lmdb_caboose_level
  >>=? fun () ->
  (* Importing savepoint in floating *)
  let block_store = Store.unsafe_get_block_store chain_store in
  Block_store.store_block block_store savepoint
  >>= fun () ->
  (* Importing ]savepoint;current_head] in floating *)
  import_floating
    lmdb_chain_store
    chain_store
    current_head_hash
    (Int32.add savepoint_level 1l)
  >>=? fun () ->
  let checkpoint_level = checkpoint.shell.level in
  (* As checkpoint = savepoint in lmbd, we cannot set a consistent savepoint *)
  dirty_read_i
    lmdb_chain_store
    current_head_hash
    (Int32.add 1l checkpoint_level)
  >>=? fun sp_header ->
  let sp = (Block_header.hash sp_header, sp_header.shell.level) in
  let caboose_hash = Block_header.hash caboose_header in
  let cb = (caboose_hash, caboose_level) in
  let cp = (Block_header.hash checkpoint, checkpoint_level) in
  return (cp, sp, cb)

let infer_checkpoint lmdb_chain_data cycle_length =
  (* When upgrading from a full or rolling node, the checkpoint may not be set
   on a "real checkpoint".*)
  Chain_data.Checkpoint.read lmdb_chain_data
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
  let lmdb_chain_data = Chain_data.get lmdb_chain_store in
  Chain_data.Current_head.read lmdb_chain_data
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
      rolling_import
        lmdb_chain_store
        chain_store
        cycle_length
        checkpoint
        current_head_hash )
  >>=? fun (new_checkpoint, new_savepoint, new_caboose) ->
  return (new_checkpoint, new_savepoint, new_caboose)

let import_protocols history_mode lmdb_store lmdb_chain_store store checkpoint
    cycle_length =
  let lmdb_chain_data = Chain_data.get lmdb_chain_store in
  match (history_mode : History_mode.t) with
  | Archive | Full _ ->
      Protocol.Contents.bindings lmdb_store
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
      Chain_data.Current_head.read lmdb_chain_data
      >>=? fun current_head_hash ->
      Block.Contents.read (lmdb_chain_store, current_head_hash)
      >>=? fun current_head ->
      (* Assume caboose = savepoint - History_mode.default_offset * cylce_size *)
      let caboose_level =
        Int32.(
          sub
            (add checkpoint.Block_header.shell.level 1l)
            (of_int (History_mode.default_offset * cycle_length)))
      in
      dirty_read_i lmdb_chain_store current_head_hash caboose_level
      >>=? fun caboose_header ->
      (* We store the oldest known protocol*)
      Block.Pruned_contents.read
        (lmdb_chain_store, caboose_header.shell.predecessor)
      >>=? fun {header = transition_header; _} ->
      let protocol_level = current_head.header.shell.proto_level in
      Chain.Protocol_info.read lmdb_chain_store protocol_level
      >>=? fun protocol_info ->
      let protocol_hash = fst protocol_info in
      let chain_store = Store.main_chain_store store in
      let transition_hash = Block_header.hash transition_header in
      make_block_repr ~with_metadata:false lmdb_chain_store transition_hash
      >>=? fun transition_block ->
      Store.Chain.set_protocol_level
        chain_store
        protocol_level
        (Store.Block.of_repr transition_block, protocol_hash)

let update_stored_data lmdb_chain_data chain_store ~new_checkpoint
    ~new_savepoint ~new_caboose =
  Chain_data.Current_head.read lmdb_chain_data
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

let upgrade_0_0_4 ~data_dir genesis patch_context ~chain_name =
  Hardcoded.check_network ~chain_name
  >>=? fun () ->
  let ( // ) = Filename.concat in
  let old_lmdb_path = data_dir // new_store_name in
  let new_lmdb_path = data_dir // lmdb_store_name in
  Unix.rename old_lmdb_path new_lmdb_path ;
  Lwt.try_bind
    (fun () ->
      init ~readonly:false new_lmdb_path
      >>=? fun lmdb_store ->
      let chain_id = Chain_id.of_block_hash genesis.Genesis.block in
      let lmdb_chain_store = Chain.get lmdb_store chain_id in
      let lmdb_chain_data = Chain_data.get lmdb_chain_store in
      let store_root = data_dir // "store" in
      let context_root = data_dir // "context" in
      Configuration.History_mode.read lmdb_store
      >>=? fun legacy_history_mode ->
      let history_mode = History_mode.convert legacy_history_mode in
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
      import_protocols
        history_mode
        lmdb_store
        lmdb_chain_store
        store
        checkpoint
        cycle_length
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
      close lmdb_store ;
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
