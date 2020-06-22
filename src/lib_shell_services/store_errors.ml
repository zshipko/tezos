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

type error +=
  | Block_not_found of Block_hash.t
  | Bad_level of {head_level : Int32.t; given_level : Int32.t}
  | Block_metadata_not_found of Block_hash.t
  | Incorrect_history_mode_switch of {
      previous_mode : History_mode.t;
      next_mode : History_mode.t;
    }
  | Cannot_switch_history_mode of {
      previous_mode : History_mode.t;
      next_mode : History_mode.t;
    }
  | Invalid_head_switch of {
      minimum_allowed_level : int32;
      given_head : Block_hash.t * int32;
    }

type error += Inconsistent_store_state of string

type error +=
  | Inconsistent_operations_hash of {
      expected : Operation_list_list_hash.t;
      got : Operation_list_list_hash.t;
    }

let () =
  register_error_kind
    `Permanent
    ~id:"store.not_found"
    ~title:"Block not found"
    ~description:"Block not found"
    ~pp:(fun ppf block_hash ->
      Format.fprintf ppf "Cannot find block %a" Block_hash.pp block_hash)
    Data_encoding.(obj1 (req "block_not_found" @@ Block_hash.encoding))
    (function Block_not_found block_hash -> Some block_hash | _ -> None)
    (fun block_hash -> Block_not_found block_hash) ;
  register_error_kind
    `Permanent
    ~id:"store.bad_level"
    ~title:"Bad level"
    ~description:"Read a block at level past our current head."
    ~pp:(fun ppf (head_level, given_level) ->
      Format.fprintf
        ppf
        "The block at level %ld is beyond our current head's level : %ld."
        given_level
        head_level)
    Data_encoding.(obj2 (req "head_level" int32) (req "given_level" int32))
    (function
      | Bad_level {head_level; given_level} ->
          Some (head_level, given_level)
      | _ ->
          None)
    (fun (head_level, given_level) -> Bad_level {head_level; given_level}) ;
  register_error_kind
    `Permanent
    ~id:"store.metadata_not_found"
    ~title:"Block metadata not found"
    ~description:"Block metadata not found"
    ~pp:(fun ppf block_hash ->
      Format.fprintf
        ppf
        "Unable to find block %a's metadata."
        Block_hash.pp
        block_hash)
    Data_encoding.(obj1 (req "block_metadata_not_found" Block_hash.encoding))
    (function
      | Block_metadata_not_found block_hash -> Some block_hash | _ -> None)
    (fun block_hash -> Block_metadata_not_found block_hash) ;
  register_error_kind
    `Permanent
    ~id:"node_config_file.incorrect_history_mode_switch"
    ~title:"Incorrect history mode switch"
    ~description:"Incorrect history mode switch."
    ~pp:(fun ppf (prev, next) ->
      Format.fprintf
        ppf
        "Incorrect history mode switch: cannot switch from history mode %a to \
         %a."
        History_mode.pp
        prev
        History_mode.pp
        next)
    (Data_encoding.obj2
       (Data_encoding.req "previous_mode" History_mode.encoding)
       (Data_encoding.req "next_mode" History_mode.encoding))
    (function
      | Incorrect_history_mode_switch x ->
          Some (x.previous_mode, x.next_mode)
      | _ ->
          None)
    (fun (previous_mode, next_mode) ->
      Incorrect_history_mode_switch {previous_mode; next_mode}) ;
  register_error_kind
    `Permanent
    ~id:"node_config_file.cannot_switch_history_mode"
    ~title:"Cannot switch history mode"
    ~description:"Cannot switch history mode."
    ~pp:(fun ppf (prev, next) ->
      Format.fprintf
        ppf
        "Cannot switch from history mode %a to %a. In order to change your \
         history mode please refer to the Tezos node documentation. If you \
         really want to change your history mode, run this command again with \
         the `--force-history-mode-switch` option."
        History_mode.pp
        prev
        History_mode.pp
        next)
    (Data_encoding.obj2
       (Data_encoding.req "previous_mode" History_mode.encoding)
       (Data_encoding.req "next_mode" History_mode.encoding))
    (function
      | Cannot_switch_history_mode x ->
          Some (x.previous_mode, x.next_mode)
      | _ ->
          None)
    (fun (previous_mode, next_mode) ->
      Cannot_switch_history_mode {previous_mode; next_mode}) ;
  register_error_kind
    `Permanent
    ~id:"store.invalid_head_switch"
    ~title:"Invalid head switch"
    ~description:
      "The given head is not consistent with the current store's savepoint"
    ~pp:(fun ppf (minimum_allowed_level, (head_hash, head_level)) ->
      Format.fprintf
        ppf
        "The given head %a (%ld) is below the minimum allowed level %ld."
        Block_hash.pp
        head_hash
        head_level
        minimum_allowed_level)
    Data_encoding.(
      obj2
        (req "minimum_allowed_level" int32)
        (req "given_head" @@ tup2 Block_hash.encoding int32))
    (function
      | Invalid_head_switch {minimum_allowed_level; given_head} ->
          Some (minimum_allowed_level, given_head)
      | _ ->
          None)
    (fun (minimum_allowed_level, given_head) ->
      Invalid_head_switch {minimum_allowed_level; given_head}) ;
  register_error_kind
    `Permanent
    ~id:"store.inconsistent_state"
    ~title:"Store inconsistent state"
    ~description:"The store is in an unexpected state."
    ~pp:(fun ppf msg ->
      Format.fprintf ppf "The store is in an unexpected state: %s" msg)
    Data_encoding.(obj1 (req "message" string))
    (function Inconsistent_store_state msg -> Some msg | _ -> None)
    (fun msg -> Inconsistent_store_state msg) ;
  register_error_kind
    `Permanent
    ~id:"snapshots.inconsistent_operation_hashes"
    ~title:"Inconsistent operation hashes"
    ~description:"The operations given do not match their hashes."
    ~pp:(fun ppf (oph, oph') ->
      Format.fprintf
        ppf
        "Inconsistent operation hashes. Expected: %a, got %a."
        Operation_list_list_hash.pp
        oph
        Operation_list_list_hash.pp
        oph')
    Data_encoding.(
      obj2
        (req "expected_operation_hashes" Operation_list_list_hash.encoding)
        (req "received_operation_hashes" Operation_list_list_hash.encoding))
    (function
      | Inconsistent_operations_hash {expected; got} ->
          Some (expected, got)
      | _ ->
          None)
    (fun (expected, got) -> Inconsistent_operations_hash {expected; got})

type cemented_store_inconsistency =
  | Missing_cycle of {low_cycle : string; high_cycle : string}
  | Bad_offset of {level : int; cycle : string}
  | Unexpected_level of {
      block_hash : Block_hash.t;
      expected : Int32.t;
      got : Int32.t;
    }
  | Corrupted_index of Block_hash.t
  | Inconsistent_highest_cemented_level of {
      highest_cemented_level : Int32.t;
      head_last_allowed_fork_level : Int32.t;
    }

let cemented_store_inconsistency_encoding =
  let open Data_encoding in
  union
    [ case
        (Tag 0)
        ~title:"Missing cycle"
        (obj2 (req "low_cycle" string) (req "high_cycle" string))
        (function
          | Missing_cycle {low_cycle; high_cycle} ->
              Some (low_cycle, high_cycle)
          | _ ->
              None)
        (fun (low_cycle, high_cycle) -> Missing_cycle {low_cycle; high_cycle});
      case
        (Tag 1)
        ~title:"Bad offset"
        (obj2 (req "level" int31) (req "cycle" string))
        (function
          | Bad_offset {level; cycle} -> Some (level, cycle) | _ -> None)
        (fun (level, cycle) -> Bad_offset {level; cycle});
      case
        (Tag 2)
        ~title:"Unexpected level"
        (obj3
           (req "block_hash" Block_hash.encoding)
           (req "expected" int32)
           (req "got" int32))
        (function
          | Unexpected_level {block_hash; expected; got} ->
              Some (block_hash, expected, got)
          | _ ->
              None)
        (fun (block_hash, expected, got) ->
          Unexpected_level {block_hash; expected; got});
      case
        (Tag 3)
        ~title:"Corrupted index"
        (obj1 (req "block_hash" Block_hash.encoding))
        (function Corrupted_index h -> Some h | _ -> None)
        (fun h -> Corrupted_index h);
      case
        (Tag 4)
        ~title:"Inconsistent highest cemented level"
        (obj2
           (req "highest_cemented_level" int32)
           (req "head_last_allowed_fork_level" int32))
        (function
          | Inconsistent_highest_cemented_level
              {highest_cemented_level; head_last_allowed_fork_level} ->
              Some (highest_cemented_level, head_last_allowed_fork_level)
          | _ ->
              None)
        (fun (highest_cemented_level, head_last_allowed_fork_level) ->
          Inconsistent_highest_cemented_level
            {highest_cemented_level; head_last_allowed_fork_level}) ]

type store_block_error =
  | Invalid_operations_length of {validation_passes : int; operations : int}
  | Invalid_operations_data_length of {
      validation_passes : int;
      operations_data : int;
    }
  | Inconsistent_operations_lengths of {
      operations_lengths : string;
      operations_data_lengths : string;
    }

let store_block_error_encoding =
  let open Data_encoding in
  union
    [ case
        (Tag 0)
        ~title:"Invalid operations length"
        (obj2 (req "validation_passes" int31) (req "operations" int31))
        (function
          | Invalid_operations_length {validation_passes; operations} ->
              Some (validation_passes, operations)
          | _ ->
              None)
        (fun (validation_passes, operations) ->
          Invalid_operations_length {validation_passes; operations});
      case
        (Tag 1)
        ~title:"Invalid operations data length"
        (obj2 (req "validation_passes" int31) (req "operations_data" int31))
        (function
          | Invalid_operations_data_length {validation_passes; operations_data}
            ->
              Some (validation_passes, operations_data)
          | _ ->
              None)
        (fun (validation_passes, operations_data) ->
          Invalid_operations_data_length {validation_passes; operations_data});
      case
        (Tag 2)
        ~title:"Inconsistent operations length"
        (obj2
           (req "operations_lengths" string)
           (req "operations_data_lengths" string))
        (function
          | Inconsistent_operations_lengths
              {operations_lengths; operations_data_lengths} ->
              Some (operations_lengths, operations_data_lengths)
          | _ ->
              None)
        (fun (operations_lengths, operations_data_lengths) ->
          Inconsistent_operations_lengths
            {operations_lengths; operations_data_lengths}) ]

type error +=
  | Wrong_predecessor of Block_hash.t * int
  | Invalid_blocks_to_cement
  | Wrong_floating_kind_swap
  | Cannot_update_floating_store
  | Merge_error of string
  | Cannot_load_table of string
  | Failed_to_init_cemented_block_store of string
  | Cannot_cement_blocks_metadata of [`Empty | `Not_cemented]
  | Cannot_cement_blocks of [`Empty | `Higher_cemented]
  | Temporary_cemented_file_exists of string
  | Inconsistent_cemented_file of string * string
  | Inconsistent_cemented_store of cemented_store_inconsistency
  | Missing_last_allowed_fork_level_block
  | Inconsistent_block_hash of {
      level : Int32.t;
      expected_hash : Block_hash.t;
      computed_hash : Block_hash.t;
    }
  | Inconsistent_block_predecessor of {
      block_hash : Block_hash.t;
      level : Int32.t;
      expected_hash : Block_hash.t;
      computed_hash : Block_hash.t;
    }
  | Cannot_encode_block of Block_hash.t
  | Cannot_store_block of Block_hash.t * store_block_error
  | Cannot_checkout_context of Block_hash.t * Context_hash.t
  | Cannot_find_protocol of int
  | Invalid_genesis_marking
  | Cannot_retrieve_savepoint of Int32.t
  | Cannot_set_checkpoint of (Block_hash.t * Int32.t)
  | Missing_commit_info of string
  | Inconsistent_chain_store
  | Fork_testchain_not_allowed
  | Cannot_fork_testchain of Chain_id.t
  | Cannot_load_testchain of string
  | Missing_activation_block of Block_hash.t * Protocol_hash.t * History_mode.t
  | Inconsistent_protocol_commit_info of Block_hash.t * Protocol_hash.t
  | Missing_activation_block_legacy of
      Int32.t * Protocol_hash.t * History_mode.t
  | Missing_stored_data of string

let () =
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.wrong_predecessor"
    ~title:"Wrong predecessor"
    ~description:"Failed to get block's predecessor"
    ~pp:(fun ppf (hash, offset) ->
      Format.fprintf
        ppf
        "Failed to get the nth predecessor of %a. The offset is invalid: %d."
        Block_hash.pp
        hash
        offset)
    Data_encoding.(obj2 (req "hash" Block_hash.encoding) (req "offset" int31))
    (function
      | Wrong_predecessor (hash, offset) -> Some (hash, offset) | _ -> None)
    (fun (hash, offset) -> Wrong_predecessor (hash, offset)) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.invalid_blocks_to_cement"
    ~title:"Invalid blocks to cement"
    ~description:"Invalid block list to cement"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Invalid block list to cement: the block list must be correctly \
         chained and their levels growing strictly by one between each block.")
    Data_encoding.empty
    (function Invalid_blocks_to_cement -> Some () | _ -> None)
    (fun () -> Invalid_blocks_to_cement) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.wrong_floating_kind_swap"
    ~title:"Wrong floating kind swap"
    ~description:"Try to swap wrong floating store kind"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Failed to swap floating stores: tried to swap floating store of the \
         same kind.")
    Data_encoding.empty
    (function Wrong_floating_kind_swap -> Some () | _ -> None)
    (fun () -> Wrong_floating_kind_swap) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_update_floating_store"
    ~title:"Cannot update floating store"
    ~description:"Cannot update floating store"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Cannot update the floating store: failed to retrieve enough blocks \
         to cement.")
    Data_encoding.empty
    (function Cannot_update_floating_store -> Some () | _ -> None)
    (fun () -> Cannot_update_floating_store) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.merge_error"
    ~title:"Merge error"
    ~description:"Error while merging the store"
    ~pp:(fun ppf trace ->
      Format.fprintf ppf "Error while merging the store: %s." trace)
    Data_encoding.(obj1 (req "trace" string))
    (function Merge_error trace -> Some trace | _ -> None)
    (fun trace -> Merge_error trace) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_load_table"
    ~title:"Cannot load table"
    ~description:"Cannot load cemented table"
    ~pp:(fun ppf path ->
      Format.fprintf
        ppf
        "Failed to load the cemented cycles table: directory %s does not exist."
        path)
    Data_encoding.(obj1 (req "path" string))
    (function Cannot_load_table path -> Some path | _ -> None)
    (fun path -> Cannot_load_table path) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.failed_to_init_cemented_block_store"
    ~title:"Failed to init cemented block store"
    ~description:"Failed to initialize the cemented block store"
    ~pp:(fun ppf path ->
      Format.fprintf
        ppf
        "Failed to initialize the cemented block store: file %s is not a \
         directory."
        path)
    Data_encoding.(obj1 (req "path" string))
    (function
      | Failed_to_init_cemented_block_store path -> Some path | _ -> None)
    (fun path -> Failed_to_init_cemented_block_store path) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_cement_blocks_metadata"
    ~title:"Cannot cement blocks metadata"
    ~description:"Cannot cement blocks metadata"
    ~pp:(fun ppf reason ->
      Format.fprintf
        ppf
        "Failed to cement the blocks metadata: %s."
        ( match reason with
        | `Empty ->
            "the given list of blocks is empty"
        | `Not_cemented ->
            "the given blocks ar not cemented" ))
    Data_encoding.(
      obj1
        (req
           "reason"
           (string_enum [("empty", `Empty); ("not_cemented", `Not_cemented)])))
    (function Cannot_cement_blocks_metadata r -> Some r | _ -> None)
    (fun r -> Cannot_cement_blocks_metadata r) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_cement_blocks"
    ~title:"Cannot cement blocks"
    ~description:"Cannot cement blocks"
    ~pp:(fun ppf reason ->
      Format.fprintf
        ppf
        "Failed to cement the blocks: %s."
        ( match reason with
        | `Empty ->
            "the given list of blocks is empty"
        | `Higher_cemented ->
            "the previously cemented blocks have higher level than the given \
             blocks" ))
    Data_encoding.(
      obj1
        (req
           "reason"
           (string_enum
              [("empty", `Empty); ("higher_cemented", `Higher_cemented)])))
    (function Cannot_cement_blocks r -> Some r | _ -> None)
    (fun r -> Cannot_cement_blocks r) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.temporary_cemented_file_exists"
    ~title:"Temporary cemented file exists"
    ~description:"The temporary cemented file already exists"
    ~pp:(fun ppf path ->
      Format.fprintf
        ppf
        "Error while merging the store: the temporary cemented file %s \
         already exists."
        path)
    Data_encoding.(obj1 (req "path" string))
    (function Temporary_cemented_file_exists path -> Some path | _ -> None)
    (fun path -> Temporary_cemented_file_exists path) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.inconsistent_cemented_file"
    ~title:"Inconsistent cemented file"
    ~description:"Failed to read a cemented file"
    ~pp:(fun ppf (path, trace) ->
      Format.fprintf
        ppf
        "Failed to read the cemented file %s. Unexpected failure: %s."
        path
        trace)
    Data_encoding.(obj2 (req "path" string) (req "trace" string))
    (function
      | Inconsistent_cemented_file (path, trace) ->
          Some (path, trace)
      | _ ->
          None)
    (fun (path, trace) -> Inconsistent_cemented_file (path, trace)) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.inconsistent_cemented_store"
    ~title:"Inconsistent cemented store"
    ~description:"Failed to check indexes consistency"
    ~pp:(fun ppf csi ->
      Format.fprintf
        ppf
        "The store is in an unexpected and inconsistent state: %s."
        ( match csi with
        | Missing_cycle {low_cycle; high_cycle} ->
            Format.sprintf
              "missing cycle between %s and %s"
              low_cycle
              high_cycle
        | Bad_offset {level; cycle} ->
            Format.asprintf
              "bad offset found for block %d in cycle %s"
              level
              cycle
        | Unexpected_level {block_hash; expected; got} ->
            Format.asprintf
              "bad level found for block %a - expected %ld got %ld"
              Block_hash.pp
              block_hash
              expected
              got
        | Corrupted_index h ->
            Format.asprintf
              "%a was not found in the imported store"
              Block_hash.pp
              h
        | Inconsistent_highest_cemented_level
            {highest_cemented_level; head_last_allowed_fork_level} ->
            Format.sprintf
              "the most recent cemented block (%ld) is not the previous \
               head's last allowed fork level (%ld)"
              highest_cemented_level
              head_last_allowed_fork_level ))
    Data_encoding.(
      obj1
        (req "inconsistent_cemented_file" cemented_store_inconsistency_encoding))
    (function Inconsistent_cemented_store csi -> Some csi | _ -> None)
    (fun csi -> Inconsistent_cemented_store csi) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.missing_last_allowed_fork_level_block"
    ~title:"Missing last allowed fork level block"
    ~description:
      "Current head's last allowed fork level block cannot be found in the \
       store."
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Current head's last allowed fork level block cannot be found in the \
         store.")
    Data_encoding.unit
    (function Missing_last_allowed_fork_level_block -> Some () | _ -> None)
    (fun () -> Missing_last_allowed_fork_level_block) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.inconsistent_block_hash"
    ~title:"Inconsistent block hash"
    ~description:"Inconsistent block hash found"
    ~pp:(fun ppf (level, expected_hash, computed_hash) ->
      Format.fprintf
        ppf
        "Inconsistent block: inconsistent hash found for block %ld. Expected \
         %a, got %a."
        level
        Block_hash.pp
        expected_hash
        Block_hash.pp
        computed_hash)
    Data_encoding.(
      obj3
        (req "level" int32)
        (req "expected_hash" Block_hash.encoding)
        (req "computed_hash" Block_hash.encoding))
    (function
      | Inconsistent_block_hash {level; expected_hash; computed_hash} ->
          Some (level, expected_hash, computed_hash)
      | _ ->
          None)
    (fun (level, expected_hash, computed_hash) ->
      Inconsistent_block_hash {level; expected_hash; computed_hash}) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.inconsistent_block_predecessor"
    ~title:"Inconsistent block predecessor"
    ~description:"Inconsistent block predecessor"
    ~pp:(fun ppf (block_hash, level, expected_hash, computed_hash) ->
      Format.fprintf
        ppf
        "Inconsistent block: inconsistent predecessor found for block %a \
         (%ld) - expected: %a vs got: %a."
        Block_hash.pp
        block_hash
        level
        Block_hash.pp
        expected_hash
        Block_hash.pp
        computed_hash)
    Data_encoding.(
      obj4
        (req "block_hash" Block_hash.encoding)
        (req "level" int32)
        (req "expected_hash" Block_hash.encoding)
        (req "computed_hash" Block_hash.encoding))
    (function
      | Inconsistent_block_predecessor
          {block_hash; level; expected_hash; computed_hash} ->
          Some (block_hash, level, expected_hash, computed_hash)
      | _ ->
          None)
    (fun (block_hash, level, expected_hash, computed_hash) ->
      Inconsistent_block_predecessor
        {block_hash; level; expected_hash; computed_hash}) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_encode_block"
    ~title:"Cannot encode block"
    ~description:"Failed to encode block"
    ~pp:(fun ppf hash ->
      Format.fprintf
        ppf
        "Failed to write block in floating store: cannot encode block %a."
        Block_hash.pp
        hash)
    Data_encoding.(obj1 (req "hash" Block_hash.encoding))
    (function Cannot_encode_block hash -> Some hash | _ -> None)
    (fun hash -> Cannot_encode_block hash) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_store_block"
    ~title:"Cannot store block"
    ~description:"Failed to store block"
    ~pp:(fun ppf (hash, err) ->
      Format.fprintf
        ppf
        "Failed to store block %a: %s."
        Block_hash.pp
        hash
        ( match err with
        | Invalid_operations_length {validation_passes; operations} ->
            Format.sprintf
              "invalid operations length %d (%d was expected)"
              operations
              validation_passes
        | Invalid_operations_data_length {validation_passes; operations_data}
          ->
            Format.sprintf
              "invalid operation_data length %d (%d was expected)"
              validation_passes
              operations_data
        | Inconsistent_operations_lengths
            {operations_lengths; operations_data_lengths} ->
            Format.sprintf
              "inconsistent operations (%s) and operations_data (%s) lengths"
              operations_lengths
              operations_data_lengths ))
    Data_encoding.(
      obj2
        (req "hash" Block_hash.encoding)
        (req "err" store_block_error_encoding))
    (function Cannot_store_block (hash, err) -> Some (hash, err) | _ -> None)
    (fun (hash, err) -> Cannot_store_block (hash, err)) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_checkout_context"
    ~title:"Cannot checkout context"
    ~description:"Failed to checkout context"
    ~pp:(fun ppf (bh, ch) ->
      Format.fprintf
        ppf
        "Failed to checkout the context (%a) for block %a."
        Context_hash.pp
        ch
        Block_hash.pp
        bh)
    Data_encoding.(
      obj2
        (req "block_hash" Block_hash.encoding)
        (req "context_hash" Context_hash.encoding))
    (function Cannot_checkout_context (bh, ch) -> Some (bh, ch) | _ -> None)
    (fun (bh, ch) -> Cannot_checkout_context (bh, ch)) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_find_protocol"
    ~title:"Cannot find protocol"
    ~description:"Cannot find protocol"
    ~pp:(fun ppf proto_level ->
      Format.fprintf ppf "Cannot find protocol with level %d." proto_level)
    Data_encoding.(obj1 (req "protocol_level" int31))
    (function
      | Cannot_find_protocol proto_level -> Some proto_level | _ -> None)
    (fun proto_level -> Cannot_find_protocol proto_level) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.invalid_genesis_marking"
    ~title:"Invalid genesis marking"
    ~description:"Cannot mark genesis as invalid"
    ~pp:(fun ppf () ->
      Format.fprintf ppf "Cannot mark the genesis block is invalid.")
    Data_encoding.empty
    (function Invalid_genesis_marking -> Some () | _ -> None)
    (fun () -> Invalid_genesis_marking) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_retrieve_savepoint"
    ~title:"Cannot retrieve savepoint"
    ~description:"Failed to retrieve savepoint"
    ~pp:(fun ppf level ->
      Format.fprintf
        ppf
        "Failed to retrieve the new savepoint hash (expected at level %ld)."
        level)
    Data_encoding.(obj1 (req "level" int32))
    (function Cannot_retrieve_savepoint level -> Some level | _ -> None)
    (fun level -> Cannot_retrieve_savepoint level) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_set_checkpoint"
    ~title:"Cannot set checkpoint"
    ~description:"The given block to be set as checkpoint is invalid."
    ~pp:(fun ppf (given_checkpoint_hash, given_checkpoint_level) ->
      Format.fprintf
        ppf
        "Failed to set the given checkpoint %a (%ld): it is either invalid, \
         or not a possible successor of the current last allowed fork level \
         block."
        Block_hash.pp
        given_checkpoint_hash
        given_checkpoint_level)
    Data_encoding.(
      obj1 (req "given_checkpoint" (tup2 Block_hash.encoding int32)))
    (function
      | Cannot_set_checkpoint given_checkpoint ->
          Some given_checkpoint
      | _ ->
          None)
    (fun given_checkpoint -> Cannot_set_checkpoint given_checkpoint) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.missing_commit_info"
    ~title:"Missing commit info"
    ~description:"Failed to retreive commit info"
    ~pp:(fun ppf trace ->
      Format.fprintf
        ppf
        "Failed to retreive commit info: %s@.Is the context initialized?"
        trace)
    Data_encoding.(obj1 (req "trace" string))
    (function Missing_commit_info trace -> Some trace | _ -> None)
    (fun trace -> Missing_commit_info trace) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.inconsistent_chain_store"
    ~title:"Inconsistent chain store"
    ~description:"Failed to load chain store"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Failed to load the chain store: could not retrieve head metadata.")
    Data_encoding.empty
    (function Inconsistent_chain_store -> Some () | _ -> None)
    (fun () -> Inconsistent_chain_store) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.fork_testchain_not_allowed"
    ~title:"Fork testchain not allowed"
    ~description:"Forking the test chain is not allowed"
    ~pp:(fun ppf () ->
      Format.fprintf
        ppf
        "Failed to fork the test chain: it is not allowed by the store's \
         configuration.")
    Data_encoding.empty
    (function Fork_testchain_not_allowed -> Some () | _ -> None)
    (fun () -> Fork_testchain_not_allowed) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_fork_testchain"
    ~title:"Cannot fork testchain"
    ~description:"Failed to fork testchain"
    ~pp:(fun ppf chain_id ->
      Format.fprintf
        ppf
        "Failed to fork the testchain: the testchain %a already exists."
        Chain_id.pp
        chain_id)
    Data_encoding.(obj1 (req "chain_id" Chain_id.encoding))
    (function Cannot_fork_testchain chain_id -> Some chain_id | _ -> None)
    (fun chain_id -> Cannot_fork_testchain chain_id) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.cannot_load_testchain"
    ~title:"Cannot load testchain"
    ~description:"Failed to load the testchain"
    ~pp:(fun ppf path ->
      Format.fprintf
        ppf
        "Failed to load the testchain as it was not found in %s."
        path)
    Data_encoding.(obj1 (req "path" string))
    (function Cannot_load_testchain path -> Some path | _ -> None)
    (fun path -> Cannot_load_testchain path) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.missing_activation_block"
    ~title:"Missing activation block"
    ~description:"Missing activation block while restoring snapshot"
    ~pp:(fun ppf (bh, ph, hm) ->
      Format.fprintf
        ppf
        "Failed to restore snapshot: the expected activation block %a \
         originating the protocol %a was not found for %a."
        Block_hash.pp
        bh
        Protocol_hash.pp
        ph
        History_mode.pp
        hm)
    Data_encoding.(
      obj3
        (req "block_hash" Block_hash.encoding)
        (req "protocol_hash" Protocol_hash.encoding)
        (req "history_mode" History_mode.encoding))
    (function
      | Missing_activation_block (bh, ph, hm) -> Some (bh, ph, hm) | _ -> None)
    (fun (bh, ph, hm) -> Missing_activation_block (bh, ph, hm)) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.inconsistent_protocol_commit_info"
    ~title:"Inconsistent protocol commit info"
    ~description:"Inconsistent protocol commit info while restoring snapshot"
    ~pp:(fun ppf (bh, ph) ->
      Format.fprintf
        ppf
        "Failed to restore snapshot: inconsistent commit info found for \
         transition block %a activating protocol %a."
        Block_hash.pp
        bh
        Protocol_hash.pp
        ph)
    Data_encoding.(
      obj2
        (req "block_hash" Block_hash.encoding)
        (req "protocol_hash" Protocol_hash.encoding))
    (function
      | Inconsistent_protocol_commit_info (bh, ph) -> Some (bh, ph) | _ -> None)
    (fun (bh, ph) -> Inconsistent_protocol_commit_info (bh, ph)) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.missing_activation_block_legacy"
    ~title:"Missing activation block legacy"
    ~description:"Missing activation block while restoring a legacy snapshot"
    ~pp:(fun ppf (bl, ph, hm) ->
      Format.fprintf
        ppf
        "Failed to restore legacy snapshot: the expected activation block \
         (level %ld) originating the protocol %a was not found for %a."
        bl
        Protocol_hash.pp
        ph
        History_mode.pp
        hm)
    Data_encoding.(
      obj3
        (req "block_hash" int32)
        (req "protocol_hash" Protocol_hash.encoding)
        (req "history_mode" History_mode.encoding))
    (function
      | Missing_activation_block_legacy (bl, ph, hm) ->
          Some (bl, ph, hm)
      | _ ->
          None)
    (fun (bl, ph, hm) -> Missing_activation_block_legacy (bl, ph, hm)) ;
  Error_monad.register_error_kind
    `Temporary
    ~id:"store.nissing_stored_data"
    ~title:"Missing stored data"
    ~description:"Failed to load stored data"
    ~pp:(fun ppf path ->
      Format.fprintf
        ppf
        "Failed to load shared data: no corresponding data found in file %s."
        path)
    Data_encoding.(obj1 (req "path" string))
    (function Missing_stored_data path -> Some path | _ -> None)
    (fun path -> Missing_stored_data path)
