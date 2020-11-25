(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2019-2020 Nomadic Labs <contact@nomadic-labs.com>           *)
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

module Migrate_from_007_to_008 = struct
  let rec path_remove_prefix = function
    | (h1 :: t1, h2 :: t2) when String.(equal h1 h2) ->
        path_remove_prefix (t1, t2)
    | ([], t2) ->
        return t2
    | (_, _) ->
        (*can't happens because the path have been digged starting with that
          prefix *)
        assert false

  let fold_keys (type t) ~index ~init ~f ~index_path ctxt =
    let (module Index : Storage_functors.INDEX with type t = t) = index in
    let rec dig len path acc =
      if Compare.Int.(len <= 0) then
        path_remove_prefix (index_path, path)
        >>=? fun key_path ->
        match Index.of_path key_path with
        | None ->
            (* can't happens as the path have been digged and can't be false *)
            assert false
        | Some value ->
            f path value acc
      else
        Raw_context.fold ctxt path ~init:(Ok acc) ~f:(fun k acc ->
            Lwt.return acc
            >>=? fun acc ->
            match k with `Dir k | `Key k -> dig (len - 1) k acc)
    in
    dig Index.path_length index_path init

  let migrate_indexed_storage (type t) ctxt ~from_index ~to_index ~index_path =
    let (module To_index : Storage_functors.INDEX with type t = t) =
      to_index
    in
    let tmp_index_path =
      let rev_path = List.rev index_path in
      List.rev (("tmp_" ^ List.hd rev_path) :: List.tl rev_path)
    in
    fold_keys
      ~index:from_index
      ~init:(ctxt, false)
      ~f:(fun old_path value (ctxt, _has_value) ->
        let new_path = tmp_index_path @ To_index.to_path value [] in
        Raw_context.copy ctxt ~from:old_path ~to_:new_path
        >>=? fun ctxt -> return (ctxt, true))
      ~index_path
      ctxt
    >>=? fun (ctxt, has_value) ->
    if has_value then
      Raw_context.remove_rec ctxt index_path
      >>= fun ctxt ->
      Raw_context.copy ctxt tmp_index_path index_path
      >>=? fun ctxt ->
      Raw_context.remove_rec ctxt tmp_index_path >>= fun ctxt -> return ctxt
    else return ctxt
end

(* This is the genesis protocol: initialise the state *)
let prepare_first_block ctxt ~typecheck ~level ~timestamp ~fitness =
  Raw_context.prepare_first_block ~level ~timestamp ~fitness ctxt
  >>=? fun (previous_protocol, ctxt, prev_blocks_per_voting_period) ->
  match previous_protocol with
  | Genesis param ->
      Commitment_storage.init ctxt param.commitments
      >>=? fun ctxt ->
      Roll_storage.init ctxt
      >>=? fun ctxt ->
      Seed_storage.init ctxt
      >>=? fun ctxt ->
      Contract_storage.init ctxt
      >>=? fun ctxt ->
      Bootstrap_storage.init
        ctxt
        ~typecheck
        ?ramp_up_cycles:param.security_deposit_ramp_up_cycles
        ?no_reward_cycles:param.no_reward_cycles
        param.bootstrap_accounts
        param.bootstrap_contracts
      >>=? fun ctxt ->
      Roll_storage.init_first_cycles ctxt
      >>=? fun ctxt ->
      Vote_storage.init
        ctxt
        ~start_position:(Level_storage.current ctxt).level_position
      >>=? fun ctxt ->
      Storage.Block_priority.init ctxt 0
      >>=? fun ctxt -> Vote_storage.update_listings ctxt
  | Delphi_007 ->
      Storage.Vote.Current_period_kind_007.delete ctxt
      >>=? fun ctxt ->
      let level_position = (Level_storage.current ctxt).level_position in
      let voting_period_index =
        Int32.(div (succ level_position) prev_blocks_per_voting_period)
      in
      let start_position = level_position in
      Storage.Vote.Current_period.init
        ctxt
        {index = voting_period_index; kind = Proposal; start_position}
      >>=? fun ctxt ->
      Storage.Vote.Pred_period_kind.init ctxt Promotion_vote
      >>=? fun ctxt ->
      Storage.Sapling.Next.init ctxt
      >>=? fun ctxt ->
      let contract_index_007 =
        ( module Storage.Make_index (Contract_repr.Index_007)
        : Storage_functors.INDEX
          with type t = Contract_repr.t )
      in
      let contract_index =
        ( module Storage.Make_index (Contract_repr.Index)
        : Storage_functors.INDEX
          with type t = Contract_repr.t )
      in
      Migrate_from_007_to_008.migrate_indexed_storage
        ctxt
        ~from_index:contract_index_007
        ~to_index:contract_index
        ~index_path:["contracts"; "index"]
      >>=? fun ctxt ->
      Storage.Contract.fold
        ~init:(ok ctxt)
        ~f:(fun contract ctxt ->
          Lwt.return ctxt
          >>=? fun ctxt ->
          Migrate_from_007_to_008.migrate_indexed_storage
            ~from_index:contract_index_007
            ~to_index:contract_index
            ~index_path:
              ( ["contracts"; "index"]
              @ Contract_repr.Index.to_path contract []
              @ ["delegated"] )
            ctxt)
        ctxt
      >>=? fun ctxt ->
      let bigmap_index_007 =
        ( module Storage.Make_index (Storage.Big_map.Index_007)
        : Storage_functors.INDEX
          with type t = Storage.Big_map.id )
      in
      let bigmap_index =
        ( module Storage.Make_index (Storage.Big_map.Index)
        : Storage_functors.INDEX
          with type t = Storage.Big_map.id )
      in
      Migrate_from_007_to_008.migrate_indexed_storage
        ctxt
        ~from_index:bigmap_index_007
        ~to_index:bigmap_index
        ~index_path:["big_maps"; "index"]
      >>=? fun ctxt ->
      let rolls_index_007 =
        ( module Storage.Make_index (Roll_repr.Index_007)
        : Storage_functors.INDEX
          with type t = Roll_repr.t )
      in
      let rolls_index =
        (module Storage.Make_index (Roll_repr.Index) : Storage_functors.INDEX
          with type t = Roll_repr.t )
      in
      let snapshot_rolls_index_007 =
        let (module Rolls_index_007) = rolls_index_007 in
        ( module Storage_functors.Pair
                   (Storage.Roll.Snapshoted_owner_index)
                   (Rolls_index_007) : Storage_functors.INDEX
          with type t = (Cycle_repr.t * int) * Roll_repr.t )
      in
      let snapshot_rolls_index =
        let (module Rolls_index) = rolls_index in
        ( module Storage_functors.Pair
                   (Storage.Roll.Snapshoted_owner_index)
                   (Rolls_index) : Storage_functors.INDEX
          with type t = (Cycle_repr.t * int) * Roll_repr.t )
      in
      Migrate_from_007_to_008.migrate_indexed_storage
        ctxt
        ~from_index:rolls_index_007
        ~to_index:rolls_index
        ~index_path:["rolls"; "index"]
      >>=? fun ctxt ->
      Migrate_from_007_to_008.migrate_indexed_storage
        ctxt
        ~from_index:snapshot_rolls_index_007
        ~to_index:snapshot_rolls_index
        ~index_path:["rolls"; "owner"; "snapshot"]
      >>=? fun ctxt ->
      Migrate_from_007_to_008.migrate_indexed_storage
        ctxt
        ~from_index:rolls_index_007
        ~to_index:rolls_index
        ~index_path:["rolls"; "owner"; "current"]

let prepare ctxt ~level ~predecessor_timestamp ~timestamp ~fitness =
  Raw_context.prepare ~level ~predecessor_timestamp ~timestamp ~fitness ctxt
