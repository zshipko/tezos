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

(** Basic blocks *)

let genesis_hash =
  Block_hash.of_b58check_exn
    "BLockGenesisGenesisGenesisGenesisGenesisGeneskvg68z"

let genesis_protocol =
  Protocol_hash.of_b58check_exn
    "ProtoDemoNoopsDemoNoopsDemoNoopsDemoNoopsDemo6XBoYp"

let proto =
  match Registered_protocol.get genesis_protocol with
  | None ->
      assert false
  | Some proto ->
      proto

module Proto = (val proto)

let zero = Bytes.create 0

(* adds n blocks on top of an initialized chain *)
let make_empty_chain chain_store n : Block_hash.t Lwt.t =
  Store.Block.read_block_opt chain_store genesis_hash
  >|= Option.unopt_assert ~loc:__POS__
  >>= fun genesis ->
  Store.Block.context_exn chain_store genesis
  >>= fun empty_context ->
  let header = Store.Block.header genesis in
  let timestamp = Store.Block.timestamp genesis in
  let empty_context_hash = Context.hash ~time:timestamp empty_context in
  Context.commit ~time:header.shell.timestamp empty_context
  >>= fun context ->
  let header = {header with shell = {header.shell with context}} in
  let context_hash = empty_context_hash in
  let message = None in
  let max_operations_ttl = 0 in
  let last_allowed_fork_level = 0l in
  let rec loop lvl pred =
    if lvl >= n then return pred
    else
      let header =
        {
          header with
          shell =
            {header.shell with predecessor = pred; level = Int32.of_int lvl};
        }
      in
      let result =
        {
          Block_validation.validation_store =
            {
              context_hash;
              message;
              max_operations_ttl;
              last_allowed_fork_level;
            };
          block_metadata = zero;
          ops_metadata = [];
        }
      in
      Store.Block.store_block
        chain_store
        ~block_header:header
        ~operations:[]
        result
      >>=? fun _ -> loop (lvl + 1) (Block_header.hash header)
  in
  loop 1 genesis_hash
  >>= function
  | Ok b ->
      Lwt.return b
  | Error err ->
      Error_monad.pp_print_error Format.err_formatter err ;
      assert false

(* helper functions ------------------------------------- *)

(* wall clock time of a unit function *)
let time1 (f : unit -> 'a) : 'a * float =
  let t = Unix.gettimeofday () in
  let res = f () in
  let wall_clock = Unix.gettimeofday () -. t in
  (res, wall_clock)

(* returns result from first run and average time of [runs] runs *)
let time ?(runs = 1) f =
  if runs < 1 then invalid_arg "time negative arg"
  else
    let rec loop cnt sum =
      if cnt = runs then sum
      else
        let (_, t) = time1 f in
        loop (cnt + 1) (sum +. t)
    in
    let (res, t) = time1 f in
    let sum = loop 1 t in
    (res, sum /. float runs)

let rec repeat f n =
  if n < 0 then invalid_arg "repeat: negative arg"
  else if n = 0 then return_unit
  else f () >>=? fun () -> repeat f (n - 1)

(* ----------------------------------------------------- *)

(* returns the predecessor at distance one, reading the header *)
let linear_predecessor chain_store (bh : Block_hash.t) :
    Block_hash.t option Lwt.t =
  Store.Block.read_block_opt chain_store bh
  >|= Option.unopt_assert ~loc:__POS__
  >>= fun b ->
  Store.Block.read_predecessor_opt chain_store b
  >|= function None -> None | Some pred -> Some (Store.Block.hash pred)

(* returns the predecessors at distance n, traversing all n intermediate blocks *)
let linear_predecessor_n chain_store (bh : Block_hash.t) (distance : int) :
    Block_hash.t option Lwt.t =
  (* let _ = Printf.printf "LP: %4i " distance; print_block_h chain bh in *)
  if distance < 1 then invalid_arg "distance<1"
  else
    let rec loop bh distance =
      if distance = 0 then Lwt.return_some bh (* reached distance *)
      else
        linear_predecessor chain_store bh
        >>= function
        | None -> Lwt.return_none | Some pred -> loop pred (distance - 1)
    in
    loop bh distance

(* Tests that the linear predecessor defined above and the
   exponential predecessor implemented in State.predecessor_n
   return the same block and it is the block at the distance
   requested *)
let test_pred (base_dir : string) : unit tzresult Lwt.t =
  let size_chain = 1000 in
  Shell_test_helpers.init_chain base_dir
  >>= fun store ->
  let chain_store = Store.main_chain_store store in
  make_empty_chain chain_store size_chain
  >>= fun head ->
  let test_once distance =
    linear_predecessor_n chain_store head distance
    >>= fun lin_res ->
    Store.Block.read_block_opt chain_store head
    >|= Option.unopt_assert ~loc:__POS__
    >>= fun head_block ->
    Store.Block.read_ancestor_hash
      chain_store
      (Store.Block.hash head_block)
      ~distance
    >>=? fun exp_res ->
    match (lin_res, exp_res) with
    | (None, None) ->
        return_unit
    | (None, Some _) | (Some _, None) ->
        Assert.fail_msg "mismatch between exponential and linear predecessor_n"
    | (Some lin_res, Some exp_res) ->
        (* check that the two results are the same *)
        assert (lin_res = exp_res) ;
        Store.Block.read_block_opt chain_store lin_res
        >|= Option.unopt_assert ~loc:__POS__
        >>= fun pred ->
        let level_pred = Int32.to_int (Store.Block.level pred) in
        Store.Block.read_block_opt chain_store head
        >|= Option.unopt_assert ~loc:__POS__
        >>= fun head ->
        let level_start = Int32.to_int (Store.Block.level head) in
        (* check distance using the level *)
        assert (level_start - distance = level_pred) ;
        return_unit
  in
  let _ = Random.self_init () in
  let range = size_chain in
  let repeats = 100 in
  repeat (fun () -> test_once (1 + Random.int range)) repeats

let seed =
  let receiver_id =
    P2p_peer.Id.of_string_exn (String.make P2p_peer.Id.size 'r')
  in
  let sender_id =
    P2p_peer.Id.of_string_exn (String.make P2p_peer.Id.size 's')
  in
  {Block_locator.receiver_id; sender_id}

(* compute locator using the linear predecessor *)
let compute_linear_locator chain_store ~size block =
  let block_hash = Store.Block.hash block in
  let header = Store.Block.header block in
  Block_locator.compute
    ~get_predecessor:(linear_predecessor_n chain_store)
    block_hash
    header
    ~size
    seed

(* given the size of a chain, returns the size required for a locator
   to reach genesis *)
let compute_size_locator size_chain =
  let repeats = 10. in
  int_of_float ((log (float size_chain /. repeats) /. log 2.) -. 1.) * 10

(* given the size of a locator, returns the size of the chain that it
   can cover back to genesis *)
let compute_size_chain size_locator =
  let repeats = 10. in
  int_of_float (repeats *. (2. ** float (size_locator + 1)))

(* test if the linear and exponential locator are the same and outputs
   their timing.
   Run the test with:
   $ dune build @runbench_locator
   Copy the output to a file timing.dat and plot it with:
   $ generate_locator_plot.sh timing.dat
*)
(*
   chain 1 year   518k   covered by locator 150
   chain 2 months 86k    covered by locator 120
*)
let test_locator base_dir =
  let size_chain = 80000 in
  (* timing locators with average over [runs] times *)
  let runs = 10 in
  let _ = Printf.printf "#runs %i\n" runs in
  (* limit after which exp should go linear *)
  let exp_limit = compute_size_chain 120 in
  let _ = Printf.printf "#exp_limit %i\n" exp_limit in
  (* size after which locator always reaches genesis *)
  let locator_limit = compute_size_locator size_chain in
  let _ = Printf.printf "#locator_limit %i\n" locator_limit in
  Shell_test_helpers.init_chain base_dir
  >>= fun store ->
  let chain_store = Store.main_chain_store store in
  time1 (fun () -> make_empty_chain chain_store size_chain)
  |> fun (res, t_chain) ->
  let _ =
    Printf.printf
      "#size_chain %i built in %f sec\n#      size      exp       lins\n"
      size_chain
      t_chain
  in
  res
  >>= fun head ->
  let check_locator size : unit tzresult Lwt.t =
    Store.Chain.caboose chain_store
    >>= fun (caboose, _) ->
    Store.Block.read_block chain_store head
    >>=? fun block ->
    time ~runs (fun () ->
        Store.Block.compute_locator chain_store ~size block seed)
    |> fun (l_exp, t_exp) ->
    time ~runs (fun () ->
        compute_linear_locator chain_store ~caboose ~size block)
    |> fun (l_lin, t_lin) ->
    l_exp
    >>= fun l_exp ->
    l_lin
    >>= fun l_lin ->
    let (_, l_exp) = (l_exp : Block_locator.t :> _ * _) in
    let (_, l_lin) = (l_lin : Block_locator.t :> _ * _) in
    let _ = Printf.printf "%10i %f %f\n" size t_exp t_lin in
    List.iter2
      (fun hn ho ->
        if not (Block_hash.equal hn ho) then
          Assert.fail_msg "Invalid locator %i" size)
      l_exp
      l_lin ;
    return_unit
  in
  let stop = locator_limit + 20 in
  let rec loop size =
    if size < stop then check_locator size >>=? fun _ -> loop (size + 5)
    else return_unit
  in
  loop 1

let wrap n f =
  Alcotest_lwt.test_case n `Quick (fun _ () ->
      Lwt_utils_unix.with_tempdir "tezos_test_" (fun dir ->
          f dir
          >>= function
          | Ok () ->
              Lwt.return_unit
          | Error error ->
              Format.kasprintf Stdlib.failwith "%a" pp_print_error error))

let tests = [wrap "test pred" test_pred]

let bench = [wrap "test locator" test_locator]

let tests =
  try if Sys.argv.(1) = "--no-bench" then tests else tests @ bench
  with _ -> tests @ bench

let () =
  Alcotest_lwt.run ~argv:[|""|] "tezos-shell" [("locator", tests)]
  |> Lwt_main.run
