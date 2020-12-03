(* How to run

   $ cp -a ../../../tezos-node-mainnet ../../../tezos-node-mainnet-copy
   $ dune exec ./test.exe -- ../../../tezos-node-mainnet-copy/context CoWYb26o6jetBijjioprzNn7s5ppNFrLsPCBWJzBjDWTZtwnoya5
*)

(* ex. "../../../tezos-node-mainnet-copy/context" *)
let context_dir = Sys.argv.(1)

(* ex. "CoWYb26o6jetBijjioprzNn7s5ppNFrLsPCBWJzBjDWTZtwnoya5" *)
let context_hash = Sys.argv.(2)

(* full test, copies 2872383 files *)
let index_path = ["rolls"; "owner"; "snapshot"]

(* small test, copies 84801 files
let index_path = ["rolls"; "owner"; "snapshot"; "296"]
*)

let depth = 8

let pp_key ppf k = Format.pp_print_string ppf (String.concat "/" (""::k))

let job =
  Context.init context_dir
  >>= fun index ->
  let hash = Context_hash.of_b58check_exn context_hash in
  Context.checkout index hash >>= fun ctxtopt ->
  let ctxt = match ctxtopt with None -> assert false | Some ctxt -> ctxt in
  let dir = Context.empty_cursor ctxt in
  Context.fold_rec ~depth ctxt index_path ~init:(dir, 0) ~f: (fun k v (dir, i) ->
      let to_ = match k with
        | [_; _; "snapshot"; d1; d2; _d3; _d4; d5] ->
            [d1; d2; d5]
        | p -> Fmt.epr "XXX path=%a\n%!" Fmt.(Dump.list string) p; assert false
      in
      Context.copy_cursor dir ~from:v ~to_ >|= fun dir ->
      (dir, i+1))
  >>= fun (dir, files) ->
  Context.set_cursor ctxt ["rolls"; "owner"; "tmp_snapshot"] dir >>= fun ctxt ->
   Format.eprintf "copied: %d@." files;
   Context.commit ~time:Time.Protocol.epoch ctxt
   >>= fun ch ->
   Format.eprintf "Committed: %a@." Context_hash.pp ch;
   Lwt.return ()

let () = Lwt_main.run job
