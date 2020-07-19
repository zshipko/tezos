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

let depth = 8 - List.length index_path

let pp_key ppf k = Format.pp_print_string ppf (String.concat "/" (""::k))

let fold_keys ~init ~f ~index_path ctxt =
  let rec dig len path acc =
    if Compare.Int.(len <= 0) then
      f path acc
    else
      Context.fold ctxt path ~init:acc ~f:(fun k acc ->
          match k with `Dir k | `Key k -> dig (len - 1) k acc)
  in
  dig depth index_path init

let job =
  Context.init context_dir
  >>= fun index ->
  let hash = Context_hash.of_b58check_exn context_hash in
  Context.checkout index hash >>= fun ctxtopt ->
  let ctxt = match ctxtopt with None -> assert false | Some ctxt -> ctxt in
  fold_keys ~init:(ctxt,0) ~f: (fun k (ctxt,i) ->
      let to_ = match k with
        | [x1; x2; "snapshot"; d1; d2; _d3; _d4; d5] ->
            [x1; x2; "tmp_snapshot"; d1; d2; d5]
        | _ -> assert false
      in
(*
      Format.eprintf "copying from %a to %a@."
        pp_key k pp_key to_;
*)
       Context.copy ctxt ~from:k ~to_ >>= function
       | None ->
           Format.eprintf "copying failed: from %a to %a@."
             pp_key k pp_key to_;
           assert false
       | Some ctxt -> Lwt.return (ctxt, i+1))
     ~index_path ctxt
   >>= fun (ctxt,files) ->
   Format.eprintf "copied: %d@." files;
   Context.commit ~time:Time.Protocol.epoch ctxt
   >>= fun ch ->
   Format.eprintf "Committed: %a@." Context_hash.pp ch;
   Lwt.return ()

let () = Lwt_main.run job
