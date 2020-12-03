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

open Error_monad

module type CONTEXT = sig
  type t

  type key = string list

  type value = Bytes.t

  val mem : t -> key -> bool Lwt.t

  val dir_mem : t -> key -> bool Lwt.t

  val get : t -> key -> value option Lwt.t

  val set : t -> key -> value -> t Lwt.t

  val copy : t -> from:key -> to_:key -> t option Lwt.t

  val remove_rec : t -> key -> t Lwt.t

  type key_or_dir = [`Key of key | `Dir of key]

  val fold :
    t -> key -> init:'a -> f:(key_or_dir -> 'a -> 'a Lwt.t) -> 'a Lwt.t

  val set_protocol : t -> Protocol_hash.t -> t Lwt.t

  val fork_test_chain :
    t -> protocol:Protocol_hash.t -> expiration:Time.Protocol.t -> t Lwt.t

  type cursor

  val empty_cursor : t -> cursor

  val set_cursor: t -> key -> cursor -> t Lwt.t

  val copy_cursor : cursor -> from:cursor -> to_:key -> cursor Lwt.t

  val fold_rec :
    ?depth:int ->
    t -> key -> init:'a -> f:(key -> cursor -> 'a -> 'a Lwt.t) -> 'a Lwt.t
end

module Witness : sig
  type (_, _) eq = Refl : ('a, 'a) eq
  type 'a t

  val make : unit -> 'a t
  val eq : 'a t -> 'b t -> ('a, 'b) eq option
end = struct
  type (_, _) eq = Refl : ('a, 'a) eq
  type _ equality = ..

  module type Inst = sig
    type t
    type _ equality += Eq : t equality
  end

  type 'a t = (module Inst with type t = 'a)

  let make : type a. unit -> a t =
    fun () ->
    let module Inst = struct
        type t = a
        type _ equality += Eq : t equality
      end in
    (module Inst)

  let eq : type a b. a t -> b t -> (a, b) eq option =
    fun (module A) (module B) -> match A.Eq with B.Eq -> Some Refl | _ -> None
end

module Context = struct
  type key = string list

  type value = Bytes.t

  type ('ctxt, 'cursor) ops =
    (module CONTEXT with type t = 'ctxt and type cursor = 'cursor)

  type _ kind = ..

  type ('a, 'b) witness = 'a Witness.t * 'b Witness.t

  let witness () = Witness.make (), Witness.make ()

  let equal (a, b) (c, d) = Witness.eq a c, Witness.eq b d

  type t =
    Context : {kind : 'a kind;
               ctxt : 'a; ops : ('a, 'b) ops;
               wit: ('a, 'b) witness }
              -> t

  let mem (Context {ops = (module Ops); ctxt; _}) key = Ops.mem ctxt key

  let set (Context ({ops = (module Ops); ctxt; _} as c)) key value =
    Ops.set ctxt key value
    >>= fun ctxt -> Lwt.return (Context {c with ctxt})

  let dir_mem (Context {ops = (module Ops); ctxt; _}) key =
    Ops.dir_mem ctxt key

  let get (Context {ops = (module Ops); ctxt; _}) key = Ops.get ctxt key

  let copy (Context ({ops = (module Ops); ctxt; _} as c)) ~from ~to_ =
    Ops.copy ctxt ~from ~to_
    >>= function
    | Some ctxt ->
        Lwt.return_some (Context {c with ctxt})
    | None ->
        Lwt.return_none

  let remove_rec (Context ({ops = (module Ops); ctxt; _ } as c)) key =
    Ops.remove_rec ctxt key
    >>= fun ctxt -> Lwt.return (Context {c with ctxt})

  type key_or_dir = [`Key of key | `Dir of key]

  let fold (Context {ops = (module Ops); ctxt; _}) key ~init ~f =
    Ops.fold ctxt key ~init ~f

  let set_protocol (Context ({ops = (module Ops); ctxt; _ } as c))
      protocol_hash =
    Ops.set_protocol ctxt protocol_hash
    >>= fun ctxt -> Lwt.return (Context {c with ctxt})

  let fork_test_chain (Context ({ops = (module Ops); ctxt; _} as c))
      ~protocol ~expiration =
    Ops.fork_test_chain ctxt ~protocol ~expiration
    >>= fun ctxt -> Lwt.return (Context {c with ctxt})

  type cursor =
    Cursor: { ops: ('a, 'b) ops; cursor: 'b; wit: ('a, 'b) witness} -> cursor

  let empty_cursor (Context {ops = (module Ops) as ops; wit; ctxt; _}) =
    Cursor { ops; wit; cursor = Ops.empty_cursor ctxt }

  let set_cursor
        (Context ({ops = (module Ops); ctxt; _} as c))
        key
        (Cursor {cursor; wit; _ })
    =
    match equal c.wit wit with
    | Some Refl, Some Refl ->
       Ops.set_cursor ctxt key cursor >|= fun ctxt ->
       Context { c with ctxt }
    | _ -> assert false

  let copy_cursor
        (Cursor {ops = (module Ops) as ops; cursor; wit; _})
        ~from:(Cursor {cursor=from; wit=wit2; _ }) ~to_ =
    match equal wit wit2 with
    | Some Refl, Some Refl ->
       Ops.copy_cursor cursor ~from ~to_
       >|= fun cursor -> Cursor { ops; wit; cursor }
    | _ -> assert false

  let fold_rec
        ?depth (Context {ops = (module Ops) as ops; ctxt; wit; _}) key ~init ~f
    =
    Ops.fold_rec ctxt ?depth key ~init ~f:(fun k cursor acc ->
        let cursor = Cursor { ops; wit; cursor } in
        f k cursor acc)

end

type validation_result = {
  context : Context.t;
  fitness : Fitness.t;
  message : string option;
  max_operations_ttl : int;
  last_allowed_fork_level : Int32.t;
}

type quota = {max_size : int; max_op : int option}

type rpc_context = {
  block_hash : Block_hash.t;
  block_header : Block_header.shell_header;
  context : Context.t;
}
