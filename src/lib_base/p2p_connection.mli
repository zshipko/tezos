(**************************************************************************)
(*                                                                        *)
(*    Copyright (c) 2014 - 2018.                                          *)
(*    Dynamic Ledger Solutions, Inc. <contact@tezos.com>                  *)
(*                                                                        *)
(*    All rights reserved. No warranty, explicit or implicit, provided.   *)
(*                                                                        *)
(**************************************************************************)

module Id : sig

  type t = P2p_addr.t * P2p_addr.port option
  val compare : t -> t -> int
  val equal : t -> t -> bool
  val pp : Format.formatter -> t -> unit
  val pp_opt : Format.formatter -> t option -> unit
  val to_string : t -> string
  val encoding : t Data_encoding.t
  val is_local : t -> bool
  val is_global : t -> bool
  val of_point : P2p_point.Id.t -> t
  val to_point : t -> P2p_point.Id.t option
  val to_point_exn : t -> P2p_point.Id.t

end

module Map : Map.S with type key = Id.t
module Set : Set.S with type elt = Id.t
module Table : Hashtbl.S with type key = Id.t

(** Information about a connection *)
module Info : sig

  type 'meta t = {
    incoming : bool;
    peer_id : P2p_peer_id.t;
    id_point : Id.t;
    remote_socket_port : P2p_addr.port;
    versions : P2p_version.t list ;
    private_node : bool ;
    remote_metadata : 'meta ;
  }

  val pp :
    (Format.formatter -> 'meta -> unit) ->
    Format.formatter -> 'meta t -> unit
  val encoding : 'meta Data_encoding.t -> 'meta t Data_encoding.t

end

module Pool_event : sig

  type t =

    | Too_few_connections
    | Too_many_connections

    | New_point of P2p_point.Id.t
    | New_peer of P2p_peer_id.t

    | Gc_points
    (** Garbage collection of known point table has been triggered. *)

    | Gc_peer_ids
    (** Garbage collection of known peer_ids table has been triggered. *)

    (* Connection-level events *)

    | Incoming_connection of P2p_point.Id.t
    (** We accept(2)-ed an incoming connection *)
    | Outgoing_connection of P2p_point.Id.t
    (** We connect(2)-ed to a remote endpoint *)
    | Authentication_failed of P2p_point.Id.t
    (** Remote point failed authentication *)

    | Accepting_request of P2p_point.Id.t * Id.t * P2p_peer_id.t
    (** We accepted a connection after authentifying the remote peer. *)
    | Rejecting_request of P2p_point.Id.t * Id.t * P2p_peer_id.t
    (** We rejected a connection after authentifying the remote peer. *)
    | Request_rejected of P2p_point.Id.t * (Id.t * P2p_peer_id.t) option
    (** The remote peer rejected our connection. *)

    | Connection_established of Id.t * P2p_peer_id.t
    (** We succesfully established a authentified connection. *)

    | Swap_request_received of { source : P2p_peer_id.t }
    (** A swap request has been received. *)
    | Swap_ack_received of { source : P2p_peer_id.t }
    (** A swap ack has been received *)
    | Swap_request_sent of { source : P2p_peer_id.t }
    (** A swap request has been sent *)
    | Swap_ack_sent of { source : P2p_peer_id.t }
    (** A swap ack has been sent *)
    | Swap_request_ignored of { source : P2p_peer_id.t }
    (** A swap request has been ignored *)
    | Swap_success of { source : P2p_peer_id.t }
    (** A swap operation has succeeded *)
    | Swap_failure of { source : P2p_peer_id.t }
    (** A swap operation has failed *)

    | Disconnection of P2p_peer_id.t
    (** We decided to close the connection. *)
    | External_disconnection of P2p_peer_id.t
    (** The connection was closed for external reason. *)

  val encoding : t Data_encoding.t

end
