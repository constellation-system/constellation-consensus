// Copyright © 2024-26 The Johns Hopkins Applied Physics Laboratory LLC.
//
// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.  If you
// would like to purchase a commercial license for this software, please
// contact APL’s Tech Transfer at 240-592-0817 or
// techtransfer@jhuapl.edu.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public
// License along with this program.  If not, see
// <https://www.gnu.org/licenses/>.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::Sender;
use std::sync::mpsc::SendError;
use std::time::Instant;

use constellation_auth::authn::AuthNedDestruct;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::dispatch::SessionDispatch;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::consensus_ctl::ConsensusCtlRound;
use constellation_component_common::bus::types::SessionDispatchTypes;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::large_obj::LargeObjSender;
use constellation_streams::multicast::MulticastStreamIdx;
use log::debug;
use log::warn;

use crate::types::ConsensusPeerTypes;
use crate::types::ConsensusRecvTypes;
use crate::types::ConsensusStateTypes;

pub(crate) struct PeerSessionDispatch<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    config: LargeObjProtoConfig<(), (), Types::IDsConfig>,
    sessions: Arc<Mutex<HashMap<Types::PeerPrin, PeerSession>>>,
    state: PeerSessionState<IDTypes, ProtoTypes, Types>,
    send: Sender<Vec<Types::HashID>>,
    state_notify: Notify,
}

pub(crate) struct PeerSessionRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    id_types: PhantomData<IDTypes>,
    proto_types: PhantomData<ProtoTypes>,
    send: Sender<Vec<Types::HashID>>,
    notify: Notify
}

struct PeerSessionMsgState<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    /// Active peers to receive this message.
    pending: HashMap<Types::PeerPrin, Notify>,
    msg: ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>
}

pub(crate) struct PeerSessionState<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    /// Active peer sessions.
    peers: Arc<Mutex<Vec<(Types::PeerPrin, Notify)>>>,
    /// Pending messages.
    pending: Arc<Mutex<Vec<PeerSessionMsgState<IDTypes, ProtoTypes, Types>>>>,
}

pub(crate) struct PeerSessionMsgs<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    proto_types: PhantomData<ProtoTypes>,
    state: PeerSessionState<IDTypes, ProtoTypes, Types>,
    peer: Types::PeerPrin
}

struct PeerSession {
    local_shutdown: ShutdownFlag
}

struct PeerSessionDispatchTypes<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    id: PhantomData<IDTypes>,
    proto: PhantomData<ProtoTypes>,
    types: PhantomData<Types>
}

#[derive(Debug)]
pub(crate) enum PeerSessionDispatchError<Prin, Encoder, Decoder, IDs> {
    Proto {
        err: LargeObjProtoCreateError<Encoder, Decoder, IDs>
    },
    Exists {
        prin: Prin
    },
    MutexPoison
}

#[derive(Debug)]
pub(crate) enum PeerSessionRecvError<HashID> {
    Send {
        err: SendError<Vec<HashID>>
    },
    IO {
        err: std::io::Error
    },
}

#[derive(Debug)]
pub(crate) enum PeerSessionAddMsgsError<Encode> {
    LargeObj {
        err: LargeObjProtoAddOutboundError<Encode>
    },
    IO {
        err: std::io::Error
    },
}

impl<IDTypes, ProtoTypes, Types> PeerSessionMsgState<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<
            Types::Hash,
            ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>,
            WrapperCodec,
            F
        >,
        sent: &mut bool,
        peer: &Types::PeerPrin
    ) -> Result<bool, PeerSessionAddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Create
            + Encoder<ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>>,
        WrapperCodec::Config: Default,
        F: Frags {
        // XXX the protocol should be refactored to send batches.
        if let Some(notify) = self.pending.remove(peer) {
            let _ = sender.add_outbound(&self.msg)
                .map_err(|err| PeerSessionAddMsgsError::LargeObj {
                    err: err
                })?;

            *sent = true;

            notify.notify()
                .map_err(|err| PeerSessionAddMsgsError::IO {
                    err: err
                })?;
        }

        Ok(!self.pending.is_empty())
    }
}

impl<IDTypes, ProtoTypes, Types> PeerSessionState<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    fn add_peer(
        &self,
        peer: Types::PeerPrin,
        notify: Notify
    ) -> Result<(), MutexPoison> {
        self.peers
            .lock()
            .map_err(|_| MutexPoison)?
            .push((peer, notify));

        Ok(())
    }

    pub(crate) fn add_outbound(
        &self,
        msg: ConsensusCtlRound<IDTypes::RoundID, Types::HashID, Types::Seal>
    ) -> Result<(), MutexPoison> {
        let peers = self
            .peers
            .lock()
            .map_err(|_| MutexPoison)?
            .iter()
            .cloned()
            .collect();
        let pending = PeerSessionMsgState {
            pending: peers,
            msg: ConsensusCtl::Round(msg)
        };

        self.pending
            .lock()
            .map_err(|_| MutexPoison)?
            .push(pending);

        Ok(())
    }

    fn add_msgs<WrapperCodec, F>(
        &self,
        sender: &mut LargeObjSender<
            Types::Hash,
            ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>,
            WrapperCodec,
            F
        >,
        peer: &Types::PeerPrin
    ) -> Result<
        Option<Instant>,
        WithMutexPoison<PeerSessionAddMsgsError<WrapperCodec::EncodeError>>
    >
    where
        WrapperCodec: Clone + Create
            + Encoder<ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>>,
        WrapperCodec::Config: Default,
        F: Frags {
        let mut outerr: Option<
            PeerSessionAddMsgsError<WrapperCodec::EncodeError>
        > = None;
        let mut sent = false;

        self.pending
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .retain_mut(|pending| {
                match pending.add_msgs(sender, &mut sent, peer) {
                    Ok(out) => out,
                    Err(err) => {
                        outerr = Some(err);
                        true
                    }
                }
            });

        if let Some(err) = outerr {
            Err(WithMutexPoison::Inner { err: err })
        } else if sent {
            Ok(Some(Instant::now()))
        } else {
            Ok(None)
        }
    }
}

impl<IDTypes, ProtoTypes, Types> Clone
    for PeerSessionRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    fn clone(&self) -> Self {
        PeerSessionRecv {
            id_types: self.id_types,
            proto_types: self.proto_types,
            notify: self.notify.clone(),
            send: self.send.clone(),
        }
    }
}

impl<IDTypes, ProtoTypes, Types> Clone
    for PeerSessionState<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    fn clone(&self) -> Self {
        PeerSessionState {
            pending: self.pending.clone(),
            peers: self.peers.clone()
        }
    }
}

impl<IDTypes, ProtoTypes, Types> Clone
    for PeerSessionMsgs<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    fn clone(&self) -> Self {
        PeerSessionMsgs {
            proto_types: self.proto_types,
            state: self.state.clone(),
            peer: self.peer.clone()
        }
    }
}

impl<IDTypes, ProtoTypes, Types>
    LargeObjMsgs<Types::Hash, ConsensusCtl<IDTypes::RoundID, Types::HashID,
                                           Types::Seal>>
    for PeerSessionMsgs<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    type AddMsgsError<Encode>
        = WithMutexPoison<PeerSessionAddMsgsError<Encode>>
    where
        Encode: Debug + Display + ScopedError;

    #[inline]
    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<
            Types::Hash,
            ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>,
            WrapperCodec,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Create
            + Encoder<ConsensusCtl<IDTypes::RoundID,
                                   Types::HashID,
                                   Types::Seal>>,
        WrapperCodec::Config: Default,
        F: Frags {
        self.state.add_msgs(sender, &self.peer)
    }
}

impl<IDTypes, ProtoTypes, Types>
    AuthNMsgRecv<Types::PeerPrin, Types::CtlAuthNMsg>
    for PeerSessionRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    /// Errors that can occur reporting messages.
    type RecvError = PeerSessionRecvError<Types::HashID>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        msg: Types::CtlAuthNMsg
    ) -> Result<(), Self::RecvError> {
        let (prin, msg) = msg.take();

        debug!(target: "peer-session-recv",
               "received message from peer {}",
               prin);

        match msg {
            ConsensusCtl::Round(_) => {
                warn!(target: "peer-session-recv",
                      "ignoring unexpected round message");

                Ok(())
            }
            ConsensusCtl::Submit(submit) => {
                self.send.send(submit.take())
                    .map_err(|err| PeerSessionRecvError::Send { err: err })?;
                self.notify.notify()
                    .map_err(|err| PeerSessionRecvError::IO { err: err })?;

                Ok(())
            }
        }
    }
}

impl<IDTypes, ProtoTypes, Types> PeerSessionDispatch<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    pub(crate) fn new(
        config: LargeObjProtoConfig<(), (), Types::IDsConfig>,
        send: Sender<Vec<Types::HashID>>,
        state_notify: Notify,
        npeers_hint: Option<usize>,
        npending_hint: Option<usize>
    ) -> Self {
        let peers = match npeers_hint {
            Some(size) => Vec::with_capacity(size),
            None => Vec::new()
        };
        let peers = Arc::new(Mutex::new(peers));
        let pending = match npending_hint {
            Some(size) => Vec::with_capacity(size),
            None => Vec::new()
        };
        let pending = Arc::new(Mutex::new(pending));
        let state = PeerSessionState {
            pending: pending,
            peers: peers
        };
        let sessions = Arc::new(Mutex::new(HashMap::new()));

        PeerSessionDispatch {
            state_notify: state_notify,
            sessions: sessions,
            config: config,
            state: state,
            send: send
        }
    }
}

impl<IDTypes, ProtoTypes, Types> SessionDispatchTypes
     for PeerSessionDispatch<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx> + Send,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> + Send,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    type SessionPrin = Types::PeerPrin;
    type Msgs = PeerSessionMsgs<IDTypes, ProtoTypes, Types>;
    type Recv = PeerSessionRecv<IDTypes, ProtoTypes, Types>;
}

impl<IDTypes, ProtoTypes, Types>
    SessionDispatch<PeerSessionDispatch<IDTypes, ProtoTypes, Types>>
    for PeerSessionDispatch<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx> + Send,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> + Send,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    type SessionError = PeerSessionDispatchError<
        Types::PeerPrin,
        Types::CtlCodecCreateError,
        Types::CtlCodecCreateError,
        Types::IDsCreateError
    >;

    fn session(
        &self,
        peer: &Types::PeerPrin,
        _shutdown: ShutdownFlag,
        notify: Notify
    ) -> Result<
        (
            ShutdownFlag,
            PeerSessionMsgs<IDTypes, ProtoTypes, Types>,
            PeerSessionRecv<IDTypes, ProtoTypes, Types>,

        ),
        Self::SessionError
    > {
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| PeerSessionDispatchError::MutexPoison)?;
        let local_shutdown = match sessions.entry(peer.clone()) {
            Entry::Vacant(ent) => {
                let local_shutdown = ShutdownFlag::new();

                debug!(target: "peer-session-dispatch",
                       "creating session for {}",
                       peer);

                ent.insert(PeerSession {
                    local_shutdown: local_shutdown.clone()
                });

                Ok(local_shutdown)
            }
            _ => Err(PeerSessionDispatchError::Exists { prin: peer.clone() })
        }?;
        let recv = PeerSessionRecv {
            id_types: PhantomData,
            proto_types: PhantomData,
            send: self.send.clone(),
            notify: self.state_notify.clone()
        };

        self.state.add_peer(peer.clone(), notify);

        let msgs = PeerSessionMsgs {
            proto_types: PhantomData,
            state: self.state.clone(),
            peer: peer.clone()
        };

        Ok((local_shutdown, msgs, recv))
    }
}

impl<HashID> ScopedError for PeerSessionRecvError<HashID> {
    fn scope(&self) -> ErrorScope {
        match self {
            PeerSessionRecvError::Send { err } => err.scope(),
            PeerSessionRecvError::IO { err } => err.scope(),
        }
    }
}

impl<Prin, Encoder, Decoder, IDs> ScopedError
    for PeerSessionDispatchError<Prin, Encoder, Decoder, IDs>
where
    Encoder: ScopedError,
    Decoder: ScopedError,
    IDs: ScopedError
{
    #[inline]
    fn scope(&self) -> ErrorScope {
        match self {
            PeerSessionDispatchError::Proto { err } => err.scope(),
            PeerSessionDispatchError::Exists { .. } |
            PeerSessionDispatchError::MutexPoison =>
                ErrorScope::Unrecoverable,
        }
    }
}

impl<Encode> ScopedError for PeerSessionAddMsgsError<Encode>
where Encode: ScopedError {
    fn scope(&self) -> ErrorScope {
        match self {
            PeerSessionAddMsgsError::LargeObj { err } => err.scope(),
            PeerSessionAddMsgsError::IO { err } => err.scope(),
        }
    }
}

impl<Prin, Encoder, Decoder, IDs> Display
    for PeerSessionDispatchError<Prin, Encoder, Decoder, IDs>
where
    Prin: Display,
    Encoder: Display,
    Decoder: Display,
    IDs: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PeerSessionDispatchError::Proto { err } => err.fmt(f),
            PeerSessionDispatchError::Exists { prin } => {
                write!(f, "peerr session already exists for {}", prin)
            }
            PeerSessionDispatchError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl<HashID> Display for PeerSessionRecvError<HashID>
where
    HashID: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PeerSessionRecvError::Send { err } => write!(f, "{}", err),
            PeerSessionRecvError::IO { err } => write!(f, "{}", err),
        }
    }
}

impl<Encode> Display for PeerSessionAddMsgsError<Encode>
where
    Encode: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PeerSessionAddMsgsError::LargeObj { err } => err.fmt(f),
            PeerSessionAddMsgsError::IO { err } => write!(f, "{}", err),
        }
    }
}
