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
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::PartyStreamIdx;
use constellation_component_common::bus::dispatch::SessionDispatch;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::frags::Frags;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::large_obj::LargeObjSender;
use log::debug;
use log::trace;
use log::warn;

use crate::state::State;
use crate::types::ConsensusPeerTypes;
use crate::types::ConsensusPeerSessionDispatchTypes;
use crate::types::ConsensusStateTypes;

pub(crate) struct PeerSessionDispatch<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    sessions: Arc<Mutex<HashMap<Types::Prin, PeerSession>>>,
    state: Arc<State<IDTypes, Types>>,
    config: LargeObjProtoConfig<(), (), Types::IDsConfig>
}

pub(crate) struct PeerSessionRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusStateTypes<IDTypes>
        + ConsensusPeerTypes<IDTypes, ProtoTypes> {
    proto_types: PhantomData<ProtoTypes>,
    state: Arc<State<IDTypes, Types>>,
    sessions: Arc<Mutex<HashMap<Types::Prin, PeerSession>>>
}

pub(crate) struct PeerSessionMsgs<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusStateTypes<IDTypes>
        + ConsensusPeerTypes<IDTypes, ProtoTypes> {
    proto_types: PhantomData<ProtoTypes>,
    state: Arc<State<IDTypes, Types>>,
    prin: Types::Prin
}

struct PeerSession {
    local_shutdown: ShutdownFlag
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
pub(crate) enum PeerSessionRecvError<Prin> {
    NotFound { prin: Prin },
    MutexPoison
}

impl<IDTypes, ProtoTypes, Types> Clone
    for PeerSessionRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusStateTypes<IDTypes>
        + ConsensusPeerTypes<IDTypes, ProtoTypes> {
    fn clone(&self) -> Self {
        PeerSessionRecv {
            hash: self.hash,
            sessions: self.sessions.clone(),
            state: self.state.clone()
        }
    }
}

impl<IDTypes, ProtoTypes, Types> Clone
    for PeerSessionMsgs<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusStateTypes<IDTypes>
        + ConsensusPeerTypes<IDTypes, ProtoTypes> {
    fn clone(&self) -> Self {
        PeerSessionMsgs {
            state: self.state.clone(),
            prin: self.prin.clone()
        }
    }
}

impl<IDTypes, ProtoTypes, Types>
    LargeObjMsgs<Types::Hash, ConsensusCtl<IDTypes::RoundID, Types::HashID,
                                           Types::Seal>>
    for PeerSessionMsgs<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusStateTypes<IDTypes>
        + ConsensusPeerTypes<IDTypes, ProtoTypes> {
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<Encode>>
    where
        Encode: Debug + Display + ScopedError;

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
            + Encoder<ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>>,
        WrapperCodec::Config: Default,
        F: Frags {
        // XXX do retry

        if let Some(msg) = self.state.get_round_msg()? {
            let msg = ConsensusCtl::Round(msg);

            sender
                .add_outbound(&msg)
                .map_err(|err| WithMutexPoison::Inner { error: err })?;

            // XXX the protocol should probably be refactored to send
            // batches.
            Ok(Some(Instant::now()))
        } else {
            Ok(None)
        }
    }
}

impl<Prin> ScopedError for PeerSessionRecvError<Prin> {
    fn scope(&self) -> ErrorScope {
        match self {
            PeerSessionRecvError::NotFound { .. } => ErrorScope::Session,
            PeerSessionRecvError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl Drop for PeerSession {
    fn drop(&mut self) {
        trace!(target: "peer-session",
               "signaling local shutdown");

        self.local_shutdown.set()
    }
}

impl<IDTypes, ProtoTypes, Types>
    AuthNMsgRecv<Types::PeerPrin,
                 ConsensusCtl<IDTypes::RoundID, Types::HashID, Types::Seal>,
                 Types::CtlAuthNMsg>
    for PeerSessionRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusStateTypes<IDTypes>
        + ConsensusPeerTypes<IDTypes, ProtoTypes> {
    /// Errors that can occur reporting messages.
    type RecvError = WithMutexPoison<Types::SubmitError>;

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
                let hashes = submit.take();

                self.state.add_hashes(hashes)
            }
        }
    }
}

impl<IDTypes, ProtoTypes, Types> PeerSessionDispatch<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    pub(crate) fn new(
        config: LargeObjProtoConfig<(), (), Types::IDsConfig>,
        state: Arc<State<IDTypes, Types>>
    ) -> Self {
        let sessions = Arc::new(Mutex::new(HashMap::new()));

        PeerSessionDispatch {
            hash: PhantomData,
            ids: PhantomData,
            sessions: sessions,
            config: config,
            state: state
        }
    }
}

impl<IDTypes, ProtoTypes, Types> SessionDispatch<Types::SessionDispTypes>
     for PeerSessionDispatch<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusPeerSessionDispatchTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    type SessionError = PeerSessionDispatchError<
        Types::PeerPrin,
        Types::CtlCodecCreateError,
        Types::CtlCodecCreateError,
        Types::IDsCreateError
    >;

    fn session(
        &self,
        prin: &Types::PeerPrin,
        shutdown: ShutdownFlag,
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
        let local_shutdown = match sessions.entry(prin.clone()) {
            Entry::Vacant(ent) => {
                let local_shutdown = ShutdownFlag::new();

                debug!(target: "peer-session-dispatch",
                       "creating session for {}",
                       prin);

                ent.insert(PeerSession {
                    local_shutdown: local_shutdown.clone()
                });

                Ok(local_shutdown)
            }
            _ => Err(PeerSessionDispatchError::Exists { prin: prin.clone() })
        }?;
        let hash = Types::default();
        let recv = PeerSessionRecv {
            hash: PhantomData,
            sessions: self.sessions.clone(),
            state: self.state.clone()
        };
        let msgs = PeerSessionMsgs {
            state: self.state.clone(),
            prin: prin
        };

        Ok((local_shutdown, msgs, recv))
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
            PeerSessionDispatchError::Proto { err } => err.scope,
            PeerSessionDispatchError::Exists { .. } |
            PeerSessionDispatchError::MutexPoison =>
                ErrorScope::Unrecoverable,
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

impl<Prin> Display for PeerSessionRecvError<Prin>
where
    Prin: Display
{
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            PeerSessionRecvError::NotFound { prin } => {
                write!(f, "no processor session exists for {}", prin)
            }
            PeerSessionRecvError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}
