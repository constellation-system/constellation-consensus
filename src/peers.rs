// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
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
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_auth::authn::PassthruMsgAuthN;
use constellation_common::codec::Codec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::ids::IDGen;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::bus::large_obj::dispatch::SessionDispatch;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::consensus_ctl::ConsensusCtlCodec;
use constellation_consensus_common::oper::OperBatch;
use constellation_consensus_common::round::RoundsAdvance;
use constellation_consensus_common::round::RoundsSubmit;
use constellation_consensus_common::round::RoundsUpdate;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::frags::Frags;
use constellation_streams::frags::OutboundFrags;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::large_obj::LargeObjMsgs;
use constellation_streams::large_obj::LargeObjProto;
use constellation_streams::large_obj::LargeObjProtoAddOutboundError;
use constellation_streams::large_obj::LargeObjProtoCreateError;
use constellation_streams::large_obj::LargeObjSender;
use log::debug;
use log::trace;
use log::warn;

use crate::state::State;

pub(crate) struct PeerSessionDispatch<
    R,
    RoundID,
    H,
    IDs,
    Prin,
    Seal,
    Oper,
    SealCodec
> where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Oper: OperBatch<H> + Send + Sync,
    Prin: Clone + Display + Eq + Hash + Send + Sync {
    hash: PhantomData<H>,
    ids: PhantomData<IDs>,
    sessions: Arc<Mutex<HashMap<Prin, PeerSession>>>,
    state: Arc<State<R, RoundID, H, Seal, Oper>>,
    config: LargeObjProtoConfig<
        <ConsensusCtlCodec<RoundID, H, Seal, SealCodec> as Codec<
            ConsensusCtl<RoundID, H::HashID, Seal>
        >>::Param,
        IDs::Config
    >
}

pub(crate) struct PeerSessionRecv<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Oper: OperBatch<H> + Send + Sync,
    Seal: Clone + Send {
    hash: PhantomData<H>,
    state: Arc<State<R, RoundID, H, Seal, Oper>>,
    sessions: Arc<Mutex<HashMap<Prin, PeerSession>>>
}

pub(crate) struct PeerSessionMsgs<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID + Send + Sync,
    Oper: OperBatch<H> + Send + Sync,
    Seal: Clone + Send {
    state: Arc<State<R, RoundID, H, Seal, Oper>>,
    prin: Prin
}

struct PeerSession {
    local_shutdown: ShutdownFlag
}

#[derive(Debug)]
pub(crate) enum PeerSessionDispatchError<Prin, Codec> {
    Proto {
        err: LargeObjProtoCreateError<Codec>
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

impl<R, RoundID, H, Prin, Seal, Oper> Clone
    for PeerSessionRecv<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Oper: OperBatch<H> + Send + Sync,
    Seal: Clone + Send
{
    fn clone(&self) -> Self {
        PeerSessionRecv {
            hash: self.hash,
            sessions: self.sessions.clone(),
            state: self.state.clone()
        }
    }
}

impl<R, RoundID, H, Prin, Seal, Oper> Clone
    for PeerSessionMsgs<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID + Send + Sync,
    Oper: OperBatch<H> + Send + Sync,
    Seal: Clone + Send
{
    fn clone(&self) -> Self {
        PeerSessionMsgs {
            state: self.state.clone(),
            prin: self.prin.clone()
        }
    }
}

unsafe impl<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec> Send
    for PeerSessionDispatch<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone + Send,
    Oper: OperBatch<H> + Send + Sync,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec> Sync
    for PeerSessionDispatch<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone + Send,
    Oper: OperBatch<H> + Send + Sync,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync
{
}

unsafe impl<R, RoundID, H, Prin, Seal, Oper> Send
    for PeerSessionRecv<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone + Send,
    Oper: OperBatch<H> + Send + Sync
{
}

unsafe impl<R, RoundID, H, Prin, Seal, Oper> Sync
    for PeerSessionRecv<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone + Send,
    Oper: OperBatch<H> + Send + Sync
{
}

impl<R, RoundID, H, Prin, Seal, Oper>
    LargeObjMsgs<H, ConsensusCtl<RoundID, H::HashID, Seal>>
    for PeerSessionMsgs<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    H: Clone + HashAlgo,
    H::HashID: Clone + Display + Eq + Hash + HashID + Send + Sync,
    Seal: Clone + Send,
    Oper: OperBatch<H> + Send + Sync
{
    type AddMsgsError<Encode>
        = WithMutexPoison<LargeObjProtoAddOutboundError<Encode>>
    where
        Encode: Display + ScopedError;

    fn add_msgs<WrapperCodec, F>(
        &mut self,
        sender: &mut LargeObjSender<
            H,
            ConsensusCtl<RoundID, H::HashID, Seal>,
            WrapperCodec,
            F
        >
    ) -> Result<Option<Instant>, Self::AddMsgsError<WrapperCodec::EncodeError>>
    where
        WrapperCodec: Clone + Codec<ConsensusCtl<RoundID, H::HashID, Seal>>,
        WrapperCodec::Param: Default,
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

impl<R, RoundID, Prin, H, Seal, Oper>
    AuthNMsgRecv<Prin, ConsensusCtl<RoundID, H::HashID, Seal>>
    for PeerSessionRecv<R, RoundID, H, Prin, Seal, Oper>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone + Send,
    Oper: OperBatch<H> + Send + Sync
{
    /// Errors that can occur reporting messages.
    type RecvError = WithMutexPoison<R::SubmitError>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        prin: &Prin,
        msg: ConsensusCtl<RoundID, H::HashID, Seal>
    ) -> Result<(), Self::RecvError> {
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

impl<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec>
    PeerSessionDispatch<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Seal: Clone + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Oper: OperBatch<H> + Send + Sync
{
    pub(crate) fn new(
        config: LargeObjProtoConfig<
            <ConsensusCtlCodec<RoundID, H, Seal, SealCodec> as Codec<
                ConsensusCtl<RoundID, H::HashID, Seal>
            >>::Param,
            IDs::Config
        >,
        state: Arc<State<R, RoundID, H, Seal, Oper>>
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

impl<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec>
    SessionDispatch<
        H,
        ConsensusCtl<RoundID, H::HashID, Seal>,
        ConsensusCtl<RoundID, H::HashID, Seal>,
        PassthruMsgAuthN<ConsensusCtl<RoundID, H::HashID, Seal>, Prin>,
        ConsensusCtlCodec<RoundID, H, Seal, SealCodec>,
        IDs,
        PeerSessionMsgs<R, RoundID, H, Prin, Seal, Oper>,
        PeerSessionRecv<R, RoundID, H, Prin, Seal, Oper>,
        Prin
    > for PeerSessionDispatch<R, RoundID, H, IDs, Prin, Seal, Oper, SealCodec>
where
    R: RoundsAdvance<RoundID>
        + RoundsUpdate<Oper>
        + RoundsSubmit<H::HashID>
        + Send,
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Default + HashAlgo + Send,
    H::HashID: Clone + Display + Hash + HashID + Eq + Send + Sync,
    IDs: IDGen + Iterator<Item = LargeObjID> + Send,
    IDs::Config: Clone,
    Prin: Clone + Display + Eq + Hash + Send + Sync,
    Seal: Clone + Send,
    SealCodec: Clone + Codec<Seal>,
    SealCodec::Param: Clone + Default,
    Oper: OperBatch<H> + Send + Sync
{
    type SessionError = PeerSessionDispatchError<
        Prin,
        <ConsensusCtlCodec<RoundID, H, Seal, SealCodec> as Codec<
            ConsensusCtl<RoundID, H::HashID, Seal>
        >>::CreateError
    >;

    fn session(
        &self,
        prin: Prin
    ) -> Result<
        (
            ShutdownFlag,
            Notify,
            LargeObjProto<
                H,
                ConsensusCtl<RoundID, H::HashID, Seal>,
                ConsensusCtl<RoundID, H::HashID, Seal>,
                PassthruMsgAuthN<ConsensusCtl<RoundID, H::HashID, Seal>, Prin>,
                (),
                ConsensusCtlCodec<RoundID, H, Seal, SealCodec>,
                IDs,
                PeerSessionMsgs<R, RoundID, H, Prin, Seal, Oper>,
                PeerSessionRecv<R, RoundID, H, Prin, Seal, Oper>,
                OutboundFrags
            >
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
        let hash = H::default();
        let recv = PeerSessionRecv {
            hash: PhantomData,
            sessions: self.sessions.clone(),
            state: self.state.clone()
        };
        let msgs = PeerSessionMsgs {
            state: self.state.clone(),
            prin: prin
        };
        let authn = PassthruMsgAuthN::default();
        let proto = LargeObjProto::create(
            self.config.clone(),
            self.state.notify(),
            recv,
            msgs,
            authn,
            hash
        )
        .map_err(|err| PeerSessionDispatchError::Proto { err: err })?;

        Ok((local_shutdown, self.state.notify(), proto))
    }
}

impl<Prin, Codec> Display for PeerSessionDispatchError<Prin, Codec>
where
    Prin: Display,
    Codec: Display
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
