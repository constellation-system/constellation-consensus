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

#[cfg(feature = "standalone")]
use std::convert::Infallible;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::path::PathBuf;
use std::thread::JoinHandle;

#[cfg(feature = "standalone")]
use clap::ArgMatches;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_common::config::Create;
use constellation_common::error::WithMutexPoison;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::bus::dispatch::DispatchLargeObjBus;
use constellation_component_common::bus::dispatch::DispatchLargeObjBusStarter;
use constellation_component_common::bus::multicast::MulticastDatagramBus;
use constellation_component_common::bus::multicast::MulticastDatagramBusStarter;
use constellation_consensus_common::proto::ConsensusProto;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
#[cfg(feature = "standalone")]
use constellation_standalone::StandaloneService;
use constellation_streams::multicast::MulticastStreamIdx;
use constellation_streams::threads::poll::PollThreadStartError;
use log::debug;
use log::error;
use log::info;

use crate::config::ConsensusComponentConfig;
#[cfg(feature = "standalone")]
use crate::config::StandaloneConfig;
use crate::peers::PeerSessionDispatch;
use crate::peers::PeerSessionState;
use crate::recv::ConsensusAuthNRecv;
use crate::state::State;
use crate::state::StateInitRoundsError;
use crate::state::StateThread;
use crate::types::ConsensusTypes;

/// Index used to identify parties in a given round.
///
/// These are generally a subset of the total set of parties, and the
/// indexing scheme may change from round to round.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartyRoundIdx(usize);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StringPrincipalCodec;

pub struct ConsensusComponent<Types>
where
    Types: 'static + ConsensusTypes + Send {
    state: State<Types::IDTypes, Types::ProtoTypes, Types>,
    state_thread: StateThread<Types::IDTypes, Types::ProtoTypes, Types>,
    consensus:
        MulticastDatagramBusStarter<Types::ConsensusBusTypes, Types::Ctx>,
    consensus_recv:
        ConsensusAuthNRecv<Types::IDTypes, Types::ProtoTypes, Types>,
    peers: DispatchLargeObjBusStarter<Types::PeerBusTypes, Types::Ctx>,
    peers_dispatch:
        PeerSessionDispatch<Types::IDTypes, Types::ProtoTypes, Types>
}

pub struct ConsensusComponentCleanup<Types>
where
    Types: 'static + ConsensusTypes + Send {
    shutdown: ShutdownFlag,
    consensus_join: MulticastDatagramBus<Types::ConsensusBusTypes, Types::Ctx>,
    peers_join: DispatchLargeObjBus<Types::PeerBusTypes, Types::Ctx>,
    state_join: JoinHandle<()>
}

pub struct ConsensusComponentErrorCleanup<Types>
where
    Types: 'static + ConsensusTypes + Send {
    shutdown: ShutdownFlag,
    peers_join: Option<DispatchLargeObjBus<Types::PeerBusTypes, Types::Ctx>>,
    state_join: Option<JoinHandle<()>>
}

pub enum ConsensusComponentCreateError<
    RoundIDs,
    Hash,
    Consensus,
    Rounds,
    Parties
> {
    RoundIDs { err: RoundIDs },
    Hash { err: Hash },
    Consensus { err: Consensus },
    Rounds { err: Rounds },
    Parties { err: Parties },
    IO { err: std::io::Error },
    MutexPoison
}

pub enum ConsensusComponentRunError {
    Poll { err: PollThreadStartError },
    IO { err: std::io::Error },
    MutexPoison
}

impl From<usize> for PartyRoundIdx {
    #[inline]
    fn from(val: usize) -> PartyRoundIdx {
        PartyRoundIdx(val)
    }
}

impl From<PartyRoundIdx> for usize {
    #[inline]
    fn from(val: PartyRoundIdx) -> usize {
        val.0
    }
}

impl<Types> ConsensusComponent<Types>
where
    Types: ConsensusTypes + Send
{
    pub fn create(
        config: ConsensusComponentConfig<
            Types::RoundIDsConfig,
            Types::ConsensusConfig,
            Types::Prin,
            Types::HashConfig,
            Types::ConsensusSessAuthConfig,
            Types::ConsensusUnixConfig,
            Types::ConsensusUDPConfig,
            Types::ConsensusEncoderConfig,
            Types::ConsensusDecoderConfig,
            Types::ResolverConfig,
            Types::ConsensusEpochsConfig,
            Types::ConsensusMsgAuthConfig,
            Types::PeerHashConfig,
            Types::PeerSessAuthConfig,
            Types::PeerMsgAuthConfig,
            Types::PeerProtoAuthConfig,
            Types::PeerUnixConfig,
            Types::PeerUDPConfig,
            Types::IDsConfig,
            Types::PeerEpochsConfig
        >,
        ctx: Types::Ctx,
        shutdown: &ShutdownFlag
    ) -> Result<
        Self,
        ConsensusComponentCreateError<
            Types::RoundIDsCreateError,
            Types::HashCreateError,
            Types::ConsensusCreateError,
            Types::ConsensusRoundsError,
            StateInitRoundsError<Types::SetPartiesError, Types::AdvanceError>
        >
    > {
        info!(target: "consensus-component",
              "starting consensus component");

        let (
            round_ids_config,
            hash_config,
            proto_config,
            consensus_config,
            peers_config
        ) = config.take();
        // XXX Hacky and wrong way to set parties
        let self_party = consensus_config.self_party().clone();
        let parties: Vec<(MulticastStreamIdx, Types::Prin)> = consensus_config
            .thread()
            .stream()
            .parties()
            .iter()
            .filter(|party| party.party() != &self_party)
            .enumerate()
            .map(|(i, party)| {
                (MulticastStreamIdx::from(i), party.party().clone())
            })
            .collect();
        let round_ids =
            Types::RoundIDs::create(round_ids_config).map_err(|err| {
                ConsensusComponentCreateError::RoundIDs { err: err }
            })?;
        let hash = Types::Hash::create(hash_config)
            .map_err(|err| ConsensusComponentCreateError::Hash { err: err })?;
        let proto = Types::Consensus::create(proto_config).map_err(|err| {
            ConsensusComponentCreateError::Consensus { err: err }
        })?;
        let rounds = proto.rounds(round_ids).map_err(|err| {
            ConsensusComponentCreateError::Rounds { err: err }
        })?;
        let (peers, peers_notify) =
            DispatchLargeObjBusStarter::create(peers_config, ctx.clone())
                .map_err(|err| ConsensusComponentCreateError::IO {
                    err: err
                })?;
        let (consensus, consensus_notify) =
            MulticastDatagramBusStarter::create(consensus_config, ctx.clone())
                .map_err(|err| ConsensusComponentCreateError::IO {
                    err: err
                })?;
        let peer_state = PeerSessionState::new();
        let state = State::new(rounds);
        let (state_thread, peers_sender, consensus_sender, state_notify) =
            StateThread::new(
                state.clone(),
                peer_state,
                peers_notify,
                consensus_notify,
                shutdown.clone(),
                hash
            )
            .map_err(|err| match err {
                WithMutexPoison::Inner { err } => {
                    ConsensusComponentCreateError::IO { err: err }
                }
                WithMutexPoison::MutexPoison => {
                    ConsensusComponentCreateError::MutexPoison
                }
            })?;
        // XXX use size hints.
        let peers_dispatch = PeerSessionDispatch::new(
            peers_sender,
            state_notify.clone(),
            None,
            None
        );
        let mut consensus_recv =
            ConsensusAuthNRecv::create(consensus_sender, state_notify.clone());

        state
            .init_rounds(self_party, parties.iter().map(|(_, id)| id))
            .map_err(|err| ConsensusComponentCreateError::Parties {
                err: err
            })?;
        consensus_recv
            .set_parties(parties.into_iter())
            .map_err(|_| ConsensusComponentCreateError::MutexPoison)?;

        Ok(ConsensusComponent {
            state: state,
            state_thread: state_thread,
            consensus: consensus,
            consensus_recv: consensus_recv,
            peers_dispatch: peers_dispatch,
            peers: peers
        })
    }

    pub fn start(
        self,
        shutdown: ShutdownFlag
    ) -> Result<
        ConsensusComponentCleanup<Types>,
        (
            ConsensusComponentErrorCleanup<Types>,
            ConsensusComponentRunError
        )
    > {
        info!(target: "consensus-component",
              "starting consensus component");

        let ConsensusComponent {
            state_thread,
            state,
            consensus,
            consensus_recv,
            peers,
            peers_dispatch
        } = self;

        match state_thread.start() {
            Ok(state_join) => match peers
                .start(peers_dispatch, shutdown.clone())
            {
                Ok(peers_join) => match consensus.start(
                    consensus_recv,
                    state,
                    shutdown.clone()
                ) {
                    Ok(consensus_join) => Ok(ConsensusComponentCleanup {
                        shutdown: shutdown,
                        consensus_join: consensus_join,
                        peers_join: peers_join,
                        state_join: state_join
                    }),
                    Err(err) => {
                        let cleanup = ConsensusComponentErrorCleanup {
                            shutdown: shutdown.clone(),
                            peers_join: Some(peers_join),
                            state_join: Some(state_join)
                        };
                        let err = ConsensusComponentRunError::Poll { err: err };

                        Err((cleanup, err))
                    }
                },
                Err(err) => {
                    let cleanup = ConsensusComponentErrorCleanup {
                        shutdown: shutdown.clone(),
                        peers_join: None,
                        state_join: Some(state_join)
                    };
                    let err = match err {
                        WithMutexPoison::Inner { err } => {
                            ConsensusComponentRunError::IO { err: err }
                        }
                        WithMutexPoison::MutexPoison => {
                            ConsensusComponentRunError::MutexPoison
                        }
                    };

                    Err((cleanup, err))
                }
            },
            Err(err) => {
                let cleanup = ConsensusComponentErrorCleanup {
                    shutdown: shutdown.clone(),
                    peers_join: None,
                    state_join: None
                };

                Err((cleanup, ConsensusComponentRunError::IO { err: err }))
            }
        }
    }
}

impl<Types> ConsensusComponentCleanup<Types>
where
    Types: ConsensusTypes + Send
{
    pub fn cleanup(mut self) {
        if let Err(err) = self.shutdown.set() {
            error!(target: "consensus-component-cleanup",
                   "error setting shutdown flag: {}",
                   err);
        }

        self.consensus_join.cleanup();
        self.peers_join.cleanup();

        debug!(target: "consensus-component-cleanup",
               "joining state thread");

        if self.state_join.join().is_err() {
            error!(target: "consensus-component-cleanup",
                   "error joining state thread")
        }

        debug!(target: "consensus-component-cleanup",
               "all threads joined");
    }
}

impl<RoundIDs, Hash, Consensus, Rounds, Parties> Display
    for ConsensusComponentCreateError<
        RoundIDs,
        Hash,
        Consensus,
        Rounds,
        Parties
    >
where
    RoundIDs: Display,
    Parties: Display,
    Hash: Display,
    Consensus: Display,
    Rounds: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ConsensusComponentCreateError::RoundIDs { err } => err.fmt(f),
            ConsensusComponentCreateError::Parties { err } => err.fmt(f),
            ConsensusComponentCreateError::Hash { err } => err.fmt(f),
            ConsensusComponentCreateError::Consensus { err } => err.fmt(f),
            ConsensusComponentCreateError::Rounds { err } => {
                write!(f, "{}", err)
            }
            ConsensusComponentCreateError::IO { err } => write!(f, "{}", err),
            ConsensusComponentCreateError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

impl Display for ConsensusComponentRunError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ConsensusComponentRunError::Poll { err } => write!(f, "{}", err),
            ConsensusComponentRunError::IO { err } => write!(f, "{}", err),
            ConsensusComponentRunError::MutexPoison => {
                write!(f, "mutex poisoned")
            }
        }
    }
}

#[cfg(feature = "standalone")]
pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

#[cfg(feature = "standalone")]
pub struct StandaloneCreateErrorCleanup {
    caches: Option<StandaloneCreateCleanup>
}

#[cfg(feature = "standalone")]
#[derive(Clone)]
pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches
}

#[cfg(feature = "standalone")]
impl NSNameCachesCtx for StandaloneCtx {
    /// Exact type of name caches.
    type NameCaches = ThreadedNSNameCaches;

    #[inline]
    fn name_caches(&mut self) -> &mut Self::NameCaches {
        &mut self.caches
    }
}

#[cfg(feature = "standalone")]
impl<Types> Standalone for ConsensusComponent<Types>
where
    Types: ConsensusTypes<Prin = String, Ctx = StandaloneCtx> + Send
{
    type Config = StandaloneConfig<
        Types::RoundIDsConfig,
        Types::ConsensusConfig,
        Types::HashConfig,
        Types::ConsensusSessAuthConfig,
        Types::ConsensusUnixConfig,
        Types::ConsensusUDPConfig,
        Types::ConsensusEncoderConfig,
        Types::ConsensusDecoderConfig,
        Types::ResolverConfig,
        Types::ConsensusEpochsConfig,
        Types::ConsensusMsgAuthConfig,
        Types::PeerHashConfig,
        Types::PeerSessAuthConfig,
        Types::PeerMsgAuthConfig,
        Types::PeerProtoAuthConfig,
        Types::PeerUnixConfig,
        Types::PeerUDPConfig,
        Types::IDsConfig,
        Types::PeerEpochsConfig
    >;
    type CreateCleanup = StandaloneCreateCleanup;
    type CreateErrorCleanup = StandaloneCreateErrorCleanup;

    const CONFIG_FILES: &[&str] = &["consensus.conf"];
    const NAME: &str = "consensus";
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn cleanup_create_err(create: Self::CreateErrorCleanup) {
        if let Some(cleanup) = create.caches {
            let StandaloneCreateCleanup {
                caches_join,
                mut shutdown
            } = cleanup;

            debug!(target: "processor",
                   "cleaning up caches");

            if let Err(err) = shutdown.set() {
                error!(target: "one-shot",
                       "error setting shutdown flag: {}",
                       err)
            } else if caches_join.join().is_err() {
                error!(target: "one-shot",
                       "error shutting down name chache threads")
            }
        }
    }

    fn create(
        _args: ArgMatches,
        config: Self::Config,
        shutdown: &ShutdownFlag
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateErrorCleanup> {
        let (name_caches_config, component_config) = config.take();
        let (caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config, shutdown.clone())
                .map_err(|err| {
                    error!(target: "processor-standalone",
                       "error creating name caches: {}",
                       err);

                    StandaloneCreateErrorCleanup { caches: None }
                })?;
        let ctx = StandaloneCtx { caches: caches };
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };

        match ConsensusComponent::create(component_config, ctx, shutdown) {
            Ok(out) => Ok((out, cleanup)),
            Err(err) => {
                error!(target: "consensus-standalone",
                       "error creating component: {}",
                       err);

                Err(StandaloneCreateErrorCleanup {
                    caches: Some(cleanup)
                })
            }
        }
    }
}

#[cfg(feature = "standalone")]
impl<Types> StandaloneService for ConsensusComponent<Types>
where
    Types: ConsensusTypes<Prin = String, Ctx = StandaloneCtx> + Send
{
    type RunCleanup = ConsensusComponentCleanup<Types>;
    type RunErrorCleanup = ConsensusComponentErrorCleanup<Types>;

    const DEFAULT_PIDFILE: &str = "/tmp/consensus.pid";

    fn run(
        self,
        _rundir: PathBuf,
        shutdown: ShutdownFlag
    ) -> Result<Self::RunCleanup, Self::RunErrorCleanup> {
        self.start(shutdown).map_err(|(cleanup, err)| {
            error!(target: "consensus-standalone",
                       "error running component: {}",
                       err);

            cleanup
        })
    }

    fn join(
        create_cleanup: Self::CreateCleanup,
        run_cleanup: Self::RunCleanup
    ) {
        let ConsensusComponentCleanup {
            consensus_join,
            peers_join,
            state_join,
            ..
        } = run_cleanup;

        consensus_join.cleanup();
        peers_join.cleanup();

        if state_join.join().is_err() {
            error!(target: "consensus-standalone-shutdown",
                   "error joining peers thread")
        }

        if create_cleanup.caches_join.join().is_err() {
            error!(target: "consensus-standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }

    fn shutdown_err(
        mut create_cleanup: Self::CreateCleanup,
        run_cleanup: Self::RunErrorCleanup
    ) {
        debug!(target: "consensus-standalone",
               "cleaning up consensus");

        let ConsensusComponentErrorCleanup {
            mut shutdown,
            peers_join,
            state_join
        } = run_cleanup;

        if let Err(err) = shutdown.set() {
            error!(target: "consensus-standalone-shutdown",
                   "error setting shutdown flag: {}",
                   err)
        } else {
            if let Some(peers) = peers_join {
                peers.cleanup()
            }

            if let Some(state) = state_join &&
                state.join().is_err()
            {
                error!(target: "consensus-standalone-shutdown",
                           "error joining peers thread")
            }
        }

        if let Err(err) = create_cleanup.shutdown.set() {
            error!(target: "consensus-standalone-shutdown",
                   "error setting shutdown flag: {}",
                   err)
        } else if create_cleanup.caches_join.join().is_err() {
            error!(target: "consensus-standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }
}

#[cfg(feature = "standalone")]
use std::net::SocketAddr;

#[cfg(feature = "standalone")]
use constellation_auth::authn::BasicAuthNed;
#[cfg(feature = "standalone")]
use constellation_auth::authn::PassthruMsgAuthN;
#[cfg(feature = "standalone")]
use constellation_auth::authn::basic::BasicAuthN;
#[cfg(feature = "standalone")]
use constellation_auth::config::BasicAuthNConfig;
#[cfg(feature = "standalone")]
use constellation_channels::config::CompoundFarChannelXfrmPeerAddr;
#[cfg(feature = "standalone")]
use constellation_channels::config::CompoundFarEndpoint;
#[cfg(feature = "standalone")]
use constellation_channels::config::ResolverConfig;
#[cfg(feature = "standalone")]
use constellation_channels::far::compound::CompoundFlow;
#[cfg(feature = "standalone")]
use constellation_channels::far::types::CompoundFarChannelsBasicAuthNedDatagramChan;
#[cfg(feature = "standalone")]
use constellation_channels::far::types::CompoundFarChannelsBasicAuthNedLargeObjChan;
#[cfg(feature = "standalone")]
use constellation_channels::resolve::MixedResolver;
#[cfg(feature = "standalone")]
use constellation_common::codec::test::TestDecodeError;
#[cfg(feature = "standalone")]
use constellation_common::codec::test::TestStringCodec;
#[cfg(feature = "standalone")]
use constellation_common::codec::test::TooShort;
#[cfg(feature = "standalone")]
use constellation_common::hashid::SHA3Algo;
#[cfg(feature = "standalone")]
use constellation_common::hashid::SHA3ID;
#[cfg(feature = "standalone")]
use constellation_common::ids::AscendingCount;
#[cfg(feature = "standalone")]
use constellation_common::net::PassthruDatagramXfrm;
#[cfg(feature = "standalone")]
use constellation_common::net::PassthruDatagramXfrmParam;
#[cfg(feature = "standalone")]
use constellation_common::unix::UnixSocketPath;
#[cfg(feature = "standalone")]
use constellation_component_common::bus::types::DispatchLargeObjBusTypes;
#[cfg(feature = "standalone")]
use constellation_component_common::bus::types::MulticastDatagramBusTypes;
#[cfg(feature = "standalone")]
use constellation_component_common::consensus_ctl::ConsensusCtl;
#[cfg(feature = "standalone")]
use constellation_component_common::consensus_ctl::ConsensusCtlCodec;
#[cfg(feature = "standalone")]
use constellation_component_common::consensus_ctl::ConsensusCtlRoundEncodeError;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::test::TestSeal;
#[cfg(feature = "standalone")]
use constellation_component_common::xact::test::TestSealCodec;
#[cfg(feature = "standalone")]
use constellation_consensus_common::parties::PartyTypes;
#[cfg(feature = "standalone")]
use constellation_consensus_common::parties::RoundIDGenTypes;
#[cfg(feature = "standalone")]
use constellation_consensus_common::parties::RoundPartyIDTypes;
#[cfg(feature = "standalone")]
use constellation_consensus_common::parties::RoundPartyIdxTypes;
#[cfg(feature = "standalone")]
use constellation_consensus_common::round::SingleRound;
#[cfg(feature = "standalone")]
use constellation_consensus_common::round::SingleRoundAdvanceError;
#[cfg(feature = "standalone")]
use constellation_consensus_common::round::SingleRoundCreateError;
#[cfg(feature = "standalone")]
use constellation_consensus_common::round::SingleRoundSubmitError;
#[cfg(feature = "standalone")]
use constellation_pbft::config::PBFTConfig;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PBFTMsgPERCodec;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PbftMsg;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PbftRequest;
#[cfg(feature = "standalone")]
use constellation_pbft::proto::OutboundPartyIdx;
#[cfg(feature = "standalone")]
use constellation_pbft::proto::PBFTProto;
#[cfg(feature = "standalone")]
use constellation_pbft::state::PBFTBatchError;
#[cfg(feature = "standalone")]
use constellation_pbft::state::PBFTProtoState;
#[cfg(feature = "standalone")]
use constellation_pbft::state::PBFTProtoStateUpdateError;
#[cfg(feature = "standalone")]
use constellation_pbft::state::PBFTProtoTypes;
#[cfg(feature = "standalone")]
use constellation_pbft::state::PBFTRoundResult;
#[cfg(feature = "standalone")]
use constellation_pbft::state::PBFTRoundStateCreateError;
#[cfg(feature = "standalone")]
use constellation_streams::large_obj::LargeObjID;
#[cfg(feature = "standalone")]
use constellation_streams::large_obj::LargeObjMsg;

#[cfg(feature = "standalone")]
use crate::peers::PeerSessionDispatchTypes;
#[cfg(feature = "standalone")]
use crate::peers::PeerSessionMsgs;
#[cfg(feature = "standalone")]
use crate::peers::PeerSessionRecv;
#[cfg(feature = "standalone")]
use crate::peers::PeerSessionRecvError;
#[cfg(feature = "standalone")]
use crate::types::ConsensusBaseTypes;
#[cfg(feature = "standalone")]
use crate::types::ConsensusMsgTypes;
#[cfg(feature = "standalone")]
use crate::types::ConsensusPeerTypes;
#[cfg(feature = "standalone")]
use crate::types::ConsensusRecvTypes;

#[cfg(feature = "standalone")]
pub struct StandaloneConsensusTypes;

#[cfg(feature = "standalone")]
pub struct StandaloneConsensusBusTypes;

#[cfg(feature = "standalone")]
pub struct StandalonePeerBusTypes;

#[cfg(feature = "standalone")]
pub struct StandalonePBFTIDTypes;

#[cfg(feature = "standalone")]
pub struct StandalonePBFTTypes;

#[cfg(feature = "standalone")]
pub type StandaloneConsensusComponent =
    ConsensusComponent<StandaloneConsensusTypes>;

#[cfg(feature = "standalone")]
impl RoundPartyIDTypes for StandalonePBFTIDTypes {
    type PartyID = MulticastStreamIdx;
    type RoundID = u128;
}

#[cfg(feature = "standalone")]
impl RoundPartyIdxTypes for StandalonePBFTIDTypes {
    type PartyRoundIdx = OutboundPartyIdx;
}

#[cfg(feature = "standalone")]
impl RoundIDGenTypes for StandalonePBFTIDTypes {
    type RoundIDs = AscendingCount<u128>;
}

#[cfg(feature = "standalone")]
impl PartyTypes for StandalonePBFTIDTypes {
    type DecodeError = TestDecodeError;
    type EncodeError = TooShort;
    type Party = String;
    type PartyCodec = TestStringCodec;
    type PartyCodecConfig = ();
    type PartyCodecCreateError = Infallible;
}

#[cfg(feature = "standalone")]
impl ConsensusBaseTypes for StandaloneConsensusTypes {
    type BatchError = PBFTBatchError;
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type Oper = PBFTRoundResult<PbftRequest>;
    type Seal = TestSeal;
    type SealCodec = TestSealCodec;
    type SealCodecCreateError = Infallible;
    type SealCodecParam = ();
}

#[cfg(feature = "standalone")]
impl ConsensusMsgTypes<StandalonePBFTIDTypes, PBFTProtoTypes>
    for StandaloneConsensusTypes
{
    type AuthNMsg = BasicAuthNed<String, PbftMsg>;
}

#[cfg(feature = "standalone")]
impl ConsensusRecvTypes<StandalonePBFTIDTypes, PBFTProtoTypes>
    for StandaloneConsensusTypes
{
    type AdvanceError =
        SingleRoundAdvanceError<PBFTRoundStateCreateError<MulticastStreamIdx>>;
    type Rounds = SingleRound<
        PBFTProtoState<SHA3Algo, MulticastStreamIdx>,
        StandalonePBFTIDTypes,
        PBFTProtoTypes
    >;
    type SetPartiesError = TooShort;
    type SubmitError = SingleRoundSubmitError<Infallible, Infallible>;
    type UpdateError = PBFTProtoStateUpdateError<SHA3ID>;
}

#[cfg(feature = "standalone")]
impl ConsensusPeerTypes<StandalonePBFTIDTypes, PBFTProtoTypes>
    for StandaloneConsensusTypes
{
    type CtlAuthNMsg =
        BasicAuthNed<String, ConsensusCtl<u128, SHA3ID, TestSeal>>;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type PeerPrin = String;
}

#[cfg(feature = "standalone")]
impl ConsensusTypes for StandaloneConsensusTypes {
    type Consensus = PBFTProto<SHA3Algo, StandalonePBFTIDTypes>;
    type ConsensusBusTypes = StandaloneConsensusBusTypes;
    type ConsensusConfig = PBFTConfig<()>;
    type ConsensusCreateError = Infallible;
    type ConsensusDecoderConfig = ();
    type ConsensusEncoderConfig = ();
    type ConsensusEpochsConfig = ();
    type ConsensusMsgAuthConfig = ();
    type ConsensusRoundsError = SingleRoundCreateError<
        Infallible,
        Infallible,
        PBFTRoundStateCreateError<MulticastStreamIdx>
    >;
    type ConsensusSessAuthConfig = BasicAuthNConfig<String>;
    type ConsensusUDPConfig = PassthruDatagramXfrmParam;
    type ConsensusUnixConfig = PassthruDatagramXfrmParam;
    type Ctx = StandaloneCtx;
    type IDTypes = StandalonePBFTIDTypes;
    type Msg = PbftMsg;
    type PeerBusTypes = StandalonePeerBusTypes;
    type PeerEpochsConfig = ();
    type PeerHashConfig = ();
    type PeerMsgAuthConfig = ();
    type PeerProtoAuthConfig = ();
    type PeerSessAuthConfig = BasicAuthNConfig<String>;
    type PeerUDPConfig = PassthruDatagramXfrmParam;
    type PeerUnixConfig = PassthruDatagramXfrmParam;
    type Prin = String;
    type ProtoTypes = PBFTProtoTypes;
    type ResolverConfig = ResolverConfig;
    type RoundID = u128;
    type RoundIDs = AscendingCount<u128>;
    type RoundIDsConfig = ();
    type RoundIDsCreateError = Infallible;
}

#[cfg(feature = "standalone")]
impl DispatchLargeObjBusTypes<StandaloneCtx> for StandalonePeerBusTypes {
    type AuthNChan = CompoundFarChannelsBasicAuthNedLargeObjChan<
        String,
        SHA3Algo,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>
    >;
    type AuthNMsg = BasicAuthNed<String, ConsensusCtl<u128, SHA3ID, TestSeal>>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = ConsensusCtlCodec<u128, SHA3Algo, TestSeal, TestSealCodec>;
    type DecoderConfig = ();
    type DecoderCreateError = Infallible;
    type DispTypes = PeerSessionDispatchTypes<
        StandalonePBFTIDTypes,
        PBFTProtoTypes,
        StandaloneConsensusTypes
    >;
    type EncodeError =
        ConsensusCtlRoundEncodeError<asn1rs::io::per::err::Error, Infallible>;
    type Encoder = ConsensusCtlCodec<u128, SHA3Algo, TestSeal, TestSealCodec>;
    type EncoderConfig = ();
    type EncoderCreateError = Infallible;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type EpochsCreateError = Infallible;
    type Hash = SHA3Algo;
    type HashConfig = ();
    type HashCreateError = Infallible;
    type HashID = SHA3ID;
    type IDs = AscendingCount<LargeObjID>;
    type IDsConfig = ();
    type IDsCreateError = Infallible;
    type InMsg = ConsensusCtl<u128, SHA3ID, TestSeal>;
    type LargeObjAuthNMsg = BasicAuthNed<String, LargeObjMsg<SHA3ID>>;
    type LargeObjMsgAuth = PassthruMsgAuthN<LargeObjMsg<SHA3ID>, String>;
    type LargeObjMsgAuthConfig = ();
    type LargeObjMsgAuthCreateError = Infallible;
    type LargeObjWrapper = LargeObjMsg<SHA3ID>;
    type MsgAuth =
        PassthruMsgAuthN<ConsensusCtl<u128, SHA3ID, TestSeal>, String>;
    type MsgAuthConfig = ();
    type MsgAuthCreateError = Infallible;
    type MsgAuthError = Infallible;
    type MsgPrin = String;
    type Msgs = PeerSessionMsgs<
        StandalonePBFTIDTypes,
        PBFTProtoTypes,
        StandaloneConsensusTypes
    >;
    type Origin = CompoundFarEndpoint;
    type OutMsg = ConsensusCtl<u128, SHA3ID, TestSeal>;
    type Recv = PeerSessionRecv<
        StandalonePBFTIDTypes,
        PBFTProtoTypes,
        StandaloneConsensusTypes
    >;
    type RecvError = PeerSessionRecvError<SHA3ID>;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = ConsensusCtl<u128, SHA3ID, TestSeal>;
}

#[cfg(feature = "standalone")]
impl MulticastDatagramBusTypes<StandaloneCtx> for StandaloneConsensusBusTypes {
    type AuthNChan = CompoundFarChannelsBasicAuthNedDatagramChan<
        String,
        PbftMsg,
        PbftMsg,
        PassthruDatagramXfrm<UnixSocketPath>,
        PassthruDatagramXfrm<SocketAddr>,
        PBFTMsgPERCodec,
        PBFTMsgPERCodec
    >;
    type AuthNMsg = BasicAuthNed<String, PbftMsg>;
    type AuthNSession = BasicAuthNed<
        String,
        CompoundFlow<
            PassthruDatagramXfrm<UnixSocketPath>,
            PassthruDatagramXfrm<SocketAddr>
        >
    >;
    type Decoder = PBFTMsgPERCodec;
    type DecoderConfig = ();
    type DecoderCreateError = Infallible;
    type EncodeError = asn1rs::io::per::err::Error;
    type Encoder = PBFTMsgPERCodec;
    type EncoderConfig = ();
    type EncoderCreateError = Infallible;
    type Epoch = u128;
    type Epochs = AscendingCount<u128>;
    type EpochsConfig = ();
    type InMsg = PbftMsg;
    type MsgAuth = PassthruMsgAuthN<PbftMsg, String>;
    type MsgAuthConfig = ();
    type MsgPrin = String;
    type Msgs =
        State<StandalonePBFTIDTypes, PBFTProtoTypes, StandaloneConsensusTypes>;
    type Origin = CompoundFarEndpoint;
    type OutMsg = PbftMsg;
    type Recv = ConsensusAuthNRecv<
        StandalonePBFTIDTypes,
        PBFTProtoTypes,
        StandaloneConsensusTypes
    >;
    type Resolver =
        MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundFarEndpoint>;
    type ResolverConfig = ResolverConfig;
    type SessionAuth = BasicAuthN<String>;
    type SessionAuthConfig = BasicAuthNConfig<String>;
    type SessionAuthNError = Infallible;
    type SessionPrin = String;
    type UDP = PassthruDatagramXfrm<SocketAddr>;
    type UDPConfig = PassthruDatagramXfrmParam;
    type UDPError = Infallible;
    type Unix = PassthruDatagramXfrm<UnixSocketPath>;
    type UnixConfig = PassthruDatagramXfrmParam;
    type UnixError = Infallible;
    type Wrapper = PbftMsg;
}
