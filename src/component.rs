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

use std::convert::Infallible;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::Arc;
use std::thread::JoinHandle;

#[cfg(feature = "standalone")]
use clap::ArgMatches;
use constellation_auth::authn::SessionAuthN;
use constellation_auth::config::TestCredConfig;
use constellation_auth::cred::SSLCred;
use constellation_channels::config::CompoundFarEndpoint;
use constellation_channels::config::ResolverConfig;
use constellation_channels::far::compound::CompoundFarChannel;
use constellation_channels::far::compound::CompoundFarChannelSessionCred;
use constellation_channels::far::compound::CompoundFarChannelXfrm;
use constellation_channels::far::udp::UDPDatagramXfrm;
use constellation_channels::far::unix::UnixDatagramXfrm;
use constellation_channels::far::FarChannelAcquiredResolve;
use constellation_channels::far::FarChannelCreate;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_channels::resolve::MixedResolver;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::codec::DatagramCodec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
use constellation_common::error::MutexPoison;
use constellation_common::error::WithMutexPoison;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::SHA3Algo;
use constellation_common::ids::AscendingCount;
use constellation_common::net::DatagramXfrm;
use constellation_common::net::DatagramXfrmCreate;
use constellation_common::net::IPEndpointAddr;
use constellation_common::net::SharedMsgs;
use constellation_common::net::Socket;
use constellation_common::sched::DenseItemID;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::version::FullVersion;
use constellation_common::version::Version;
use constellation_common::version::VersionSuffix;
use constellation_component_common::config::PartiesConfig;
use constellation_component_common::config::DispatchLargeObjBusConfig;
use constellation_component_common::config::MulticastDatagramBusConfig;
use constellation_component_common::bus::dispatch::DispatchLargeObjBus;
use constellation_component_common::bus::dispatch::DispatchLargeObjBusStarter;
use constellation_component_common::bus::multicast::MulticastDatagramBus;
use constellation_component_common::bus::multicast::MulticastDatagramBusStarter;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::consensus_ctl::ConsensusCtlCodec;
use constellation_consensus_common::oper::OperBatch;
use constellation_consensus_common::parties::StaticParties;
use constellation_consensus_common::proto::ConsensusProto;
use constellation_consensus_common::proto::SharedConsensusProto;
use constellation_consensus_common::round::RoundsSubmit;
use constellation_consensus_common::state::ProtoState;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PBFTMsgPERCodec;
#[cfg(feature = "standalone")]
use constellation_pbft::proto::PBFTProto;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
#[cfg(feature = "standalone")]
use constellation_standalone::StandaloneService;
use constellation_streams::multicast::MulticastStreamIdx;
use constellation_streams::threads::poll::PollThreadStartError;
use log::debug;
use log::error;
use log::info;
use log::warn;

use crate::config::ConsensusComponentConfig;
//#[cfg(feature = "standalone")]
//use crate::config::StandaloneConfig;
use crate::peers::PeerSessionDispatch;
use crate::peers::PeerSessionState;
use crate::recv::ConsensusAuthNRecv;
use crate::state::State;
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
where Types: 'static + ConsensusTypes + Send
{
    state: StateThread<Types::IDTypes, Types::ProtoTypes, Types>,
    multicast: MulticastDatagramBusStarter<Types::ConsensusBusTypes, Types::Ctx>,
    peers: DispatchLargeObjBusStarter<Types::PeerBusTypes, Types::Ctx>,
}

pub struct ConsensusComponentCleanup<Types>
where Types: 'static + ConsensusTypes + Send
{
    shutdown: ShutdownFlag,
    consensus: MulticastDatagramBus<Types::ConsensusBusTypes, Types::Ctx>,
    peers: DispatchLargeObjBus<Types::PeerBusTypes, Types::Ctx>,
    state_join: JoinHandle<()>
}

pub enum ConsensusComponentRunError<RoundIDs, Hash, Consensus, Rounds> {
    RoundIDs {
        err: RoundIDs
    },
    Hash {
        err: Hash
    },
    Consensus {
        err: Consensus
    },
    Rounds {
        err: Rounds
    },
    Poll {
        err: PollThreadStartError
    },
    IO {
        err: std::io::Error
    },
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
where Types: ConsensusTypes + Send
{
    pub fn start(
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
        shutdown: ShutdownFlag
    ) -> Result<
        ConsensusComponentCleanup<Types>,
        ConsensusComponentRunError<
            Types::RoundIDsCreateError,
            Types::HashCreateError,
            Types::ConsensusCreateError,
            Types::ConsensusRoundsError,
        >
    > {
        info!(target: "consensus-component",
              "starting consensus component");

        let (round_ids_config, hash_config, proto_config,
             consensus_config, peers_config) =
            config.take();
        // XXX Hacky and wrong way to set parties
        let parties: Vec<(MulticastStreamIdx, Types::Prin)> = consensus_config
            .thread()
            .stream()
            .parties()
            .iter()
            .enumerate()
            .map(|(i, party)| (MulticastStreamIdx::from(i),
                               party.party().clone()))
            .collect();
        let round_ids = Types::RoundIDs::create(round_ids_config)
            .map_err(|err| ConsensusComponentRunError::RoundIDs { err: err })?;
        let hash = Types::Hash::create(hash_config)
            .map_err(|err| ConsensusComponentRunError::Hash { err: err })?;
        let proto = Types::Consensus::create(proto_config)
            .map_err(|err| ConsensusComponentRunError::Consensus { err: err })?;
        let rounds = proto.rounds(round_ids)
            .map_err(|err| ConsensusComponentRunError::Rounds { err: err })?;
        let (peers, peers_notify) =
            DispatchLargeObjBusStarter::create(peers_config, ctx.clone())
            .map_err(|err| ConsensusComponentRunError::IO { err: err })?;
        let (consensus, consensus_notify) =
            MulticastDatagramBusStarter::create(consensus_config, ctx.clone())
            .map_err(|err| ConsensusComponentRunError::IO { err: err })?;
        let peer_state = PeerSessionState::new();
        let state = State::new(rounds);
        let (state_thread, peers_sender, consensus_sender, state_notify) =
            StateThread::new(
                state.clone(), peer_state, peers_notify,
                consensus_notify, shutdown.clone(), hash
            )
            .map_err(|err| ConsensusComponentRunError::IO { err: err })?;
        // XXX use size hints.
        let peer_dispatch = PeerSessionDispatch::new(
            peers_sender,
            state_notify.clone(),
            None,
            None
        );
        let mut consensus_recv = ConsensusAuthNRecv::create(
            consensus_sender, state_notify.clone()
        );
        let state_join = state_thread.start()
            .map_err(|err| ConsensusComponentRunError::IO { err: err })?;

        consensus_recv.set_parties(parties.into_iter())
            .map_err(|_| ConsensusComponentRunError::MutexPoison)?;

        let peers = peers.start(peer_dispatch, shutdown.clone())
            .map_err(|err| match err {
                WithMutexPoison::Inner { err } =>
                    ConsensusComponentRunError::IO { err: err },
                WithMutexPoison::MutexPoison =>
                    ConsensusComponentRunError::MutexPoison
            })?;
        let consensus = consensus.start(consensus_recv, state, shutdown.clone())
            .map_err(|err| ConsensusComponentRunError::Poll { err: err })?;

        Ok(ConsensusComponentCleanup {
            shutdown: shutdown,
            consensus: consensus,
            peers: peers,
            state_join: state_join
        })
    }
}

impl<Types> ConsensusComponentCleanup<Types>
where Types: ConsensusTypes + Send
{
    pub fn cleanup(mut self) {
        if let Err(err) = self.shutdown.set() {
            error!(target: "consensus-component-cleanup",
                   "error setting shutdown flag: {}",
                   err);
        }

        self.consensus.cleanup();
        self.peers.cleanup();

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

#[cfg(feature = "standalone")]
pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

#[cfg(feature = "standalone")]
#[derive(Clone)]
pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches,
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

impl<RoundIDs, Hash, Consensus, Rounds> Display
    for ConsensusComponentRunError<RoundIDs, Hash, Consensus, Rounds>
where
    RoundIDs: Display,
    Hash: Display,
    Consensus: Display,
    Rounds: Display {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            ConsensusComponentRunError::RoundIDs { err } => err.fmt(f),
            ConsensusComponentRunError::Hash { err } => err.fmt(f),
            ConsensusComponentRunError::Consensus { err } => err.fmt(f),
            ConsensusComponentRunError::Rounds { err } => write!(f, "{}", err),
            ConsensusComponentRunError::Poll { err } => write!(f, "{}", err),
            ConsensusComponentRunError::IO { err } => write!(f, "{}", err),
            ConsensusComponentRunError::MutexPoison =>
                write!(f, "mutex poisoned")
        }
    }
}

/*
// ISSUE #2: AscendingCount is temporary, and will be replaced with a random
// number stream.
#[cfg(feature = "standalone")]
impl Standalone
    for CompoundConsensusComponent<
        StandaloneCtx,
        AscendingCount<u128>,
        AscendingCount<u128>,
        PBFTProto<SHA3Algo, AscendingCount<u128>, String>,
        PBFTMsgPERCodec,
        SHA3Algo,
        AscendingCount<LargeObjID>,
        StringPrincipalCodec
    >
{
    type Config = StandaloneConfig;
    type CreateCleanup = StandaloneCreateCleanup;

    const CONFIG_FILES: &[&str] = &["consensus.conf"];
    const NAME: &str = "consensus";
    const VERSION: FullVersion = FullVersion::new(
        None,
        Version::new(0, 0, 0),
        Some(VersionSuffix::Development)
    );

    fn create(
        _args: ArgMatches,
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let (name_caches_config, component_config) = config.take();
        let (consensus_config, consensus_registry_config, peers_config) =
            component_config.take();
        let (
            peers_registry_config,
            peers_comm_config,
            peers_large_obj_config,
            peers_authn_config
        ) = peers_config.take();
        let shutdown = ShutdownFlag::new();
        let (mut caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config, shutdown.clone());
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };
        let (consensus_listener, consensus_reporter) =
            ThreadedFlowsListener::new();
        let (peers_listener, peers_reporter) = ThreadedFlowsListener::new();

        // ISSUE #6: This part is temporary, until we get a real
        // authenticator.
        let multicast_config = consensus_config.multicast();
        let parties_config = multicast_config.parties();
        let authn_parties = match parties_config {
            PartiesConfig::Static { stat } => {
                let mut authn_parties = Vec::with_capacity(stat.len());

                for party in stat {
                    let id = party.party();

                    for conn in party.party_config().connections() {
                        for endpoint in conn.endpoints() {
                            match endpoint {
                                CompoundFarEndpoint::Unix { unix_datagram } => {
                                    match UnixSocketAddr::try_from(
                                        unix_datagram
                                    ) {
                                        Ok(addr) => {
                                            let cred =
                                                TestCred::Unix { addr: addr };

                                            authn_parties
                                                .push((cred, id.clone()));
                                        }
                                        Err(err) => {
                                            warn!(target: "start",
                                              "error converting path: {}",
                                              err);
                                        }
                                    }
                                }
                                CompoundFarEndpoint::UDP { udp } => match udp
                                    .ip_endpoint()
                                {
                                    IPEndpointAddr::Addr(addr) => {
                                        let addr =
                                            SocketAddr::new(*addr, udp.port());
                                        let cred = TestCred::IP { addr: addr };

                                        authn_parties.push((cred, id.clone()));
                                    }
                                    IPEndpointAddr::Name(name) => {
                                        warn!(target: "start",
                                               "discarding endpoint {}",
                                               name);
                                    }
                                }
                            }
                        }
                    }
                }

                authn_parties
            }
        };
        let consensus_authn =
            Arc::new(TestAuthN::from_parties(authn_parties.into_iter()));

        let consensus_registry = match StandaloneRegistry::create(
            &mut caches,
            consensus_authn,
            consensus_reporter,
            consensus_registry_config
        ) {
            Ok(registry) => registry,
            Err(err) => {
                error!(target: "start",
                       "error creating channel registry: {}",
                       err);

                return Err(cleanup);
            }
        };
        let peers_authn = match TestAuthN::create(peers_authn_config) {
            Ok(peers_authn) => peers_authn,
            Err(err) => {
                error!(target: "start",
                       "error creating authn: {}",
                       err);

                return Err(cleanup);
            }
        };
        let peers_authn = Arc::new(peers_authn);
        let peers_registry = match StandaloneRegistry::create(
            &mut caches,
            peers_authn,
            peers_reporter,
            peers_registry_config
        ) {
            Ok(registry) => registry,
            Err(err) => {
                error!(target: "start",
                       "error creating channel registry: {}",
                       err);

                return Err(cleanup);
            }
        };
        let consensus_ctx = StandaloneCtx {
            registry: Arc::new(consensus_registry),
            caches: caches.clone()
        };
        let peers_ctx = StandaloneCtx {
            registry: Arc::new(peers_registry),
            caches: caches
        };
        let round_ids = AscendingCount::default();
        let standalone = ConsensusComponent {
            channel: PhantomData,
            resolver: PhantomData,
            proto: PhantomData,
            xfrm: PhantomData,
            flow: PhantomData,
            round_ids: round_ids,
            consensus_config: consensus_config,
            consensus_listener: consensus_listener,
            peers_large_obj_config: peers_large_obj_config,
            peers_comm_config: peers_comm_config,
            peers_listener: peers_listener,
            shutdown: shutdown,
            consensus_ctx: consensus_ctx,
            peers_ctx: peers_ctx
        };

        Ok((standalone, cleanup))
    }
}

impl StandaloneService
    for CompoundConsensusComponent<
        StandaloneCtx,
        AscendingCount<u128>,
        AscendingCount<u128>,
        PBFTProto<SHA3Algo, AscendingCount<u128>, String>,
        PBFTMsgPERCodec,
        SHA3Algo,
        AscendingCount<LargeObjID>,
        StringPrincipalCodec
    >
{
    type RunCleanup = ConsensusComponentCleanup;
    type RunErrorCleanup = ();

    fn run(self) -> Result<Self::RunCleanup, Self::RunErrorCleanup> {
        match self.start() {
            Ok(out) => Ok(out),
            Err(err) => {
                error!(target: "consensus-component",
                       "{}", err);

                Err(())
            }
        }
    }

    fn shutdown(
        mut create_cleanup: Self::CreateCleanup,
        run_cleanup: Option<Self::RunCleanup>
    ) {
        debug!(target: "consensus-standalone",
               "cleaning up consensus");

        create_cleanup.shutdown.set();

        if let Some(cleanup) = run_cleanup {
            debug!(target: "consensus-standalone",
               "cleaning up runtime");

            cleanup.cleanup();
        }

        debug!(target: "consensus-standalone",
               "cleaning up caches");

        if create_cleanup.caches_join.join().is_err() {
            error!(target: "standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }

    fn shutdown_err(
        mut create_cleanup: Self::CreateCleanup,
        _run_cleanup: ()
    ) {
        debug!(target: "consensus-standalone",
               "cleaning up consensus");

        create_cleanup.shutdown.set();

        if create_cleanup.caches_join.join().is_err() {
            error!(target: "standalone-shutdown",
                   "error shutting down name chache threads")
        }
    }
}

pub struct StringPrincipalDecodeError;

impl Display for StringPrincipalDecodeError {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "UTF-8 error")
    }
}

impl ScopedError for StringPrincipalDecodeError {
    fn scope(&self) -> ErrorScope {
        ErrorScope::Unrecoverable
    }
}

impl Codec<String> for StringPrincipalCodec {
    type CreateError = Infallible;
    type DecodeError = StringPrincipalDecodeError;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: Self::Param) -> Result<Self, Self::CreateError> {
        Ok(StringPrincipalCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        val: &String
    ) -> usize {
        val.as_bytes().len()
    }

    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(String, usize), Self::DecodeError> {
        let len = buf.len();
        let string = from_utf8(buf)
            .map_err(|_| StringPrincipalDecodeError)?
            .to_string();

        Ok((string, len))
    }

    fn encode(
        &mut self,
        val: &String,
        buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        let bytes = val.as_bytes();
        let len = bytes.len();

        buf.copy_from_slice(bytes);

        Ok(len)
    }

    fn encode_to_vec(
        &mut self,
        val: &String
    ) -> Result<Vec<u8>, Self::EncodeError> {
        Ok(val.as_bytes().to_vec())
    }
}

impl Display for ConsensusComponentRunError {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "failed to start consensus component")
    }
}

// ISSUE #2: Delete from here

#[derive(Clone, Debug)]
pub struct TestSeal;

#[derive(Clone)]
pub struct TestSealCodec;

impl Codec<TestSeal> for TestSealCodec {
    type CreateError = Infallible;
    type DecodeError = Infallible;
    type EncodeError = Infallible;
    type Param = ();

    #[inline]
    fn create(_param: ()) -> Result<Self, Infallible> {
        Ok(TestSealCodec)
    }

    #[inline]
    fn buf_size(
        &self,
        _val: &TestSeal
    ) -> usize {
        0
    }

    #[inline]
    fn encode(
        &mut self,
        _val: &TestSeal,
        _buf: &mut [u8]
    ) -> Result<usize, Self::EncodeError> {
        Ok(0)
    }

    #[inline]
    fn decode(
        &mut self,
        _buf: &[u8]
    ) -> Result<(TestSeal, usize), Self::DecodeError> {
        Ok((TestSeal, 0))
    }
}

// ISSUE #2: to here
*/
