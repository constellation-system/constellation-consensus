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
use constellation_channels::far::compound::CompoundFarChannelXfrmPeerAddr;
use constellation_channels::far::compound::CompoundFarIPChannelXfrmPeerAddr;
use constellation_channels::far::udp::UDPDatagramXfrm;
use constellation_channels::far::unix::UnixDatagramXfrm;
use constellation_channels::far::FarChannelAcquiredResolve;
use constellation_channels::far::FarChannelCreate;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_channels::resolve::MixedResolver;
use constellation_common::codec::DatagramCodec;
use constellation_common::error::ErrorScope;
use constellation_common::error::ScopedError;
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
use constellation_component_common::bus::dispatch::DispatchBus;
use constellation_component_common::bus::dispatch::DispatchBusCleanup;
use constellation_component_common::bus::multicast::MulticastBus;
use constellation_component_common::bus::multicast::MulticastBusCleanup;
use constellation_component_common::config::PartiesConfig;
use constellation_component_common::config::DispatchLargeObjBusConfig;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_component_common::consensus_ctl::ConsensusCtlCodec;
use constellation_component_common::PartyStreamIdx;
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
use constellation_streams::addrs::Addrs;
use constellation_streams::addrs::AddrsCreate;
use constellation_streams::channels::ChannelParam;
use constellation_streams::config::LargeObjProtoConfig;
use constellation_streams::error::ErrorReportInfo;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::stream::StreamID;
use log::debug;
use log::error;
use log::info;
use log::warn;

use crate::config::ConsensusConfig;
#[cfg(feature = "standalone")]
use crate::config::StandaloneConfig;
use crate::peers::PeerSessionDispatch;
use crate::recv::ConsensusAuthNRecv;
use crate::state::State;
use crate::state::StateMsgs;
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
where Types: ConsensusTypes
{
    round_ids: Types::RoundIDs,
    consensus_config: ConsensusConfig<
        Types::Party,
        Types::PartyCodecConfig,
        Types::ConsensusMsgAuthConfig,
        Types::ConsensusConfig,
        Types::ConsensusChansConfig,
        Types::ConsensusEpochsConfig,
        Types::Addr
    >,
    peers_bus_config: DispatchLargeObjBusConfig<Types::PeerChansConfig,
                                                Types::PeerEpochsConfig,
                                                Types::PeerMsgAuthConfig>,
    shutdown: ShutdownFlag,
    consensus_ctx: Types::Ctx,
    peers_ctx: Types::Ctx
}

pub struct ConsensusComponentCleanup {
    shutdown: ShutdownFlag,
    multicast: DispatchBusCleanup,
    peers: DispatchBusCleanup,
    state_join: JoinHandle<()>
}

pub struct ConsensusComponentRunError;

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
where Types: ConsensusTypes
{
    pub fn start(
        self
    ) -> Result<ConsensusComponentCleanup, ConsensusComponentRunError> {
        let ConsensusComponent {
            consensus_config,
            peers_bus_config,
            consensus_ctx,
            peers_ctx,
            shutdown,
            round_ids,
            ..
        } = self;

        info!(target: "consensus-component",
              "starting consensus component");

        let (proto_config, self_party, prin_codec_param, multicast_config) =
            consensus_config.take();
        let prin_codec = match Types::PartyCodec::create(prin_codec_param) {
            Ok(codec) => codec,
            Err(err) => {
                error!(target: "consensus-component",
                       "error creating principal codec: {}",
                       err);

                return Err(ConsensusComponentRunError);
            }
        };
        let proto: SharedConsensusProto<Types::Proto, Types::ProtoTypes> =
            match SharedConsensusProto::create(proto_config, prin_codec.clone())
        {
            Ok(proto) => proto,
            Err(err) => {
                error!(target: "consensus-component",
                           "error creating consensus protocol: {}",
                           err);

                return Err(ConsensusComponentRunError);
            }
        };
        let rounds = match proto.rounds(round_ids) {
            Ok(proto) => proto,
            Err(err) => {
                error!(target: "consensus-component",
                           "error creating consensus protocol rounds: {}",
                           err);

                return Err(ConsensusComponentRunError);
            }
        };
        let state = State::new(rounds);
        let state = Arc::new(state);
        let peer_dispatch =
            PeerSessionDispatch::new(peers_bus_config, state.clone());
        let peers_comm: DispatchBus<Types::PeerBusTypes> =
            match DispatchBus::create(
                peers_bus_config,
                peer_dispatch,
                shutdown.clone(),
                peers_ctx
            ) {
                Ok(peer_comm) => peer_comm,
                Err(err) => {
                    error!(target: "consensus-component",
                           "error creating peer dispatch comm: {}",
                           err);

                    return Err(ConsensusComponentRunError);
                }
            };
        let notify = state.notify();
        let (state_thread, round_reporter) = StateThread::create(
            state.clone(),
            shutdown.clone(),
            Types::Hash::default()
        );
        let mut authn_msg_recv = ConsensusAuthNRecv::create(
            round_reporter,
            state.clone(),
            notify.clone()
        );
        let multicast: MulticastBus<Types::ConsensusBusTypes, Types::Ctx> =
            match MulticastBus::create(
                Some(self_party.clone()),
                multicast_config,
                consensus_ctx,
                shutdown.clone(),
                notify,
                authn_msg_recv.clone(),
                StateMsgs::from(state.clone())
            ) {
                Ok(multicast) => multicast,
                Err(err) => {
                    error!(target: "consensus-component",
                           "error creating consensus multicast comm: {}",
                           err);

                    return Err(ConsensusComponentRunError);
                }
            };
        let party_data = match multicast.parties() {
            Ok(parties) => {
                let mut parties: Vec<(PartyStreamIdx, Types::Party)> =
                    parties.collect();

                parties.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

                let mut party_data = Vec::with_capacity(parties.len());

                for (idx, party) in parties.into_iter() {
                    let idx: usize = idx.into();

                    if idx == party_data.len() {
                        party_data.push(party)
                    } else {
                        error!(target: "consensus-component",
                               "stream parties skipped an index");

                        return Err(ConsensusComponentRunError);
                    }
                }

                party_data
            }
        };

        if let Err(err) = state.init_rounds(prin_codec, self_party, &party_data)
        {
            error!("error setting parties: {}", err);

            return Err(ConsensusComponentRunError);
        }

        let Ok(party_iter) = multicast.parties();

        if let Err(err) = authn_msg_recv.set_parties(party_iter) {
            error!("error setting parties: {}", err);
        }

        let state_join = state_thread.start();

        debug!(target: "consensus-component",
               "starting multicaster");

        match multicast.start() {
            Ok(multicast_cleanup) => match peers_comm.start() {
                Ok(peers_cleanup) => Ok(ConsensusComponentCleanup {
                    shutdown: shutdown,
                    multicast: multicast_cleanup,
                    peers: peers_cleanup,
                    state_join: state_join
                }),
                Err(err) => {
                    error!("error starting peers bus: {}", err);

                    Err(ConsensusComponentRunError)
                }
            },
            Err(err) => {
                error!("error starting multicast bus: {}", err);

                Err(ConsensusComponentRunError)
            }
        }
    }
}

impl ConsensusComponentCleanup {
    pub fn cleanup(mut self) {
        self.shutdown.set();
        self.multicast.cleanup();
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
