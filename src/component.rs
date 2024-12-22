// Copyright © 2024 The Johns Hopkins Applied Physics Laboratory LLC.
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
use std::iter::successors;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::str::Utf8Error;
use std::sync::Arc;
use std::thread::JoinHandle;

use constellation_auth::authn::PassthruMsgAuthN;
use constellation_auth::authn::SessionAuthN;
use constellation_auth::authn::TestAuthN;
use constellation_auth::cred::SSLCred;
use constellation_channels::config::ChannelRegistryChannelsConfig;
use constellation_channels::config::CompoundEndpoint;
use constellation_channels::config::ResolverConfig;
use constellation_channels::far::compound::CompoundFarChannel;
#[cfg(feature = "standalone")]
use constellation_channels::far::compound::CompoundFarChannelThreadedFlows;
use constellation_channels::far::compound::CompoundFarChannelXfrm;
use constellation_channels::far::compound::CompoundFarChannelXfrmPeerAddr;
use constellation_channels::far::compound::CompoundFarCredential;
use constellation_channels::far::compound::CompoundFarIPChannelXfrmPeerAddr;
use constellation_channels::far::flows::CreateOwnedFlows;
use constellation_channels::far::flows::Flows;
use constellation_channels::far::flows::OwnedFlows;
use constellation_channels::far::flows::OwnedFlowsNegotiator;
use constellation_channels::far::flows::ThreadedFlows;
use constellation_channels::far::flows::ThreadedFlowsListener;
use constellation_channels::far::flows::ThreadedFlowsPullStreamListener;
#[cfg(feature = "standalone")]
use constellation_channels::far::registry::FarChannelRegistry;
use constellation_channels::far::registry::FarChannelRegistryChannels;
use constellation_channels::far::registry::FarChannelRegistryCtx;
use constellation_channels::far::registry::FarChannelRegistryID;
use constellation_channels::far::udp::UDPDatagramXfrm;
use constellation_channels::far::unix::UnixDatagramXfrm;
use constellation_channels::far::FarChannelAcquiredResolve;
use constellation_channels::far::FarChannelCreate;
use constellation_channels::far::FarChannelOwnedFlows;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_channels::resolve::cache::ThreadedNSNameCaches;
use constellation_channels::resolve::MixedResolver;
use constellation_channels::unix::UnixSocketAddr;
use constellation_common::codec::DatagramCodec;
use constellation_common::net::DatagramXfrm;
use constellation_common::net::DatagramXfrmCreate;
use constellation_common::net::DatagramXfrmCreateParam;
use constellation_common::net::IPEndpointAddr;
use constellation_common::net::Socket;
use constellation_common::sched::DenseItemID;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_consensus_common::parties::StaticParties;
use constellation_consensus_common::proto::ConsensusProto;
use constellation_consensus_common::proto::ConsensusProtoRounds;
use constellation_consensus_common::proto::SharedConsensusProto;
use constellation_consensus_common::round::SharedRounds;
use constellation_consensus_common::state::ProtoState;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PBFTMsgPERCodec;
#[cfg(feature = "standalone")]
use constellation_pbft::proto::PBFTProto;
#[cfg(feature = "standalone")]
use constellation_standalone::Standalone;
use constellation_streams::addrs::Addrs;
use constellation_streams::addrs::AddrsCreate;
use constellation_streams::channels::ChannelParam;
use constellation_streams::error::ErrorReportInfo;
use constellation_streams::multicast::StreamMulticaster;
use constellation_streams::select::StreamSelector;
use constellation_streams::stream::pull::PullStreams;
use constellation_streams::stream::pull::PullStreamsReporter;
use constellation_streams::stream::ConcurrentStream;
use constellation_streams::stream::PushStreamParties;
use constellation_streams::stream::PushStreamReporter;
use constellation_streams::stream::StreamID;
use log::debug;
use log::error;
use log::info;
use log::warn;

use crate::config::ConsensusConfig;
use crate::config::PartiesConfig;
#[cfg(feature = "standalone")]
use crate::config::StandaloneConfig;
use crate::poll::PollThread;
use crate::send::SendThread;
use crate::state::StateThread;

/// Index used to identify principals in the stream.
///
/// These correspond one-to-one with consensus parties, but not all
/// parties may be present in a given round.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartyStreamIdx(usize);

/// Index used to identify parties in a given round.
///
/// These are generally a subset of the total set of parties, and the
/// indexing scheme may change from round to round.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartyRoundIdx(usize);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StringPrincipalCodec;

// ISSUE #2: this is temporary, and will be replaced with a random
// number stream.
pub struct AscendingCount {
    curr: u128
}

pub type CompoundConsensusComponent<
    Ctx,
    RoundIDs,
    Proto,
    MsgCodec,
    Prin,
    PrinCodec
> = ConsensusComponent<
    RoundIDs,
    Proto,
    MsgCodec,
    CompoundFarChannel,
    ThreadedFlows<
        CompoundFarChannel,
        CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>,
        FarChannelRegistryID
    >,
    Arc<TestAuthN<String, TestCred>>,
    CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>,
    Ctx,
    MixedResolver<CompoundFarChannelXfrmPeerAddr, CompoundEndpoint>,
    Prin,
    PrinCodec,
    CompoundEndpoint
>;

pub struct ConsensusComponent<
    RoundIDs,
    Proto,
    MsgCodec,
    Channel,
    F,
    AuthN,
    Xfrm,
    Ctx,
    Resolver,
    Prin,
    PrinCodec,
    Endpoint
> where
    RoundIDs: 'static + Iterator + Send,
    RoundIDs::Item: Clone + Display + Ord + Send,
    Proto: ConsensusProto<Prin, PrinCodec>
        + ConsensusProtoRounds<
            RoundIDs,
            PartyStreamIdx,
            Prin,
            PrinCodec,
            StaticParties<PartyStreamIdx>
        > + Send,
    <Proto::State as ProtoState<RoundIDs::Item, PartyStreamIdx>>::Oper: Send,
    Proto::Msg: Clone + Debug + Send,
    Proto::Out: Send,
    MsgCodec: Clone + DatagramCodec<Proto::Msg> + Send,
    <MsgCodec as DatagramCodec<Proto::Msg>>::Param: Default,
    <MsgCodec as DatagramCodec<Proto::Msg>>::EncodeError:
        ErrorReportInfo<DenseItemID<usize>>,
    AuthN: Clone
        + SessionAuthN<<Channel::Nego as OwnedFlowsNegotiator>::Flow, Prin = Prin>
        + SessionAuthN<<Channel::Owned as OwnedFlows>::Flow, Prin = Prin>
        + Send
        + Sync,
    <AuthN as SessionAuthN<<Channel::Owned as OwnedFlows>::Flow>>::Prin: Send,
    Ctx: 'static
        + FarChannelRegistryCtx<Channel, F, AuthN, Xfrm>
        + NSNameCachesCtx
        + Send
        + Sync,
    Ctx::NameCaches: NSNameCachesCtx,
    Channel: 'static
        + FarChannelOwnedFlows<F, AuthN, Xfrm>
        + FarChannelCreate
        + Send
        + Sync,
    <Channel::Owned as OwnedFlows>::Flow: 'static + ConcurrentStream + Send,
    F: 'static
        + Flows<Xfrm = Channel::Xfrm>
        + CreateOwnedFlows<Channel::Nego, AuthN, ChannelID = FarChannelRegistryID>
        + OwnedFlows<Xfrm = Channel::Xfrm>,
    <F::Xfrm as DatagramXfrm>::PeerAddr:
        From<<Channel::Xfrm as DatagramXfrm>::PeerAddr>,
    F::CreateParam: Clone + Default + Send + Sync,
    F::Reporter: Clone + Send + Sync,
    Xfrm: 'static
        + DatagramXfrm
        + DatagramXfrmCreate<Addr = Channel::Param>
        + DatagramXfrmCreateParam,
    Xfrm::CreateParam: Clone + Default + Send + Sync,
    Xfrm::LocalAddr: From<<Channel::Socket as Socket>::Addr>,
    <Channel::Xfrm as DatagramXfrm>::PeerAddr:
        'static + Clone + Eq + Hash + Send + Sync,
    Channel::Acquired:
        FarChannelAcquiredResolve<Resolved = Channel::Param> + Send + Sync,
    Channel::Param: 'static
        + Clone
        + Display
        + Eq
        + Hash
        + PartialEq
        + ChannelParam<<Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + Send
        + Sync,
    Channel::Owned: Send + Sync,
    F::Socket: From<Channel::Socket>,
    Prin: 'static + Clone + Display + Eq + Hash + Send,
    PrinCodec: Clone + DatagramCodec<Prin> + Send,
    PrinCodec::Param: Default,
    Endpoint: 'static + Send,
    Resolver: 'static
        + Addrs<Addr = <Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + AddrsCreate<Ctx, Vec<Endpoint>, Config = ResolverConfig>
        + Send
        + Sync,
    Resolver::Origin: 'static
        + Clone
        + Eq
        + Hash
        + Into<Option<IPEndpointAddr>>
        + Send
        + Sync,
    [(); PrinCodec::MAX_BYTES]:,
    [(); MsgCodec::MAX_BYTES]: {
    round_ids: RoundIDs,
    channel: PhantomData<Channel>,
    proto: PhantomData<Proto>,
    flow: PhantomData<F>,
    xfrm: PhantomData<Xfrm>,
    resolver: PhantomData<Resolver>,
    config: ConsensusConfig<
        Prin,
        PrinCodec::Param,
        Proto::Config,
        ChannelRegistryChannelsConfig<MsgCodec::Param>,
        Endpoint
    >,
    listener: ThreadedFlowsListener<
        StreamID<
            <Channel::Xfrm as DatagramXfrm>::PeerAddr,
            F::ChannelID,
            Channel::Param
        >,
        <AuthN as SessionAuthN<<Channel::Owned as OwnedFlows>::Flow>>::Prin,
        <Channel::Owned as OwnedFlows>::Flow
    >,
    shutdown: ShutdownFlag,
    ctx: Ctx
}

pub struct ConsensusComponentCleanup {
    notify: Notify,
    shutdown: ShutdownFlag,
    poll_join: JoinHandle<()>,
    sender_join: JoinHandle<()>,
    pull_join: JoinHandle<()>,
    state_join: JoinHandle<()>
}

pub struct ConsensusComponentRunError;

impl From<usize> for PartyStreamIdx {
    #[inline]
    fn from(val: usize) -> PartyStreamIdx {
        PartyStreamIdx(val)
    }
}

impl From<PartyStreamIdx> for usize {
    #[inline]
    fn from(val: PartyStreamIdx) -> usize {
        val.0
    }
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

impl<
        RoundIDs,
        Proto,
        MsgCodec,
        Channel,
        F,
        AuthN,
        Xfrm,
        Ctx,
        Resolver,
        Prin,
        PrinCodec,
        Endpoint
    >
    ConsensusComponent<
        RoundIDs,
        Proto,
        MsgCodec,
        Channel,
        F,
        AuthN,
        Xfrm,
        Ctx,
        Resolver,
        Prin,
        PrinCodec,
        Endpoint
    >
where
    RoundIDs: 'static + Iterator + Send,
    RoundIDs::Item: 'static + Clone + Display + Ord + Send,
    Proto: ConsensusProto<Prin, PrinCodec>
        + ConsensusProtoRounds<
            RoundIDs,
            PartyStreamIdx,
            Prin,
            PrinCodec,
            StaticParties<PartyStreamIdx>
        > + Send,
    <Proto::State as ProtoState<RoundIDs::Item, PartyStreamIdx>>::Oper:
        'static + Send,
    Proto::Msg: 'static + Clone + Debug + Send,
    Proto::Rounds: 'static + Send,
    Proto::Out: 'static + Send,
    MsgCodec: 'static + Clone + DatagramCodec<Proto::Msg> + Send,
    <MsgCodec as DatagramCodec<Proto::Msg>>::Param: Default,
    <MsgCodec as DatagramCodec<Proto::Msg>>::EncodeError:
        ErrorReportInfo<DenseItemID<usize>>,
    AuthN: 'static
        + Clone
        + SessionAuthN<<Channel::Nego as OwnedFlowsNegotiator>::Flow, Prin = Prin>
        + SessionAuthN<<Channel::Owned as OwnedFlows>::Flow, Prin = Prin>
        + Send
        + Sync,
    <AuthN as SessionAuthN<<Channel::Owned as OwnedFlows>::Flow>>::Prin: Send,
    Ctx: 'static
        + FarChannelRegistryCtx<Channel, F, AuthN, Xfrm>
        + NSNameCachesCtx
        + Send
        + Sync,
    Ctx::NameCaches: NSNameCachesCtx,
    Channel: 'static
        + FarChannelOwnedFlows<F, AuthN, Xfrm>
        + FarChannelCreate
        + Send
        + Sync,
    <Channel::Owned as OwnedFlows>::Flow: 'static + ConcurrentStream + Send,
    F: 'static
        + Flows<Xfrm = Channel::Xfrm>
        + CreateOwnedFlows<Channel::Nego, AuthN, ChannelID = FarChannelRegistryID>
        + OwnedFlows<Xfrm = Channel::Xfrm>,
    <F::Xfrm as DatagramXfrm>::PeerAddr:
        From<<Channel::Xfrm as DatagramXfrm>::PeerAddr>,
    F::CreateParam: Clone + Default + Send + Sync,
    F::Reporter: Clone + Send + Sync,
    Xfrm: 'static
        + DatagramXfrm
        + DatagramXfrmCreate<Addr = Channel::Param>
        + DatagramXfrmCreateParam,
    Xfrm::CreateParam: Clone + Default + Send + Sync,
    Xfrm::LocalAddr: From<<Channel::Socket as Socket>::Addr>,
    <Channel::Xfrm as DatagramXfrm>::PeerAddr:
        'static + Clone + Eq + Hash + Send + Sync,
    Channel::Acquired:
        FarChannelAcquiredResolve<Resolved = Channel::Param> + Send + Sync,
    Channel::Param: 'static
        + Clone
        + Display
        + Eq
        + Hash
        + PartialEq
        + ChannelParam<<Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + Send
        + Sync,
    Channel::Owned: Send + Sync,
    F::Socket: From<Channel::Socket>,
    Endpoint: 'static + Send,
    Prin: 'static + Clone + Display + Eq + Hash + Send,
    PrinCodec: 'static + Clone + DatagramCodec<Prin> + Send,
    PrinCodec::Param: Default,
    Resolver: 'static
        + Addrs<Addr = <Channel::Xfrm as DatagramXfrm>::PeerAddr>
        + AddrsCreate<Ctx, Vec<Endpoint>, Config = ResolverConfig>
        + Send
        + Sync,
    Resolver::Origin: 'static
        + Clone
        + Eq
        + Hash
        + Into<Option<IPEndpointAddr>>
        + Send
        + Sync,
    [(); PrinCodec::MAX_BYTES]:,
    [(); MsgCodec::MAX_BYTES]:
{
    pub fn start(
        self
    ) -> Result<ConsensusComponentCleanup, ConsensusComponentRunError> {
        let ConsensusComponent {
            config,
            listener,
            mut ctx,
            shutdown,
            round_ids,
            ..
        } = self;

        info!(target: "consensus-component",
              "starting consensus component");

        debug!(target: "consensus-component",
               "initializing channels");

        // Bring up all channels.
        if let Err(err) = ctx.far_channel_registry().acquire_all(&mut ctx) {
            error!(target: "consensus-component",
                   "error initializing channel registry: {}",
                   err);

            return Err(ConsensusComponentRunError);
        }

        // Bring up the pull-side.
        debug!(target: "consensus-component",
               "initializing pull streams");

        let (proto_config, multicast_config) = config.take();
        let (self_party, prin_codec_param, slots_config, parties_config) =
            multicast_config.take();

        // ISSUE #5: get the codec config properly
        let msg_codec = match MsgCodec::create(MsgCodec::Param::default()) {
            Ok(codec) => codec,
            Err(err) => {
                error!(target: "consensus-component",
                       "error creating message codec: {}",
                       err);

                return Err(ConsensusComponentRunError);
            }
        };
        let prin_codec = match PrinCodec::create(prin_codec_param) {
            Ok(codec) => codec,
            Err(err) => {
                error!(target: "consensus-component",
                       "error creating principal codec: {}",
                       err);

                return Err(ConsensusComponentRunError);
            }
        };

        let listener =
            ThreadedFlowsPullStreamListener::create(listener, msg_codec);
        let (pull_streams, pull_listener, receiver) =
            PullStreams::with_capacity(
                listener,
                shutdown.clone(),
                PassthruMsgAuthN::default(),
                1
            );
        let stream_reporter = pull_streams.reporter();

        // Bring up the push-side.
        debug!(target: "consensus-component",
               "initializing push streams");

        let (stream, parties) = match parties_config {
            PartiesConfig::Static { stat } => {
                let mut party_streams = Vec::with_capacity(stat.len());

                for party in stat {
                    let (party, party_config) = party.take();

                    let mut stream = match StreamSelector::<
                        _,
                        FarChannelRegistryChannels<
                            Proto::Msg,
                            MsgCodec,
                            PullStreamsReporter<Proto::Msg, _, _, _>,
                            Channel,
                            F,
                            AuthN,
                            Xfrm
                        >,
                        Resolver,
                        Ctx
                    >::create(
                        &mut ctx,
                        stream_reporter.clone(),
                        party_config,
                        successors(Some(0), |n| Some(n + 1))
                    ) {
                        Ok(stream_multiplexer) => stream_multiplexer,
                        Err(err) => {
                            error!(target: "consensus-component",
                                       "error creating stream multiplexer: {}",
                                       err);

                            return Err(ConsensusComponentRunError);
                        }
                    };
                    // Refresh the streams to ensure no bad stream reporting.
                    if let Err(err) = stream.refresh(&mut ctx) {
                        error!(target: "consensus-component",
                               "error doing initial stream refresh: {}",
                               err);

                        return Err(ConsensusComponentRunError);
                    }

                    party_streams.push((party, stream))
                }

                let stream: StreamMulticaster<
                    Prin,
                    PartyStreamIdx,
                    Proto::Msg,
                    StreamSelector<
                        _,
                        FarChannelRegistryChannels<
                            Proto::Msg,
                            MsgCodec,
                            PullStreamsReporter<Proto::Msg, _, _, _>,
                            Channel,
                            F,
                            AuthN,
                            Xfrm
                        >,
                        Resolver,
                        Ctx
                    >,
                    Ctx
                > = StreamMulticaster::create(
                    party_streams.into_iter(),
                    slots_config
                );
                let parties = match stream.parties() {
                    Ok(parties) => {
                        StaticParties::from_parties(parties.map(|(idx, _)| idx))
                    }
                    // This is here as a placeholder; this type is
                    // uninhabited, and Rust > 1.81 clippy generates an error
                    // for this.
                    Err(_) => panic!("Impossible case!")
                };

                (stream, parties)
            }
        };

        let party_data = match stream.parties() {
            Ok(parties) => {
                let mut parties: Vec<(PartyStreamIdx, Prin)> =
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
            // This is here as a placeholder; this type is
            // uninhabited, and Rust > 1.81 clippy generates an error
            // for this.
            Err(_) => panic!("Impossible case!")
        };
        let party_iter = match stream.parties() {
            Ok(party_iter) => party_iter,
            // This is here as a placeholder; this type is
            // uninhabited, and Rust > 1.81 clippy generates an error
            // for this.
            Err(_) => panic!("Impossible case!")
        };
        let proto: SharedConsensusProto<
            Proto,
            RoundIDs,
            PartyStreamIdx,
            Prin,
            PrinCodec,
            StaticParties<PartyStreamIdx>
        > = match SharedConsensusProto::create(proto_config, prin_codec) {
            Ok(proto) => proto,
            Err(err) => {
                error!(target: "consensus-component",
                           "error creating consensus protocol: {}",
                           err);

                return Err(ConsensusComponentRunError);
            }
        };
        let rounds =
            match proto.rounds(round_ids, parties, self_party, &party_data) {
                Ok(proto) => proto,
                Err(err) => {
                    error!(target: "consensus-component",
                           "error creating consensus protocol rounds: {}",
                           err);

                    return Err(ConsensusComponentRunError);
                }
            };

        debug!(target: "consensus-component",
               "starting pull listener");

        let stream_reporter = stream.reporter(stream_reporter);
        let notify = Notify::new();
        let (state, round_reporter) =
            StateThread::create(notify.clone(), shutdown.clone());
        let state_join = state.start(rounds.clone());
        let pull_join = pull_listener.start(stream_reporter);
        let poll: PollThread<
            SharedRounds<Proto::Rounds, _, _, _, _, _>,
            _,
            _,
            StaticParties<PartyStreamIdx>,
            _,
            _,
            _,
            _,
            _
        > = PollThread::create(
            receiver,
            round_reporter,
            rounds.clone(),
            notify.clone(),
            party_iter,
            shutdown.clone()
        );
        let poll_join = poll.start();
        let sender: SendThread<
            SharedRounds<Proto::Rounds, _, _, _, _, _>,
            _,
            StaticParties<PartyStreamIdx>,
            _,
            _,
            _,
            _,
            _
        > = SendThread::create(
            ctx,
            rounds,
            notify.clone(),
            stream,
            shutdown.clone()
        );
        let notify = sender.notify();
        let sender_join = sender.start();

        Ok(ConsensusComponentCleanup {
            notify: notify,
            shutdown: shutdown,
            poll_join: poll_join,
            sender_join: sender_join,
            state_join: state_join,
            pull_join: pull_join
        })
    }
}

impl ConsensusComponentCleanup {
    pub fn cleanup(mut self) {
        self.shutdown.set();

        if let Err(err) = self.notify.notify() {
            error!(target: "consensus-component-cleanup",
                   "error notifying sender: {}",
                   err)
        }

        debug!(target: "consensus-component-cleanup",
               "joining sender");

        if self.sender_join.join().is_err() {
            error!(target: "consensus-component-cleanup",
                   "error joining sender")
        }

        debug!(target: "consensus-component-cleanup",
               "joining listener");

        if self.poll_join.join().is_err() {
            error!(target: "consensus-component-cleanup",
                   "error joining polling thread")
        }

        debug!(target: "consensus-component-cleanup",
               "joining pull streams");

        if self.pull_join.join().is_err() {
            error!(target: "consensus-component-cleanup",
                   "error joining pull streams listener")
        }

        debug!(target: "consensus-component-cleanup",
               "joining state thread");

        if self.state_join.join().is_err() {
            error!(target: "consensus-component-cleanup",
                   "error joining state thread")
        }
    }
}

#[cfg(feature = "standalone")]
pub struct StandaloneCreateCleanup {
    shutdown: ShutdownFlag,
    caches_join: JoinHandle<()>
}

#[cfg(feature = "standalone")]
pub type StandaloneRegistry = FarChannelRegistry<
    CompoundFarChannel,
    CompoundFarChannelThreadedFlows<
        UnixDatagramXfrm,
        UDPDatagramXfrm,
        FarChannelRegistryID
    >,
    Arc<TestAuthN<String, TestCred>>,
    CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>
>;

#[cfg(feature = "standalone")]
pub struct StandaloneCtx {
    caches: ThreadedNSNameCaches,
    registry: Arc<StandaloneRegistry>
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
impl
    FarChannelRegistryCtx<
        CompoundFarChannel,
        CompoundFarChannelThreadedFlows<
            UnixDatagramXfrm,
            UDPDatagramXfrm,
            FarChannelRegistryID
        >,
        Arc<TestAuthN<String, TestCred>>,
        CompoundFarChannelXfrm<UnixDatagramXfrm, UDPDatagramXfrm>
    > for StandaloneCtx
{
    #[inline]
    fn far_channel_registry(&mut self) -> Arc<StandaloneRegistry> {
        self.registry.clone()
    }
}

#[cfg(feature = "standalone")]
impl Standalone
    for CompoundConsensusComponent<
        StandaloneCtx,
        AscendingCount,
        PBFTProto<AscendingCount, String, StringPrincipalCodec>,
        PBFTMsgPERCodec,
        String,
        StringPrincipalCodec
    >
{
    type Config = StandaloneConfig;
    type CreateCleanup = StandaloneCreateCleanup;
    type RunCleanup = ConsensusComponentCleanup;
    type RunErrorCleanup = ();

    const COMPONENT_NAME: &'static str = "consensus";
    const CONFIG_FILES: &'static [&'static str] = &["consensus.conf"];

    fn create(
        config: Self::Config
    ) -> Result<(Self, Self::CreateCleanup), Self::CreateCleanup> {
        let (name_caches_config, registry_config, consensus_config) =
            config.take();
        let (mut caches, caches_join) =
            ThreadedNSNameCaches::create(name_caches_config);
        let shutdown = ShutdownFlag::new();
        let cleanup = StandaloneCreateCleanup {
            shutdown: shutdown.clone(),
            caches_join: caches_join
        };
        let (listener, reporter) = ThreadedFlowsListener::new();

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
                                CompoundEndpoint::Unix { unix } => {
                                    match UnixSocketAddr::try_from(unix) {
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
                                CompoundEndpoint::IP { ip } => match ip
                                    .ip_endpoint()
                                {
                                    IPEndpointAddr::Addr(addr) => {
                                        let addr =
                                            SocketAddr::new(*addr, ip.port());
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

        let authn = Arc::new(TestAuthN::create(authn_parties.into_iter()));

        match StandaloneRegistry::create(
            &mut caches,
            authn,
            reporter,
            registry_config
        ) {
            Ok(registry) => {
                let ctx = StandaloneCtx {
                    registry: Arc::new(registry),
                    caches: caches
                };
                let round_ids = AscendingCount { curr: 0 };
                let standalone = ConsensusComponent {
                    channel: PhantomData,
                    proto: PhantomData,
                    flow: PhantomData,
                    xfrm: PhantomData,
                    resolver: PhantomData,
                    round_ids: round_ids,
                    config: consensus_config,
                    shutdown: shutdown,
                    listener: listener,
                    ctx: ctx
                };

                Ok((standalone, cleanup))
            }
            Err(err) => {
                error!(target: "start",
                       "error creating channel registry: {}",
                       err);

                Err(cleanup)
            }
        }
    }

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
            cleanup.cleanup();
        }

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

impl Display for PartyStreamIdx {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        write!(f, "{}", self.0)
    }
}

impl DatagramCodec<String> for StringPrincipalCodec {
    type CreateError = Infallible;
    type DecodeError = Utf8Error;
    type EncodeError = Infallible;
    type Param = ();

    const MAX_BYTES: usize = 1024;

    #[inline]
    fn create(_param: Self::Param) -> Result<Self, Self::CreateError> {
        Ok(StringPrincipalCodec)
    }

    fn decode(
        &mut self,
        buf: &[u8]
    ) -> Result<(String, usize), Self::DecodeError> {
        let len = buf.len();
        let string = from_utf8(buf)?.to_string();

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

// ISSUE #2, ISSUE #6: Delete from here

impl Iterator for AscendingCount {
    type Item = u128;

    fn next(&mut self) -> Option<u128> {
        let out = self.curr;

        self.curr += 1;

        Some(out)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum TestCred {
    IP { addr: SocketAddr },
    Unix { addr: UnixSocketAddr }
}

impl<Basic> From<SSLCred<'_, CompoundFarCredential<'_, Basic>>> for TestCred
where
    TestCred: From<Basic>
{
    fn from(_val: SSLCred<'_, CompoundFarCredential<'_, Basic>>) -> TestCred {
        panic!("Not supported!")
    }
}

impl From<CompoundFarIPChannelXfrmPeerAddr> for TestCred {
    fn from(val: CompoundFarIPChannelXfrmPeerAddr) -> TestCred {
        match val {
            CompoundFarIPChannelXfrmPeerAddr::UDP { udp } => {
                TestCred::IP { addr: udp }
            }
            _ => panic!("Not supported!")
        }
    }
}

impl From<CompoundFarChannelXfrmPeerAddr> for TestCred {
    fn from(val: CompoundFarChannelXfrmPeerAddr) -> TestCred {
        match val {
            CompoundFarChannelXfrmPeerAddr::Unix { unix } => {
                TestCred::Unix { addr: unix }
            }
            CompoundFarChannelXfrmPeerAddr::IP { ip } => TestCred::from(ip)
        }
    }
}

impl<Basic> From<CompoundFarCredential<'_, Basic>> for TestCred
where
    TestCred: From<Basic>
{
    fn from(val: CompoundFarCredential<'_, Basic>) -> TestCred {
        match val {
            CompoundFarCredential::Basic { basic } => TestCred::from(basic),
            _ => panic!("Not supported!")
        }
    }
}

impl Display for TestCred {
    #[inline]
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            TestCred::IP { addr } => write!(f, "ip://{}", addr),
            TestCred::Unix { addr } => write!(f, "unix://{}", addr)
        }
    }
}

// ISSUE #2, ISSUE #6: to here
