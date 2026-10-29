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

use constellation_auth::config::TestAuthNConfig;
use constellation_auth::config::TestCredConfig;
use constellation_channels::config::CompoundFarChannelConfig;
use constellation_channels::config::CompoundFarEndpoint;
use constellation_channels::config::CompoundXfrmCreateParam;
use constellation_channels::config::ThreadedFlowsParams;
use constellation_channels::config::ThreadedNSNameCachesConfig;
#[cfg(feature = "standalone")]
use constellation_common::codec::Decoder;
#[cfg(feature = "standalone")]
use constellation_common::codec::Encoder;
#[cfg(feature = "standalone")]
use constellation_common::ids::AscendingCount;
use constellation_component_common::config::DispatchLargeObjBusConfig;
use constellation_component_common::config::MulticastDatagramBusConfig;
#[cfg(feature = "standalone")]
use constellation_pbft::config::PBFTConfig;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PBFTMsgPERCodec;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PbftMsg;
use constellation_streams::config::LargeObjProtoConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename = "consensus-component")]
#[serde(rename_all = "kebab-case")]
pub struct ConsensusComponentConfig<
    RoundIDs,
    Proto,
    PartyID,
    Hash,
    ConsensusAuthN,
    ConsensusUnix,
    ConsensusUDP,
    ConsensusEnc,
    ConsensusDec,
    ConsensusResolver,
    ConsensusEpochs,
    ConsensusMsgAuthN,
    PeerHash,
    PeerSessAuth,
    PeerMsgAuth,
    PeerProtoAuth,
    PeerUnix,
    PeerUDP,
    PeerIDs,
    PeerEpochs
> where
    RoundIDs: Default,
    Proto: Default,
    Hash: Default,
    ConsensusResolver: Default,
    ConsensusEpochs: Default,
    ConsensusUnix: Default,
    ConsensusUDP: Default,
    ConsensusEnc: Default,
    ConsensusDec: Default,
    PeerHash: Default,
    PeerUnix: Default,
    PeerUDP: Default,
    PeerIDs: Default,
    PeerEpochs: Default {
    /// Channel registry configuration.
    #[serde(default)]
    round_ids: RoundIDs,
    #[serde(default)]
    hash: Hash,
    #[serde(default)]
    proto: Proto,
    #[serde(flatten)]
    multicast: MulticastDatagramBusConfig<
        PartyID,
        ConsensusAuthN,
        ConsensusUnix,
        ConsensusUDP,
        ConsensusEnc,
        ConsensusDec,
        ConsensusResolver,
        ConsensusEpochs,
        ConsensusMsgAuthN,
    >,
    peers: DispatchLargeObjBusConfig<
        PeerHash,
        PeerSessAuth,
        PeerMsgAuth,
        PeerProtoAuth,
        PeerUnix,
        PeerUDP,
        (),
        (),
        PeerIDs,
        PeerEpochs
    >
}

impl<
    RoundIDs,
    Proto,
    PartyID,
    Hash,
    ConsensusAuthN,
    ConsensusUnix,
    ConsensusUDP,
    ConsensusEnc,
    ConsensusDec,
    ConsensusResolver,
    ConsensusEpochs,
    ConsensusMsgAuthN,
    PeerHash,
    PeerSessAuth,
    PeerMsgAuth,
    PeerProtoAuth,
    PeerUnix,
    PeerUDP,
    PeerIDs,
    PeerEpochs
> ConsensusComponentConfig<
    RoundIDs,
    Proto,
    PartyID,
    Hash,
    ConsensusAuthN,
    ConsensusUnix,
    ConsensusUDP,
    ConsensusEnc,
    ConsensusDec,
    ConsensusResolver,
    ConsensusEpochs,
    ConsensusMsgAuthN,
    PeerHash,
    PeerSessAuth,
    PeerMsgAuth,
    PeerProtoAuth,
    PeerUnix,
    PeerUDP,
    PeerIDs,
    PeerEpochs
> where
    RoundIDs: Default,
    Proto: Default,
    Hash: Default,
    ConsensusResolver: Default,
    ConsensusEpochs: Default,
    ConsensusUnix: Default,
    ConsensusUDP: Default,
    ConsensusEnc: Default,
    ConsensusDec: Default,
    PeerHash: Default,
    PeerUnix: Default,
    PeerUDP: Default,
    PeerIDs: Default,
    PeerEpochs: Default {
    #[inline]
    pub fn multicast(
        &self
    ) -> &MulticastDatagramBusConfig<
        PartyID,
        ConsensusAuthN,
        ConsensusUnix,
        ConsensusUDP,
        ConsensusEnc,
        ConsensusDec,
        ConsensusResolver,
        ConsensusEpochs,
        ConsensusMsgAuthN,
    > {
        &self.multicast
    }

    #[inline]
    pub fn round_ids(&self) -> &RoundIDs {
        &self.round_ids
    }

    #[inline]
    pub fn proto(&self) -> &Proto {
        &self.proto
    }

    #[inline]
    pub fn hash(&self) -> &Hash {
        &self.hash
    }

    #[inline]
    pub fn peers(
        &self
    ) -> &DispatchLargeObjBusConfig<
        PeerHash,
        PeerSessAuth,
        PeerMsgAuth,
        PeerProtoAuth,
        PeerUnix,
        PeerUDP,
        (),
        (),
        PeerIDs,
        PeerEpochs
    > {
        &self.peers
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        RoundIDs,
        Hash,
        Proto,
        MulticastDatagramBusConfig<
            PartyID,
            ConsensusAuthN,
            ConsensusUnix,
            ConsensusUDP,
            ConsensusEnc,
            ConsensusDec,
            ConsensusResolver,
            ConsensusEpochs,
            ConsensusMsgAuthN,
        >,
        DispatchLargeObjBusConfig<
            PeerHash,
            PeerSessAuth,
            PeerMsgAuth,
            PeerProtoAuth,
            PeerUnix,
            PeerUDP,
            (),
            (),
            PeerIDs,
            PeerEpochs
        >
    ) {
        (self.round_ids, self.hash, self.proto, self.multicast, self.peers)
    }
}

/*
/// Top-level master configuration object.
///
/// Most configuration objects should be contained in
/// [ConsensusConfig]; however, there are additional configurations
/// that need to exist in a standalone instance, such as
/// [NameCachesRefreshConfig].
#[cfg(feature = "standalone")]
#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "consensus-config")]
#[serde(rename_all = "kebab-case")]
pub struct StandaloneConfig {
    /// Name cache configuration.
    #[serde(default)]
    name_caches: ThreadedNSNameCachesConfig,
    /// Core consensus engine configuration.
    consensus: ConsensusComponentConfig<
        String,
        (),
        PBFTConfig,
        CompoundFarChannelConfig,
        ThreadedFlowsParams,
        ChannelRegistryChannelsConfig<
            <PBFTMsgPERCodec as Codec<PbftMsg>>::Param
        >,
        <AscendingCount<u128> as IDGen>::Config,
        LargeObjProtoConfig<(), ()>,
        TestAuthNConfig<String, TestCredConfig>,
        CompoundXfrmCreateParam<(), ()>,
        CompoundFarEndpoint
    >
}

#[cfg(feature = "standalone")]
impl StandaloneConfig {
    #[inline]
    pub fn consensus(
        &self
    ) -> &ConsensusComponentConfig<
        String,
        (),
        PBFTConfig,
        CompoundFarChannelConfig,
        ThreadedFlowsParams,
        ChannelRegistryChannelsConfig<
            <PBFTMsgPERCodec as Codec<PbftMsg>>::Param
        >,
        <AscendingCount<u128> as IDGen>::Config,
        LargeObjProtoConfig<(), ()>,
        TestAuthNConfig<String, TestCredConfig>,
        CompoundXfrmCreateParam<(), ()>,
        CompoundFarEndpoint
    > {
        &self.consensus
    }

    /// Decompose this `StandaloneConfig` into its components.
    #[inline]
    pub fn take(
        self
    ) -> (
        ThreadedNSNameCachesConfig,
        ConsensusComponentConfig<
            String,
            (),
            PBFTConfig,
            CompoundFarChannelConfig,
            ThreadedFlowsParams,
            ChannelRegistryChannelsConfig<
                <PBFTMsgPERCodec as Codec<PbftMsg>>::Param
            >,
            <AscendingCount<u128> as IDGen>::Config,
            LargeObjProtoConfig<(), ()>,
            TestAuthNConfig<String, TestCredConfig>,
            CompoundXfrmCreateParam<(), ()>,
            CompoundFarEndpoint
        >
    ) {
        (self.name_caches, self.consensus)
    }
}

// #[test]
// fn test_party_config() {
// let yaml = concat!(
// "retry:\n",
// "  factor: 100\n",
// "  exp-base: 2.0\n",
// "  exp-factor: 1.0\n",
// "  exp-rounds-cap: 20\n",
// "  linear-factor: 1.0\n",
// "  linear-rounds-cap: 50\n",
// "  max-random: 100\n",
// "  addend: 50\n",
// "udp:\n",
// "  addr: 10.10.10.10\n",
// "  port: 10000\n"
// );
// let retry = Retry::new(100, 2.0, 1.0, 20, 1.0, Some(50), 100, 50);
// let addr: SocketAddr = "10.10.10.10:10000".parse().unwrap();
// let udp = UDPFarChannelConfig::new(addr.ip(), addr.port());
// let endpoint = CompoundFarChannelConfig::UDP { udp: udp };
// let expected = PartyConfig {
// retry: retry,
// endpoint: endpoint
// };
// let actual = serde_yaml::from_str(yaml).unwrap();
//
// assert_eq!(expected, actual);
// }
//
// #[test]
// fn test_static_parties_config() {
// let yaml = concat!(
// "parties:\n",
// "  - udp:\n",
// "      addr: 10.10.10.10\n",
// "      port: 10000\n",
// "    retry:\n",
// "      factor: 100\n",
// "      exp-base: 2.0\n",
// "      exp-factor: 1.0\n",
// "      exp-rounds-cap: 20\n",
// "      linear-factor: 1.0\n",
// "      linear-rounds-cap: 50\n",
// "      max-random: 100\n",
// "      addend: 50\n",
// "  - unix:\n",
// "      path: /var/run/test/test.sock"
// );
// let retry = Retry::new(100, 2.0, 1.0, 20, 1.0, Some(50), 100, 50);
// let addr: SocketAddr = "10.10.10.10:10000".parse().unwrap();
// let udp = UDPFarChannelConfig::new(addr.ip(), addr.port());
// let endpoint = CompoundFarChannelConfig::UDP { udp: udp };
// let party_1 = PartyConfig {
// retry: retry,
// endpoint: endpoint
// };
// let path = PathBuf::from("/var/run/test/test.sock");
// let unix = UnixFarChannelConfig::new(path);
// let endpoint = CompoundFarChannelConfig::Unix { unix: unix };
// let party_2 = PartyConfig {
// retry: Retry::default(),
// endpoint: endpoint
// };
// let expected = StaticPartiesConfig {
// parties: vec![party_1, party_2]
// };
// let actual = serde_yaml::from_str(yaml).unwrap();
//
// assert_eq!(expected, actual);
// }
// #[test]
// fn test_parties_static_parties_config() {
// let yaml = concat!(
// "static:\n",
// "  parties:\n",
// "    - udp:\n",
// "        addr: 10.10.10.10\n",
// "        port: 10000\n",
// "      retry:\n",
// "        factor: 100\n",
// "        exp-base: 2.0\n",
// "        exp-factor: 1.0\n",
// "        exp-rounds-cap: 20\n",
// "        linear-factor: 1.0\n",
// "        linear-rounds-cap: 50\n",
// "        max-random: 100\n",
// "        addend: 50\n",
// "    - unix:\n",
// "        path: /var/run/test/test.sock"
// );
// let retry = Retry::new(100, 2.0, 1.0, 20, 1.0, Some(50), 100, 50);
// let addr: SocketAddr = "10.10.10.10:10000".parse().unwrap();
// let udp = UDPFarChannelConfig::new(addr.ip(), addr.port());
// let endpoint = CompoundFarChannelConfig::UDP { udp: udp };
// let party_1 = PartyConfig {
// retry: retry,
// endpoint: endpoint
// };
// let path = PathBuf::from("/var/run/test/test.sock");
// let unix = UnixFarChannelConfig::new(path);
// let endpoint = CompoundFarChannelConfig::Unix { unix: unix };
// let party_2 = PartyConfig {
// retry: Retry::default(),
// endpoint: endpoint
// };
// let parties = StaticPartiesConfig {
// parties: vec![party_1, party_2]
// };
// let expected = PartiesConfig::Static { parties: parties };
// let actual = serde_yaml::from_str(yaml).unwrap();
//
// assert_eq!(expected, actual);
// }
*/
