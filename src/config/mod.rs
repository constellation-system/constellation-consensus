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

#[cfg(feature = "standalone")]
use constellation_channels::config::ChannelRegistryChannelsConfig;
use constellation_channels::config::ChannelRegistryConfig;
use constellation_channels::config::CompoundEndpoint;
use constellation_channels::config::CompoundFarChannelConfig;
use constellation_channels::config::CompoundXfrmCreateParam;
use constellation_channels::config::ResolverConfig;
use constellation_channels::config::ThreadedFlowsParams;
use constellation_channels::config::ThreadedNSNameCachesConfig;
#[cfg(feature = "standalone")]
use constellation_common::codec::DatagramCodec;
#[cfg(feature = "standalone")]
use constellation_pbft::config::PBFTConfig;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PBFTMsgPERCodec;
#[cfg(feature = "standalone")]
use constellation_pbft::msgs::PbftMsg;
use constellation_streams::config::BatchSlotsConfig;
use constellation_streams::config::PartyConfig;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "consensus-pool")]
#[serde(rename_all = "kebab-case")]
pub struct ConsensusConfig<PartyID, PartyCodec, Proto, Channels, Endpoint>
where
    PartyCodec: Default,
    Channels: Default,
    Proto: Default {
    #[serde(default)]
    proto: Proto,
    #[serde(flatten)]
    multicast: MulticastConfig<PartyID, PartyCodec, Channels, Endpoint>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "multicast")]
#[serde(rename_all = "kebab-case")]
pub struct MulticastConfig<PartyID, PartyCodec, Channels, Endpoint>
where
    PartyCodec: Default,
    Channels: Default {
    #[serde(flatten)]
    #[serde(default)]
    slots: BatchSlotsConfig,
    /// Party identitfying this node.
    #[serde(rename = "self")]
    self_party: PartyID,
    #[serde(default)]
    party_codec: PartyCodec,
    #[serde(flatten)]
    parties: PartiesConfig<PartyID, Channels, Endpoint>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(rename = "static-parties")]
#[serde(rename_all = "kebab-case")]
pub struct StaticPartyConfig<PartyID, Channels, Endpoint>
where
    Channels: Default {
    party: PartyID,
    #[serde(flatten)]
    config: PartyConfig<ResolverConfig, Channels, String, Endpoint>
}

#[derive(Clone, Debug, Deserialize, PartialEq, PartialOrd, Serialize)]
#[serde(untagged)]
pub enum PartiesConfig<PartyID, Channels, Endpoint>
where
    Channels: Default {
    Static {
        #[serde(rename = "static")]
        stat: Vec<StaticPartyConfig<PartyID, Channels, Endpoint>>
    }
}
impl<PartyID, PartyCodec, Channels, Endpoint>
    MulticastConfig<PartyID, PartyCodec, Channels, Endpoint>
where
    PartyCodec: Default,
    Channels: Default
{
    #[inline]
    pub fn create(
        self_party: PartyID,
        party_codec: PartyCodec,
        slots: BatchSlotsConfig,
        parties: PartiesConfig<PartyID, Channels, Endpoint>
    ) -> MulticastConfig<PartyID, PartyCodec, Channels, Endpoint> {
        MulticastConfig {
            party_codec: party_codec,
            self_party: self_party,
            slots: slots,
            parties: parties
        }
    }

    #[inline]
    pub fn parties(&self) -> &PartiesConfig<PartyID, Channels, Endpoint> {
        &self.parties
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        PartyID,
        PartyCodec,
        BatchSlotsConfig,
        PartiesConfig<PartyID, Channels, Endpoint>
    ) {
        (self.self_party, self.party_codec, self.slots, self.parties)
    }
}

impl<PartyID, Channels, Endpoint> StaticPartyConfig<PartyID, Channels, Endpoint>
where
    Channels: Default
{
    #[inline]
    pub fn create(
        party: PartyID,
        config: PartyConfig<ResolverConfig, Channels, String, Endpoint>
    ) -> StaticPartyConfig<PartyID, Channels, Endpoint> {
        StaticPartyConfig {
            party: party,
            config: config
        }
    }

    #[inline]
    pub fn party(&self) -> &PartyID {
        &self.party
    }

    #[inline]
    pub fn party_config(
        &self
    ) -> &PartyConfig<ResolverConfig, Channels, String, Endpoint> {
        &self.config
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        PartyID,
        PartyConfig<ResolverConfig, Channels, String, Endpoint>
    ) {
        (self.party, self.config)
    }
}

impl<PartyID, PartyCodec, Proto, Channels, Endpoint>
    ConsensusConfig<PartyID, PartyCodec, Proto, Channels, Endpoint>
where
    PartyCodec: Default,
    Channels: Default,
    Proto: Default
{
    #[inline]
    pub fn multicast(
        &self
    ) -> &MulticastConfig<PartyID, PartyCodec, Channels, Endpoint> {
        &self.multicast
    }

    #[inline]
    pub fn proto(&self) -> &Proto {
        &self.proto
    }

    #[inline]
    pub fn take(
        self
    ) -> (
        Proto,
        MulticastConfig<PartyID, PartyCodec, Channels, Endpoint>
    ) {
        (self.proto, self.multicast)
    }
}

pub type RegistryConfig = ChannelRegistryConfig<
    CompoundFarChannelConfig,
    ThreadedFlowsParams,
    CompoundXfrmCreateParam<(), ()>
>;

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
    /// Channel registry configuration.
    #[serde(flatten)]
    registry: RegistryConfig,
    /// Core consensus engine configuration.
    consensus: ConsensusConfig<
        String,
        (),
        PBFTConfig,
        ChannelRegistryChannelsConfig<
            <PBFTMsgPERCodec as DatagramCodec<PbftMsg>>::Param
        >,
        CompoundEndpoint
    >
}

#[cfg(feature = "standalone")]
impl StandaloneConfig {
    #[inline]
    pub fn consensus(
        &self
    ) -> &ConsensusConfig<
        String,
        (),
        PBFTConfig,
        ChannelRegistryChannelsConfig<
            <PBFTMsgPERCodec as DatagramCodec<PbftMsg>>::Param
        >,
        CompoundEndpoint
    > {
        &self.consensus
    }

    /// Decompose this `StandaloneConfig` into its components.
    #[inline]
    pub fn take(
        self
    ) -> (
        ThreadedNSNameCachesConfig,
        RegistryConfig,
        ConsensusConfig<
            String,
            (),
            PBFTConfig,
            ChannelRegistryChannelsConfig<
                <PBFTMsgPERCodec as DatagramCodec<PbftMsg>>::Param
            >,
            CompoundEndpoint
        >
    ) {
        (self.name_caches, self.registry, self.consensus)
    }
}

#[cfg(test)]
use constellation_common::net::IPEndpoint;
#[cfg(test)]
use constellation_common::net::IPEndpointAddr;
#[cfg(test)]
use constellation_common::retry::Retry;
#[cfg(test)]
use constellation_streams::config::ConnectionConfig;
#[cfg(test)]
use constellation_streams::config::FarSchedulerConfig;

#[test]
fn test_connection_config() {
    let yaml = concat!(
        "channel-names:\n",
        "  - \"chan-1\"\n",
        "  - \"chan-2\"\n",
        "endpoints:\n",
        "  - ip:\n",
        "      addr: localhost\n",
        "      port: 10000\n",
    );
    let channames = vec!["chan-1", "chan-2"];
    let endpoint = CompoundEndpoint::IP {
        ip: IPEndpoint::new(IPEndpointAddr::name(String::from("localhost")),
                            10000)
    };
    let endpoints = vec![endpoint];
    let expected = ConnectionConfig::new((), channames, endpoints);
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn test_party_config() {
    let yaml = concat!(
        "retry:\n",
        "  factor: 100\n",
        "  exp-base: 2.0\n",
        "  exp-factor: 1.0\n",
        "  exp-rounds-cap: 20\n",
        "  linear-factor: 1.0\n",
        "  linear-rounds-cap: 50\n",
        "  max-random: 100\n",
        "  addend: 50\n",
        "connections:\n",
        "  - channel-names:\n",
        "      - \"chan-1\"\n",
        "      - \"chan-2\"\n",
        "    endpoints:\n",
        "      - ip:\n",
        "          addr: localhost\n",
        "          port: 10000\n",
        "  - channel-names:\n",
        "      - \"chan-2\"\n",
        "    endpoints:\n",
        "      - ip:\n",
        "          addr: localhost\n",
        "          port: 9999\n",
    );
    let retry = Retry::new(100, 2.0, 1.0, 20, 1.0, Some(50), 100, 50);
    let addr_10000 = CompoundEndpoint::IP {
        ip: IPEndpoint::new(IPEndpointAddr::name(String::from("localhost")),
                            10000)
    };
    let channames_10000 = vec!["chan-1", "chan-2"];
    let endpoints_10000 = vec![addr_10000];
    let connection_10000 = ConnectionConfig::new(
        (), channames_10000, endpoints_10000
    );
    let addr_9999 = CompoundEndpoint::IP {
        ip: IPEndpoint::new(IPEndpointAddr::name(String::from("localhost")),
                            9999)
    };
    let channames_9999 = vec!["chan-2"];
    let endpoints_9999 = vec![addr_9999];
    let connection_9999 = ConnectionConfig::new(
        (), channames_9999, endpoints_9999
    );
    let connections = vec![connection_10000, connection_9999];
    let expected = PartyConfig::new(
        FarSchedulerConfig::default(), (), retry, connections, None
    );
    let actual = serde_yaml::from_str(yaml).unwrap();

    assert_eq!(expected, actual);
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
