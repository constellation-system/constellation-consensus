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

use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;

use constellation_auth::authn::AuthNed;
use constellation_common::codec::Decoder;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_component_common::PartyStreamIdx;
use constellation_component_common::bus::dispatch::SessionDispatchTypes;
use constellation_component_common::bus::dispatch::DispatchBusTypes;
use constellation_component_common::bus::multicast::MulticastBusTypes;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_consensus_common::oper::OperBatch;
use constellation_consensus_common::parties::PartyTypes;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::parties::RoundIDGenTypes;
use constellation_consensus_common::proto::ConsensusProto;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_consensus_common::proto::ConsensusProtoOutboundTypes;
use constellation_consensus_common::round::RoundsAdvance;
use constellation_consensus_common::round::RoundsRecv;
use constellation_consensus_common::round::RoundsSubmit;
use constellation_consensus_common::round::RoundsUpdate;
use constellation_consensus_common::state::RoundResultReporter;
use constellation_streams::large_obj::LargeObjID;

pub trait ConsensusBaseTypes {
    type HashID: Clone + Display + Hash + HashID + Eq + Send;
    type Hash: HashAlgo<HashID = Self::HashID>;
    type Oper: OperBatch<Self::Hash>;
    type Seal;
    type SealCodec: Clone + Decoder<Self::Seal> + Encoder<Self::Seal>
        + CreateWithParam<Self::SealCodecParam>;
    type SealCodecParam: Clone + Default;
}

pub trait ConsensusMsgTypes<IDTypes, ProtoTypes>: ConsensusBaseTypes
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> {
    type Prin: Display + Eq + Hash + Send;
    type AuthNMsg: AuthNed<Self::Prin, ProtoTypes::Msg>;
}

pub trait ConsensusStateTypes<IDTypes>: ConsensusBaseTypes
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> {
    type SubmitError: Display + ScopedError;
    type AdvanceError: Debug + Display;
    type Rounds: RoundsAdvance<
        IDTypes::RoundID,
        AdvanceError = Self::AdvanceError
    > + RoundsUpdate<Self::Oper>
        + RoundsSubmit<Self::HashID, SubmitError = Self::SubmitError>
        + Send;
}

pub trait ConsensusRecvTypes<IDTypes, ProtoTypes>:
    ConsensusMsgTypes<IDTypes, ProtoTypes>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>
{
    type SubmitError: Debug + Display + ScopedError;
    type Rounds: RoundsRecv<IDTypes, ProtoTypes, IDTypes::Oper>
        + RoundsSubmit<
            IDTypes::HashID,
            SubmitError = Self::SubmitError
        >
        + Send
        + Sync;
    type Reporter: RoundResultReporter<IDTypes::RoundID, Self::Oper>
        + Send
        + Sync;
}

pub trait ConsensusPeerTypes<IDTypes, ProtoTypes>:
    ConsensusMsgTypes<IDTypes, ProtoTypes>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> {
    type PeerPrin: Display + Eq + Hash + Send;
    type IDsConfig: Clone + Default;
    type IDs: Create<Config = Self::IDsConfig>
        + Iterator<Item = LargeObjID> + Send;
    type CtlAuthNMsg: AuthNed<
        ConsensusCtl<IDTypes::RoundID, Self::HashID, Self::Seal>,
        ProtoTypes::Msg
    >;
}

pub trait ConsensusPeerSessionDispatchTypes<IDTypes, ProtoTypes>:
    ConsensusPeerTypes<IDTypes, ProtoTypes>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> {
    type SessionDispTypes: SessionDispatchTypes<
        MsgPrin = Self::PeerPrin,
        InMsg = ConsensusCtl<IDTypes::RoundID, Self::HashID, Self::Seal>,
        OutMsg = ConsensusCtl<IDTypes::RoundID, Self::HashID, Self::Seal>,
        AuthNMsg = Self::CtlAuthNMsg
    >;
}

pub trait ConsensusTypes:
    ConsensusPeerSessionDispatchTypes<Self::IDTypes, Self::ProtoTypes> {
    type Ctx;
    type Addr: Clone + Debug + Display + Eq + Hash + Send;
    type Party: Clone + Display + Eq + Hash;
    type PartyCodecConfig: Default;
    type PartyCodec: Decoder<Self::Party>
        + Encoder<Self::Party>
        + Create<Config = Self::PartyCodecConfig>;
    type RoundID: From<u128> + Into<u128>;
    type RoundIDs: Iterator<Item = Self::RoundID>;
    type IDTypes: RoundIDGenTypes<
        PartyID = PartyStreamIdx,
        RoundID = Self::RoundID,
        RoundIDs = Self::RoundIDs
    > + PartyTypes;
    type ProtoTypes: ConsensusProtoOutboundTypes<Self::IDTypes>;
    type ConsensusEpochsConfig: Default;
    type ConsensusMsgAuthConfig;
    type ConsensusChansConfig;
    type ConsensusBusTypes: MulticastBusTypes<
        Self::Ctx,
        Addr = Self::Addr,
        MsgAuthConfig = Self::ConsensusMsgAuthConfig,
        ChansConfig = Self::ConsensusChansConfig,
        EpochsConfig = Self::ConsensusEpochsConfig
    >;
    type ConsensusConfig: Default;
    type Consensus: ConsensusProto<
        Self::ProtoTypes,
        Config = Self::ConsensusConfig
    >;

    type PeerChansConfig;
    type PeerEpochsConfig: Default;
    type PeerMsgAuthConfig;
    type PeerBusTypes: DispatchBusTypes<
        Self::Ctx,
        Addr = Self::Addr,
        SessionPrin = Self::PeerPrin,
        MsgPrin = Self::PeerPrin,
        InMsg = ConsensusCtl<Self::RoundID, Self::HashID, Self::Seal>,
        OutMsg = ConsensusCtl<Self::RoundID, Self::HashID, Self::Seal>,
        AuthNMsg = Self::CtlAuthNMsg,
        ChansConfig = Self::PeerChansConfig,
        MsgAuthConfig = Self::PeerMsgAuthConfig,
        EpochsConfig = Self::PeerEpochsConfig
    >;
}
