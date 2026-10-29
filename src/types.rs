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
use constellation_auth::authn::AuthNedDestruct;
use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::codec::Decoder;
use constellation_common::codec::Encoder;
use constellation_common::config::Create;
use constellation_common::config::CreateWithParam;
use constellation_common::error::ScopedError;
use constellation_common::hashid::HashAlgo;
use constellation_common::hashid::HashID;
use constellation_common::net::SharedMsgs;
use constellation_component_common::bus::types::SessionDispatchTypes;
use constellation_component_common::bus::types::DispatchLargeObjBusTypes;
use constellation_component_common::bus::types::MulticastDatagramBusTypes;
use constellation_component_common::consensus_ctl::ConsensusCtl;
use constellation_consensus_common::oper::OperBatch;
use constellation_consensus_common::parties::PartyTypes;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::parties::RoundPartyIdxTypes;
use constellation_consensus_common::parties::RoundIDGenTypes;
use constellation_consensus_common::proto::ConsensusProto;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_consensus_common::proto::ConsensusProtoOutboundTypes;
use constellation_consensus_common::round::RoundMsg;
use constellation_consensus_common::round::RoundsAdvance;
use constellation_consensus_common::round::RoundsRecv;
use constellation_consensus_common::round::RoundsSubmit;
use constellation_consensus_common::round::RoundsUpdate;
use constellation_streams::large_obj::LargeObjID;
use constellation_streams::multicast::MulticastStreamIdx;

use crate::peers::PeerSessionDispatch;
use crate::peers::PeerSessionMsgs;
use crate::peers::PeerSessionRecv;
use crate::recv::ConsensusAuthNRecv;
use crate::state::State;

pub trait ConsensusBaseTypes {
    type HashID: Clone + Debug + Display + Hash + HashID + Eq + Send;
    type HashConfig: Default;
    type HashCreateError: Debug + Display;
    type Hash: Clone + Create<Config = Self::HashConfig,
                              CreateError = Self::HashCreateError>
        + HashAlgo<HashID = Self::HashID> + Send;
    type BatchError: Debug + Display;
    type Oper: OperBatch<
            Self::Hash,
            BatchError = Self::BatchError
        > + Send;
    type Seal: Clone + Send;
    type SealCodec: Clone + Decoder<Self::Seal> + Encoder<Self::Seal>
        + CreateWithParam<Self::SealCodecParam>;
    type SealCodecParam: Clone + Default;
}

pub trait ConsensusMsgTypes<IDTypes, ProtoTypes>: ConsensusBaseTypes
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> {
    type Prin: Clone + Display + Eq + Hash + Send + Sync;
    type AuthNMsg: AuthNed<Self::Prin>
        + AuthNedDestruct<Self::Prin, ProtoTypes::Msg>;
}

pub trait ConsensusRecvTypes<IDTypes, ProtoTypes>:
    ConsensusMsgTypes<IDTypes, ProtoTypes>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>
{
    type SubmitError: Debug + Display + ScopedError;
    type AdvanceError: Debug + Display;
    type UpdateError: Debug + Display;
    type Rounds: RoundsRecv<IDTypes, ProtoTypes, Self::Oper>
      + RoundsAdvance<
        IDTypes::RoundID,
        AdvanceError = Self::AdvanceError
    > + RoundsUpdate<
        Self::Oper,
        UpdateError = Self::UpdateError
    > + RoundsSubmit<Self::HashID, SubmitError = Self::SubmitError>
        + SharedMsgs<MulticastStreamIdx, ProtoTypes::Msg>
        + Send;
}

pub trait ConsensusPeerTypes<IDTypes, ProtoTypes>:
    ConsensusMsgTypes<IDTypes, ProtoTypes>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> {
    type PeerPrin: Clone + Debug + Display + Eq + Hash + Send + Sync;
    type IDsCreateError: Debug + Display + ScopedError;
    type IDsConfig: Clone + Default + Send;
    type IDs: Create<Config = Self::IDsConfig>
        + Iterator<Item = LargeObjID> + Send;
    type CtlAuthNMsg: AuthNed<Self::PeerPrin>
        + AuthNedDestruct<
            Self::Prin,
            ConsensusCtl<IDTypes::RoundID, Self::HashID, Self::Seal>
        >;
    type CtlCodecCreateError: Debug + Display + ScopedError;
    type CtlCodec: Clone + Create<CreateError = Self::CtlCodecCreateError>
        + Decoder<ConsensusCtl<IDTypes::RoundID, Self::HashID, Self::Seal>>
        + Encoder<ConsensusCtl<IDTypes::RoundID, Self::HashID, Self::Seal>>;
}

pub trait ConsensusPeerSessionDispatchTypes<IDTypes, ProtoTypes>:
    ConsensusPeerTypes<IDTypes, ProtoTypes>
    + ConsensusRecvTypes<IDTypes, ProtoTypes> + Sized
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID> {
    type SessionDispTypes: SessionDispatchTypes<
        SessionPrin = Self::PeerPrin,
        Msgs = PeerSessionMsgs<IDTypes, ProtoTypes, Self>,
        Recv = PeerSessionRecv<IDTypes, ProtoTypes, Self>
    >;
}

pub trait ConsensusTypes:
    ConsensusPeerSessionDispatchTypes<Self::IDTypes, Self::ProtoTypes> {
    type Ctx: 'static + Clone + NSNameCachesCtx + Send;
    type Addr: Clone + Debug + Display + Eq + Hash + Send;
    type PrinCodecConfig: Default;
    type PrinCodec: Decoder<Self::Prin>
        + Encoder<Self::Prin>
        + Create<Config = Self::PrinCodecConfig>;
    type RoundID: Clone + Debug + Display + From<u128> + Into<u128> + Ord + Send;
    type RoundIDsConfig: Default;
    type RoundIDsCreateError: Debug + Display;
    type RoundIDs: Iterator<Item = Self::RoundID>
        + Create<Config = Self::RoundIDsConfig,
                 CreateError = Self::RoundIDsCreateError>;
    type IDTypes: RoundIDGenTypes<
        PartyID = MulticastStreamIdx,
        RoundID = Self::RoundID,
        RoundIDs = Self::RoundIDs
    > + RoundPartyIdxTypes + PartyTypes + Send;
    type Msg: Clone + Debug + RoundMsg<Self::RoundID> + Send;
    type ProtoTypes: ConsensusProtoOutboundTypes<
        Self::IDTypes,
        Msg = Self::Msg
    > + Send;
    type ConsensusEpochsConfig: Clone + Default + Send;
    type ConsensusSessAuthConfig: Clone + Send;
    type ConsensusMsgAuthConfig: Clone + Send;
    type ConsensusUDPConfig: Clone + Default + Send;
    type ConsensusUnixConfig: Clone + Default + Send;
    type ConsensusChansConfig: Clone + Send;
    type ConsensusBusTypes: MulticastDatagramBusTypes<
        Self::Ctx,
        // XXX This might be overly restrictive
        MsgPrin = Self::Prin,
        SessionPrin = Self::Prin,
        OutMsg = Self::Msg,
        AuthNMsg = Self::AuthNMsg,
        MsgAuthConfig = Self::ConsensusMsgAuthConfig,
        EpochsConfig = Self::ConsensusEpochsConfig,
        ResolverConfig = Self::ResolverConfig,
        EncoderConfig = Self::ConsensusEncoderConfig,
        DecoderConfig = Self::ConsensusDecoderConfig,
        UDPConfig = Self::ConsensusUDPConfig,
        UnixConfig = Self::ConsensusUnixConfig,
        SessionAuthConfig = Self::ConsensusSessAuthConfig,
        Recv = ConsensusAuthNRecv<Self::IDTypes, Self::ProtoTypes, Self>,
        Msgs = State<Self::IDTypes, Self::ProtoTypes, Self>
    >;
    type ConsensusEncoderConfig: Clone + Default + Send;
    type ConsensusDecoderConfig: Clone + Default + Send;
    type ConsensusConfig: Clone + Default + Send;
    type ConsensusCreateError: Debug + Display;
    type ConsensusRoundsError: Debug + Display;
    type Consensus: ConsensusProto<
        Self::IDTypes,
        Config = Self::ConsensusConfig,
        Rounds = Self::Rounds,
        CreateError = Self::ConsensusCreateError,
        RoundsError = Self::ConsensusRoundsError
    >;
    type ResolverConfig: Clone + Default + Send;
    type PeerHashConfig: Clone + Default + Send;
    type PeerEpochsConfig: Clone + Default + Send;
    type PeerProtoAuthConfig: Clone + Send;
    type PeerMsgAuthConfig: Clone + Send;
    type PeerSessAuthConfig: Clone + Send;
    type PeerUDPConfig: Clone + Default + Send;
    type PeerUnixConfig: Clone + Default + Send;
    type PeerBusTypes: DispatchLargeObjBusTypes<
        Self::Ctx,
        SessionPrin = Self::PeerPrin,
        MsgPrin = Self::PeerPrin,
        InMsg = ConsensusCtl<Self::RoundID, Self::HashID, Self::Seal>,
        OutMsg = ConsensusCtl<Self::RoundID, Self::HashID, Self::Seal>,
        EncoderConfig = (),
        DecoderConfig = (),
        AuthNMsg = Self::CtlAuthNMsg,
        IDsConfig = Self::IDsConfig,
        HashConfig = Self::PeerHashConfig,
        UDPConfig = Self::PeerUDPConfig,
        UnixConfig = Self::PeerUnixConfig,
        EpochsConfig = Self::PeerEpochsConfig,
        MsgAuthConfig = Self::PeerMsgAuthConfig,
        SessionAuthConfig = Self::PeerSessAuthConfig,
        LargeObjMsgAuthConfig = Self::PeerProtoAuthConfig,
        DispTypes = PeerSessionDispatch<Self::IDTypes, Self::ProtoTypes, Self>
    >;
}
