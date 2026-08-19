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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::error::MutexPoison;
use constellation_common::sync::Notify;
use constellation_component_common::PartyStreamIdx;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_consensus_common::round::RoundsRecv;
use log::error;
use log::warn;

use crate::state::State;
use crate::types::ConsensusRecvTypes;
use crate::types::ConsensusStateTypes;

pub(crate) struct ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    party_types: PhantomData<IDTypes>,
    proto_types: PhantomData<ProtoTypes>,
    types: PhantomData<Types>,
    prins: Arc<RwLock<HashMap<Types::Prin, PartyStreamIdx>>>,
    reporter: Types::Reporter,
    notify: Notify,
    state: Arc<State<IDTypes, Types>>
}

impl<IDTypes, ProtoTypes, Types> Clone
    for ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    #[inline]
    fn clone(&self) -> Self {
        ConsensusAuthNRecv {
            id_types: self.id_types,
            proto_types: self.proto_types,
            types: self.types,
            prins: self.prins.clone(),
            reporter: self.reporter.clone(),
            notify: self.notify.clone(),
            state: self.state.clone()
        }
    }
}

impl<IDTypes, ProtoTypes, Types>
    ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    #[inline]
    pub(crate) fn create(
        reporter: Types::Reporter,
        state: Arc<State<IDTypes, Types>>,
        notify: Notify
    ) -> Self {
        ConsensusAuthNRecv {
            oper: PhantomData,
            msg: PhantomData,
            round_ids: PhantomData,
            prins: Arc::new(RwLock::new(HashMap::new())),
            reporter: reporter,
            notify: notify,
            state: state
        }
    }

    #[inline]
    pub(crate) fn set_parties<I>(
        &mut self,
        prins: I
    ) -> Result<(), MutexPoison>
    where
        I: Iterator<Item = (PartyStreamIdx, Types::Prin)> {
        let mut guard = self.prins.write().map_err(|_| MutexPoison)?;

        *guard = prins.map(|(a, b)| (b, a)).collect();

        Ok(())
    }
}

impl<IDTypes, ProtoTypes, Types> Drop for
    ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    fn drop(&mut self) {
        if let Err(err) = self.notify.notify() {
            error!(target: "consensus-recv-authn-msg",
                   "error notifying sender: {}",
                   err);
        }
    }
}

impl<IDTypes, ProtoTypes, Types>
    AuthNMsgRecv<Types::Prin, ProtoTypes::Msg, Types::AuthNMsg>
    for ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>
        + ConsensusStateTypes<IDTypes>
{
    type RecvError = MutexPoison;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        prin: &Types::Prin,
        msg: ProtoTypes::Msg
    ) -> Result<(), Self::RecvError> {
        let guard = self.prins.read().map_err(|_| MutexPoison)?;

        match guard.get(prin) {
            Some(party) => {
                if let Err(err) =
                    self.state.recv(&mut self.reporter, party, msg)
                {
                    warn!(target: "consensus-recv-authn-msg",
                          "error receiving message from {}: {}",
                          prin, err)
                }

                self.notify.notify()
            }
            None => {
                warn!(target: "consensus-recv-authn-msg",
                      "discarding message from unknown principal {}",
                      prin);

                Ok(())
            }
        }
    }
}
