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
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::mpsc::Sender;
use std::sync::mpsc::SendError;

use constellation_auth::authn::AuthNedDestruct;
use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::error::ErrorScope;
use constellation_common::error::MutexPoison;
use constellation_common::error::ScopedError;
use constellation_common::sync::Notify;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_streams::multicast::MulticastStreamIdx;
use log::error;
use log::warn;

use crate::types::ConsensusRecvTypes;

pub struct ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    party_types: PhantomData<IDTypes>,
    proto_types: PhantomData<ProtoTypes>,
    types: PhantomData<Types>,
    prins: Arc<RwLock<HashMap<Types::Prin, MulticastStreamIdx>>>,
    send: Sender<(MulticastStreamIdx, ProtoTypes::Msg)>,
    notify: Notify,
}

#[derive(Debug)]
pub enum RecvMsgError<Msg> {
    Send {
        err: SendError<Msg>
    },
    Notify {
        err: Error
    },
    MutexPoison
}

impl<IDTypes, ProtoTypes, Types> Clone
    for ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    #[inline]
    fn clone(&self) -> Self {
        ConsensusAuthNRecv {
            party_types: self.party_types,
            proto_types: self.proto_types,
            types: self.types,
            prins: self.prins.clone(),
            notify: self.notify.clone(),
            send: self.send.clone()
        }
    }
}

impl<IDTypes, ProtoTypes, Types>
    ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    #[inline]
    pub(crate) fn create(
        send: Sender<(MulticastStreamIdx, ProtoTypes::Msg)>,
        notify: Notify
    ) -> Self {
        ConsensusAuthNRecv {
            party_types: PhantomData,
            proto_types: PhantomData,
            types: PhantomData,
            prins: Arc::new(RwLock::new(HashMap::new())),
            notify: notify,
            send: send
        }
    }

    #[inline]
    pub(crate) fn set_parties<I>(
        &mut self,
        prins: I
    ) -> Result<(), MutexPoison>
    where
        I: Iterator<Item = (MulticastStreamIdx, Types::Prin)> {
        let mut guard = self.prins.write().map_err(|_| MutexPoison)?;

        *guard = prins.map(|(a, b)| (b, a)).collect();

        Ok(())
    }
}

impl<IDTypes, ProtoTypes, Types> Drop for
    ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    fn drop(&mut self) {
        if let Err(err) = self.notify.notify() {
            error!(target: "consensus-recv-authn-msg",
                   "error notifying sender: {}",
                   err);
        }
    }
}

impl<IDTypes, ProtoTypes, Types>
    AuthNMsgRecv<Types::Prin, Types::AuthNMsg>
    for ConsensusAuthNRecv<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    ProtoTypes::Msg: Debug,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    type RecvError = RecvMsgError<(MulticastStreamIdx, ProtoTypes::Msg)>;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        msg: Types::AuthNMsg
    ) -> Result<(), Self::RecvError> {
        let guard = self.prins.read().map_err(|_| RecvMsgError::MutexPoison)?;
        let (prin, msg) = msg.take();

        match guard.get(&prin) {
            Some(party) => {
                self.send.send((party.clone(), msg))
                    .map_err(|err| RecvMsgError::Send { err: err })?;
                self.notify.notify()
                    .map_err(|err| RecvMsgError::Notify { err: err })
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

impl<Msg> ScopedError for RecvMsgError<Msg> {
    fn scope(&self) -> ErrorScope {
        match self {
            RecvMsgError::Send { err } => err.scope(),
            RecvMsgError::Notify { err } => err.scope(),
            RecvMsgError::MutexPoison => ErrorScope::Unrecoverable
        }
    }
}

impl<Msg> Display for RecvMsgError<Msg> {
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), std::fmt::Error> {
        match self {
            RecvMsgError::Send { err } => write!(f, "{}", err),
            RecvMsgError::Notify { err } => write!(f, "{}", err),
            RecvMsgError::MutexPoison => write!(f, "mutex poisoned"),
        }
    }
}
