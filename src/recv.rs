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

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::RwLock;

use constellation_auth::authn::AuthNMsgRecv;
use constellation_common::error::MutexPoison;
use constellation_common::sync::Notify;
use constellation_consensus_common::round::RoundMsg;
use constellation_consensus_common::round::RoundsRecv;
use constellation_consensus_common::state::RoundResultReporter;
use log::error;
use log::warn;

use crate::component::PartyStreamIdx;

pub(crate) struct ConsensusAuthNRecv<R, Reporter, RoundID, Prin, Oper, Msg>
where
    R: RoundsRecv<RoundID, PartyStreamIdx, Oper, Msg> + Send + Sync,
    Reporter: RoundResultReporter<RoundID, Oper> + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Prin: Display + Eq + Hash + Send,
    Msg: RoundMsg<RoundID> + Send {
    oper: PhantomData<Oper>,
    msg: PhantomData<Msg>,
    round_ids: PhantomData<RoundID>,
    prins: Arc<RwLock<HashMap<Prin, PartyStreamIdx>>>,
    reporter: Reporter,
    notify: Notify,
    rounds: R
}

unsafe impl<R, Reporter, RoundID, Prin, Oper, Msg> Send
    for ConsensusAuthNRecv<R, Reporter, RoundID, Prin, Oper, Msg>
where
    R: RoundsRecv<RoundID, PartyStreamIdx, Oper, Msg> + Send + Sync,
    Reporter: RoundResultReporter<RoundID, Oper> + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Prin: Display + Eq + Hash + Send,
    Msg: RoundMsg<RoundID> + Send
{
}

unsafe impl<R, Reporter, RoundID, Prin, Oper, Msg> Sync
    for ConsensusAuthNRecv<R, Reporter, RoundID, Prin, Oper, Msg>
where
    R: RoundsRecv<RoundID, PartyStreamIdx, Oper, Msg> + Send + Sync,
    Reporter: RoundResultReporter<RoundID, Oper> + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Prin: Display + Eq + Hash + Send,
    Msg: RoundMsg<RoundID> + Send
{
}

impl<R, Reporter, RoundID, Prin, Oper, Msg> Clone
    for ConsensusAuthNRecv<R, Reporter, RoundID, Prin, Oper, Msg>
where
    R: Clone + RoundsRecv<RoundID, PartyStreamIdx, Oper, Msg> + Send + Sync,
    Reporter: Clone + RoundResultReporter<RoundID, Oper> + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Prin: Display + Eq + Hash + Send,
    Msg: RoundMsg<RoundID> + Send
{
    #[inline]
    fn clone(&self) -> Self {
        ConsensusAuthNRecv {
            oper: self.oper,
            msg: self.msg,
            round_ids: self.round_ids,
            prins: self.prins.clone(),
            reporter: self.reporter.clone(),
            notify: self.notify.clone(),
            rounds: self.rounds.clone()
        }
    }
}

impl<R, Reporter, RoundID, Prin, Oper, Msg>
    ConsensusAuthNRecv<R, Reporter, RoundID, Prin, Oper, Msg>
where
    R: RoundsRecv<RoundID, PartyStreamIdx, Oper, Msg> + Send + Sync,
    Reporter: RoundResultReporter<RoundID, Oper> + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Prin: Display + Eq + Hash + Send,
    Msg: RoundMsg<RoundID> + Send
{
    #[inline]
    pub(crate) fn create(
        reporter: Reporter,
        rounds: R,
        notify: Notify
    ) -> Self {
        ConsensusAuthNRecv {
            oper: PhantomData,
            msg: PhantomData,
            round_ids: PhantomData,
            prins: Arc::new(RwLock::new(HashMap::new())),
            reporter: reporter,
            notify: notify,
            rounds: rounds
        }
    }

    #[inline]
    pub(crate) fn set_parties<I>(
        &mut self,
        prins: I
    ) -> Result<(), MutexPoison>
    where
        I: Iterator<Item = (PartyStreamIdx, Prin)> {
        let mut guard = self.prins.write().map_err(|_| MutexPoison)?;

        *guard = prins.map(|(a, b)| (b, a)).collect();

        Ok(())
    }
}

impl<R, Reporter, RoundID, Prin, Oper, Msg> Drop
    for ConsensusAuthNRecv<R, Reporter, RoundID, Prin, Oper, Msg>
where
    R: RoundsRecv<RoundID, PartyStreamIdx, Oper, Msg> + Send + Sync,
    Reporter: RoundResultReporter<RoundID, Oper> + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Prin: Display + Eq + Hash + Send,
    Msg: RoundMsg<RoundID> + Send
{
    fn drop(&mut self) {
        if let Err(err) = self.notify.notify() {
            error!(target: "consensus-recv-authn-msg",
                   "error notifying sender: {}",
                   err);
        }
    }
}

impl<R, Reporter, RoundID, Prin, Oper, Msg> AuthNMsgRecv<Prin, Msg>
    for ConsensusAuthNRecv<R, Reporter, RoundID, Prin, Oper, Msg>
where
    R: RoundsRecv<RoundID, PartyStreamIdx, Oper, Msg> + Send + Sync,
    Reporter: RoundResultReporter<RoundID, Oper> + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Prin: Display + Eq + Hash + Send,
    Msg: RoundMsg<RoundID> + Send
{
    type RecvError = MutexPoison;

    /// Receive an authenticated message.
    fn recv_auth_msg(
        &mut self,
        prin: &Prin,
        msg: Msg
    ) -> Result<(), Self::RecvError> {
        let guard = self.prins.read().map_err(|_| MutexPoison)?;

        match guard.get(prin) {
            Some(party) => {
                if let Err(err) =
                    self.rounds.recv(&mut self.reporter, party, msg)
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
