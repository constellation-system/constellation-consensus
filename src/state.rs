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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::MutexPoison;
use constellation_common::error::WithMutexPoison;
use constellation_common::net::SharedMsgs;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::consensus_ctl::ConsensusCtlRound;
use constellation_consensus_common::oper::OperBatch;
use constellation_consensus_common::parties::PartyTypes;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_consensus_common::round::RoundMsg;
use constellation_consensus_common::round::Rounds;
use constellation_consensus_common::round::RoundsAdvance;
use constellation_consensus_common::round::RoundsRecv;
use constellation_consensus_common::round::RoundsSetParties;
use constellation_consensus_common::round::RoundsSubmit;
use constellation_consensus_common::round::RoundsUpdate;
use constellation_streams::multicast::MulticastStreamIdx;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::types::ConsensusRecvTypes;
use crate::types::ConsensusStateTypes;

pub(crate) struct StateDispatch<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    /// [Sender], which communicates [Receiver]s for outgoing
    /// [ConsensusCtlRound]s.
    send: Sender<Receiver<ConsensusCtlRound<IDTypes::RoundID,
                                            Types::HashID,
                                            Types::Seal>>>
}

pub(crate) struct State<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    // XXX this may be better off as an mpsc.
    outbound: Mutex<VecDeque<ConsensusCtlRound<IDTypes::RoundID,
                                               Types::HashID,
                                               Types::Seal>>>,
    rounds: Mutex<Types::Rounds>,
    // Used to notify the sender of a new round.
    notify: Notify
}

pub(crate) struct StateThread<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    state: Arc<State<IDTypes, Types>>,
    round_reqs: Receiver<(IDTypes::RoundID, Types::Oper)>,
    shutdown: ShutdownFlag,
    hash: Types::Hash
}

pub(crate) struct StateThreadReporter<RoundID, Oper> {
    send: Sender<(RoundID, Oper)>
}

#[derive(Clone)]
pub(crate) struct StateMsgs<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    state: Arc<State<IDTypes, Types>>
}

pub(crate) enum StateInitRoundsError<Parties, Advance> {
    Parties { err: Parties },
    Advance { err: Advance },
    MutexPoison
}

impl<IDTypes, Types> From<Arc<State<IDTypes, Types>>>
    for StateMsgs<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    #[inline]
    fn from(val: Arc<State<IDTypes, Types>>) -> Self {
        StateMsgs { state: val }
    }
}

impl<IDTypes, Types, Msg> SharedMsgs<MulticastStreamIdx, Msg>
    for StateMsgs<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Rounds: SharedMsgs<MulticastStreamIdx, Msg>,
    Types::Hash: Send,
    Msg: RoundMsg<IDTypes::RoundID> {
    type MsgsError = WithMutexPoison<
        <Types::Rounds as SharedMsgs<MulticastStreamIdx, Msg>>::MsgsError
    >;

    fn msgs(
        &mut self,
        live: &HashSet<MulticastStreamIdx>,
        now: Instant
    ) -> Result<
        (
            Option<Vec<(Vec<MulticastStreamIdx>, Vec<Msg>)>>,
            Option<Instant>
        ),
        Self::MsgsError
    > {
        self.state
            .rounds
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .msgs(live, now)
            .map_err(|err| WithMutexPoison::Inner { err: err })
    }
}

impl<IDTypes, Types> State<IDTypes, Types>
where
    IDTypes: PartyTypes + RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    #[inline]
    pub(crate) fn new(
        rounds: Types::Rounds,
        notify: Notify
    ) -> Self {
        let outbound = VecDeque::new();

        // XXX use size hints.
        State {
            rounds: Mutex::new(rounds),
            outbound: Mutex::new(outbound),
            notify: notify
        }
    }

    pub(crate) fn init_rounds(
        &self,
        codec: IDTypes::PartyCodec,
        self_party: IDTypes::Party,
        party_data: &[IDTypes::Party]
    ) -> Result<
        (),
        StateInitRoundsError<
            <Types::Rounds as RoundsSetParties<IDTypes>>::SetPartiesError,
            Types::AdvanceError
        >
    >
    where
        Types::Rounds: RoundsSetParties<IDTypes> {
        let mut guard = self
            .rounds
            .lock()
            .map_err(|_| StateInitRoundsError::MutexPoison)?;

        guard
            .set_parties(codec, self_party, party_data)
            .map_err(|err| StateInitRoundsError::Parties { err: err })?;
        guard
            .advance()
            .map_err(|err| StateInitRoundsError::Advance { err: err })?;

        Ok(())
    }

    pub(crate) fn recv<ProtoTypes>(
        &self,
        party: &MulticastStreamIdx,
        msg: ProtoTypes::Msg
    ) -> Result<
        (),
        WithMutexPoison<
            <Types::Rounds as RoundsRecv<IDTypes, ProtoTypes,
                                         Types::Oper>>::RecvError
        >
    >
    where
        ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
        Types::Rounds: RoundsRecv<IDTypes, ProtoTypes, Types::Oper> {
        self.rounds
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .recv(reporter, party, msg)
            .map_err(|err| WithMutexPoison::Inner { err: err })
    }

    #[inline]
    pub(crate) fn notify(&self) -> Notify {
        self.notify.clone()
    }

    pub(crate) fn get_round_msg(
        &self
    ) -> Result<
        Option<ConsensusCtlRound<IDTypes::RoundID, Types::HashID, Types::Seal>>,
        MutexPoison
    > {
        self.outbound
            .lock()
            .map(|mut queue| queue.pop_back())
            .map_err(|_| MutexPoison)
    }

    pub(crate) fn add_hashes(
        &self,
        hashes: Vec<Types::HashID>
    ) -> Result<(), WithMutexPoison<Types::SubmitError>> {
        debug!(target: "consensus-component-state",
               "submitting {} hashes",
               hashes.len());

        self.rounds
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .submit_reqs(hashes.into_iter())
            .map_err(|err| WithMutexPoison::Inner { err: err })?;
        self.notify
            .notify()
            .map_err(|_| WithMutexPoison::MutexPoison)
    }

    pub(crate) fn add_round(
        &self,
        round: IDTypes::RoundID,
        hashes: Vec<Types::HashID>
    ) -> Result<(), WithMutexPoison<std::io::Error>> {
        debug!(target: "consensus-component-state",
               "adding round {} to outbound queue",
               round);

        self.outbound
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .push_back(ConsensusCtlRound::new(round, hashes, None));
        self.notify.notify()
            .map_err(|err| WithMutexPoison::Inner { err: err })?;

        Ok(())
    }

    fn advance_round(
        &self,
        mut rounds: MutexGuard<'_, Types::Rounds>,
    ) -> (bool, Option<Instant>) {
        match rounds.advance() {
            Ok(Some((_, deadline))) => {
                // Notify the bus for new messages.

                if let Err(err) = self.notify.notify() {
                    error!(target: "consensus-component-state-thread",
                           "error notifying sender: {}",
                           err);
                }

                (true, deadline)
            }
            // XXX this will go away
            Ok(None) => {
                if let Err(err) = self.notify.notify() {
                    error!(target: "consensus-component-state-thread",
                           "error notifying sender: {}",
                           err);
                }

                (true, None)
            }
            Err(err) => {
                error!(target: "consensus-component-state-thread",
                       "error advancing to next round: {}",
                       err);

                (false, None)
            }
        }
    }
}

impl<RoundID, Oper> Clone for StateThreadReporter<RoundID, Oper> {
    #[inline]
    fn clone(&self) -> Self {
        StateThreadReporter {
            send: self.send.clone()
        }
    }
}

impl<IDTypes, Types> StateThread<IDTypes, Types>
where
    IDTypes: 'static + PartyTypes + RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    Types: 'static + ConsensusStateTypes<IDTypes>,
    Types::Hash: Send {
    pub(crate) fn create(
        state: Arc<State<IDTypes, Types>>,
        shutdown: ShutdownFlag,
        hash: Types::Hash
    ) -> (Self, StateThreadReporter<IDTypes::RoundID, Types::Oper>) {
        let (send, recv) = channel();

        (
            StateThread {
                shutdown: shutdown,
                state: state,
                recv: recv,
                hash: hash
            },
            StateThreadReporter { send: send }
        )
    }

    fn process_new_batch(
        &self,
        rounds: MutexGuard<'_, Types::Rounds>,
        round: IDTypes::RoundID,
        oper: Types::Oper
    ) -> (bool, Option<Instant>) {
        match oper.take_batch(&self.hash) {
            Ok(Some(batch)) => {
                if let Err(err) = self.state.add_round(round, batch)
                {
                    error!(target: "consensus-component-state-thread",
                   "error adding round: {}",
                   err);

                    (false, None)
                } else {
                    self.state.advance_round(rounds)
                }
            }
            Ok(None) => self.state.advance_round(rounds),
            Err(err) => {
                error!(target: "consensus-component-state-thread",
                   "error getting hashes for batch: {}",
                   err);

                (false, None)
            }
        }
    }

    fn process_one(
        &mut self,
        round: IDTypes::RoundID,
        oper: Types::Oper
    ) -> (bool, Option<Instant>) {
        match self.state.rounds.lock() {
            Ok(mut rounds) => {
                if let Err(err) = rounds.update(&oper) {
                    debug!(target: "consensus-component-state-thread",
                       "error applying state update: {}",
                       err);

                    (false, None)
                } else {
                    self.process_new_batch(rounds, round, oper)
                }
            }
            Err(_) => {
                error!(target: "consensus-component-state-thread",
                       "mutex poisoned");

                (false, None)
            }
        }
    }

    fn time_update(&mut self) -> (bool, Option<Instant>) {
        match self.state.rounds.lock() {
            Ok(mut rounds) => match rounds.time_update() {
                Ok(deadline) => {
                    if let Err(err) = self.state.notify().notify() {
                        error!(target: "consensus-component-state-thread",
                           "error notifying sender: {}",
                           err);

                        (false, None)
                    } else {
                        (true, deadline)
                    }
                }
                Err(err) => {
                    error!(target: "consensus-component-state-thread",
                           "error in time update: {}",
                           err);

                    (false, None)
                }
            },
            Err(_) => {
                error!(target: "consensus-component-state-thread",
                       "mutex poisoned");

                (false, None)
            }
        }
    }

    fn recv_one(
        &mut self,
        deadline: Option<Instant>
    ) -> (bool, Option<Instant>) {
        if let Some(when) = &deadline {
            let now = Instant::now();

            if now < *when {
                let duration = *when - now;

                trace!(target: "consensus-component-state-thread",
                       "waiting for round result for {}.{:.03}s",
                       duration.as_secs(),
                       duration.subsec_millis());

                match self.recv.recv_timeout(duration) {
                    Ok((round, oper)) => self.process_one(round, oper),
                    Err(RecvTimeoutError::Timeout) => self.time_update(),
                    Err(RecvTimeoutError::Disconnected) => {
                        debug!(target: "consensus-component-state-thread",
                               "mpsc channel disconnected");

                        (false, None)
                    }
                }
            } else {
                trace!(target: "consensus-component-state-thread",
                       "time update deadline expired");

                // Deadline already expired.
                self.time_update()
            }
        } else {
            trace!(target: "consensus-component-state-thread",
                   "waiting for round result");

            match self.recv.recv() {
                Ok((round, oper)) => self.process_one(round, oper),
                Err(err) => {
                    debug!(target: "consensus-component-state-thread",
                           "saw shutdown condition: {}",
                           err);

                    (false, None)
                }
            }
        }
    }

    fn run(mut self) {
        let mut valid = true;
        let mut deadline = None;

        info!(target: "consensus-component-state-thread",
               "state thread starting");

        while self.shutdown.is_live() && valid {
            (valid, deadline) = self.recv_one(deadline);
        }

        info!(target: "consensus-component-state-thread",
               "state thread exiting")
    }

    pub(crate) fn start(self) -> Result<JoinHandle<()>, std::io::Error> {
        Builder::new()
            .name(String::from("consensus-state-thread"))
            .spawn(move || self.run())
    }
}

impl<Parties, Advance> Display for StateInitRoundsError<Parties, Advance>
where
    Parties: Display,
    Advance: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StateInitRoundsError::Parties { err } => err.fmt(f),
            StateInitRoundsError::Advance { err } => err.fmt(f),
            StateInitRoundsError::MutexPoison => write!(f, "mutex poisoned")
        }
    }
}
