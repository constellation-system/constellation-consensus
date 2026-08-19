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
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::thread::spawn;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::MutexPoison;
use constellation_common::error::WithMutexPoison;
use constellation_common::net::SharedMsgs;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::consensus_ctl::ConsensusCtlRound;
use constellation_component_common::PartyStreamIdx;
use constellation_consensus_common::oper::OperBatch;
use constellation_consensus_common::parties::PartyTypes;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_consensus_common::round::RoundMsg;
use constellation_consensus_common::round::RoundsAdvance;
use constellation_consensus_common::round::RoundsRecv;
use constellation_consensus_common::round::RoundsSetParties;
use constellation_consensus_common::round::RoundsUpdate;
use constellation_consensus_common::state::RoundResultReporter;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::types::ConsensusStateTypes;

pub(crate) struct State<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    Types: ConsensusStateTypes<IDTypes> {
    oper: PhantomData<Types::Oper>,
    // XXX this may be better off as an mpsc.
    outbound: Mutex<VecDeque<ConsensusCtlRound<IDTypes::RoundID,
                                               Types::HashID,
                                               Types::Seal>>>,
    rounds: Mutex<Types::Rounds>,
    notify: Notify
}

pub(crate) struct StateThread<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    Types: ConsensusStateTypes<IDTypes> {
    state: Arc<State<IDTypes, Types>>,
    recv: Receiver<(IDTypes::RoundID, Types::Oper)>,
    shutdown: ShutdownFlag,
    hash: Types::Hash
}

pub(crate) struct StateThreadReporter<RoundID, Oper> {
    send: Sender<(RoundID, Oper)>
}

#[derive(Clone)]
pub(crate) struct StateMsgs<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    Types: ConsensusStateTypes<IDTypes> {
    arc: Arc<State<IDTypes, Types>>
}

pub(crate) enum StateInitRoundsError<Parties, Advance> {
    Parties { err: Parties },
    Advance { err: Advance },
    MutexPoison
}

impl<IDTypes, Types> From<Arc<State<IDTypes, Types>>>
    for StateMsgs<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    Types: ConsensusStateTypes<IDTypes> {
    #[inline]
    fn from(val: Arc<State<IDTypes, Types>>) -> Self {
        StateMsgs { arc: val }
    }
}

impl<IDTypes, Types, Msg> SharedMsgs<PartyStreamIdx, Msg>
    for StateMsgs<IDTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    Types: ConsensusStateTypes<IDTypes>,
    Types::Rounds: SharedMsgs<PartyStreamIdx, Msg>,
    Msg: RoundMsg<IDTypes::RoundID> {
    type MsgsError = WithMutexPoison<
        <Types::Rounds as SharedMsgs<PartyStreamIdx, Msg>>::MsgsError
    >;

    fn msgs(
        &mut self,
        live: &HashSet<PartyStreamIdx>,
        now: Instant
    ) -> Result<
        (
            Option<Vec<(Vec<PartyStreamIdx>, Vec<Msg>)>>,
            Option<Instant>
        ),
        Self::MsgsError
    > {
        self.arc
            .rounds
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .msgs()
            .map_err(|err| WithMutexPoison::Inner { error: err })
    }
}

impl<IDTypes, Types> State<IDTypes, Types>
where
    IDTypes: PartyTypes + RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    Types: ConsensusStateTypes<IDTypes> {
    #[inline]
    pub(crate) fn new(rounds: Types::Rounds) -> Self {
        let outbound = VecDeque::new();

        // XXX use size hints.
        State {
            oper: PhantomData,
            rounds: Mutex::new(rounds),
            outbound: Mutex::new(outbound),
            notify: Notify::new()
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

    pub(crate) fn recv<ProtoTypes, Reporter>(
        &self,
        reporter: &mut Reporter,
        party: &PartyStreamIdx,
        msg: ProtoTypes::Msg
    ) -> Result<
        (),
        WithMutexPoison<
            <Types::Rounds as RoundsRecv<IDTypes, ProtoTypes,
                                         Types::Oper>>::RecvError<
                Reporter::ReportError
            >
        >
    >
    where
        ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
        Types::Rounds: RoundsRecv<IDTypes, ProtoTypes, Types::Oper>,
        Reporter: RoundResultReporter<IDTypes::RoundID, Types::Oper> {
        self.rounds
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .recv(reporter, party, msg)
            .map_err(|err| WithMutexPoison::Inner { error: err })
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
            .submit_elems(hashes.into_iter())
            .map_err(|err| WithMutexPoison::Inner { error: err })?;
        self.notify
            .notify()
            .map_err(|_| WithMutexPoison::MutexPoison)
    }

    pub(crate) fn add_round(
        &self,
        round: IDTypes::RoundID,
        hashes: Vec<Types::HashID>
    ) -> Result<(), MutexPoison> {
        debug!(target: "consensus-component-state",
               "adding round {} to outbound queue",
               round);

        self.outbound
            .lock()
            .map_err(|_| MutexPoison)?
            .push_back(ConsensusCtlRound::new(round, hashes, None));
        self.notify.notify()?;

        Ok(())
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
    IDTypes: PartyTypes + RoundPartyIDTypes<PartyID = PartyStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128>,
    Types: ConsensusStateTypes<IDTypes> {
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

    fn advance_round(
        &mut self,
        rounds: MutexGuard<'_, Types::Rounds>,
        round: IDTypes::RoundID,
        oper: Types::Oper
    ) -> (bool, Option<Instant>) {
        match rounds.advance() {
            Ok(Some((_, deadline))) => {
                if let Err(err) = self.state.notify().notify() {
                    error!(target: "consensus-component-state-thread",
                   "error notifying sender: {}",
                   err);
                }

                (true, deadline)
            }
            // XXX this will go away
            Ok(None) => {
                if let Err(err) = self.state.notify().notify() {
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

    fn process_new_batch(
        &mut self,
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
                    self.advance_round(rounds, round, oper)
                }
            }
            Ok(None) => self.advance_round(rounds, round, oper),
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

    pub(crate) fn start(self) -> JoinHandle<()> {
        spawn(move || self.run())
    }
}

impl<RoundID, Oper> RoundResultReporter<RoundID, Oper>
    for StateThreadReporter<RoundID, Oper>
where
    RoundID: Send,
    Oper: Send + Sync
{
    type ReportError = SendError<(RoundID, Oper)>;

    fn report(
        &self,
        round: RoundID,
        oper: Oper
    ) -> Result<(), Self::ReportError> {
        self.send.send((round, oper))
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
