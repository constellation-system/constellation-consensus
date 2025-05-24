// Copyright © 2024-25 The Johns Hopkins Applied Physics Laboratory LLC.
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
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::thread::spawn;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::MutexPoison;
use constellation_common::hashid::HashID;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_component_common::consensus_ctl::ConsensusCtlRound;
use constellation_consensus_common::round::RoundsAdvance;
use constellation_consensus_common::round::RoundsUpdate;
use constellation_consensus_common::state::RoundResultReporter;
use log::debug;
use log::error;
use log::info;
use log::trace;

const MAX_BATCH_SIZE: usize = 16;

struct Inbound<H>
where
    H: Clone + Display + Hash + HashID + Eq + Send {
    /// Hashes that are actually still live.
    hashes: HashSet<H>,
    /// Queue of hashes, which may contain some that are dead.
    reqs: VecDeque<H>
}

pub(crate) struct State<RoundID, H, Seal>
where
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Display + Hash + HashID + Eq + Send + Sync,
    Seal: Send {
    inbound: RwLock<Inbound<H>>,
    // XXX this may be better off as an mpsc.
    outbound: Mutex<VecDeque<ConsensusCtlRound<RoundID, H, Seal>>>,
    notify: Notify
}

pub(crate) struct StateThread<R, RoundID, H, Seal, Oper>
where
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Display + Hash + HashID + Eq + Send + Sync,
    RoundID: Clone + Display + Ord + Send,
    Oper: Send,
    Seal: Send {
    rounds: PhantomData<R>,
    state: Arc<State<RoundID, H, Seal>>,
    recv: Receiver<(RoundID, Oper)>,
    shutdown: ShutdownFlag
}

pub(crate) struct StateThreadReporter<RoundID, Oper> {
    send: Sender<(RoundID, Oper)>
}

impl<RoundID, H, Seal> State<RoundID, H, Seal>
where
    RoundID: Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: Clone + Display + Hash + HashID + Eq + Send + Sync,
    Seal: Send
{
    #[inline]
    pub(crate) fn new() -> Self {
        let inbound = Inbound {
            hashes: HashSet::new(),
            reqs: VecDeque::new()
        };
        let outbound = VecDeque::new();

        // XXX use size hints.
        State {
            inbound: RwLock::new(inbound),
            outbound: Mutex::new(outbound),
            notify: Notify::new()
        }
    }

    #[inline]
    pub(crate) fn notify(&self) -> Notify {
        self.notify.clone()
    }

    pub(crate) fn get_batch(
        &self,
        buf: &mut [H; MAX_BATCH_SIZE]
    ) -> Result<usize, MutexPoison> {
        let mut count = 0;
        let inbound = self.inbound.read().map_err(|_| MutexPoison)?;

        debug!(target: "consensus-component-state",
               "getting hashes for new round");

        // Filter the queue by what's actually in the live hash set.
        for hash in inbound
            .reqs
            .iter()
            .filter(|ent| inbound.hashes.contains(ent))
            .take(MAX_BATCH_SIZE)
        {
            trace!(target: "consensus-component-state",
                   "adding hash {} to batch",
                   hash);

            buf[count] = hash.clone();
            count += 1;
        }

        Ok(count)
    }

    pub(crate) fn get_round_msg(
        &self
    ) -> Result<Option<ConsensusCtlRound<RoundID, H, Seal>>, MutexPoison> {
        self.outbound
            .lock()
            .map(|mut queue| queue.pop_back())
            .map_err(|_| MutexPoison)
    }

    fn clear_hashes(
        &self,
        hashes: &[H]
    ) -> Result<(), MutexPoison> {
        let mut inbound = self.inbound.write().map_err(|_| MutexPoison)?;

        trace!(target: "consensus-component-state",
               "clearing committed hashes from state");

        // Remove the hashes from the live set.
        for hash in hashes.iter() {
            trace!(target: "consensus-component-state",
                   "clearing hash {}",
                   hash);

            let _ = inbound.hashes.remove(&hash);
        }

        // Clear out the front of the queue.
        while inbound
            .reqs
            .front()
            .map_or(false, |hash| !inbound.hashes.contains(hash))
        {
            if let Some(hash) = inbound.reqs.pop_front() {
                trace!(target: "consensus-component-state",
                       "popped hash {} from queue",
                       hash);
            } else {
                error!(target: "consensus-component-state",
                       "queue should not have been empty");
            }
        }

        Ok(())
    }

    pub(crate) fn add_hashes(
        &self,
        hashes: Vec<H>
    ) -> Result<(), MutexPoison> {
        let mut inbound = self.inbound.write().map_err(|_| MutexPoison)?;
        let mut changed = false;

        debug!(target: "consensus-component-state",
               "attempting to add {} hashes",
               hashes.len());

        for hash in hashes.into_iter() {
            if inbound.hashes.insert(hash.clone()) {
                trace!(target: "consensus-component-state",
                       "adding new hash {}",
                       hash);

                inbound.reqs.push_back(hash);
                changed = true;
            } else {
                trace!(target: "consensus-component-state",
                       "hash {} already known",
                       hash);
            }
        }

        if changed {
            self.notify.notify()?
        }

        Ok(())
    }

    pub(crate) fn add_round(
        &self,
        round: RoundID,
        hashes: Vec<H>
    ) -> Result<(), MutexPoison> {
        debug!(target: "consensus-component-state",
               "adding round {} to outbound queue",
               round);

        self.clear_hashes(&hashes)?;
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

impl<R, RoundID, H, Seal, Oper> StateThread<R, RoundID, H, Seal, Oper>
where
    R: 'static + RoundsAdvance<RoundID> + RoundsUpdate<Oper> + Send,
    RoundID: 'static + Clone + Display + From<u128> + Into<u128> + Ord + Send,
    H: 'static + Clone + Display + Hash + HashID + Eq + Send + Sync,
    Oper: 'static + Send,
    Seal: 'static + Send
{
    pub(crate) fn create(
        state: Arc<State<RoundID, H, Seal>>,
        shutdown: ShutdownFlag
    ) -> (Self, StateThreadReporter<RoundID, Oper>) {
        let (send, recv) = channel();

        (
            StateThread {
                rounds: PhantomData,
                shutdown: shutdown,
                state: state,
                recv: recv
            },
            StateThreadReporter { send: send }
        )
    }

    fn process_one(
        &mut self,
        rounds: &mut R,
        oper: Oper
    ) -> (bool, Option<Instant>) {
        match rounds.update(oper) {
            Ok(()) => match rounds.advance() {
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
            },
            Err(err) => {
                debug!(target: "consensus-component-state-thread",
                       "error applying state update: {}",
                       err);

                (false, None)
            }
        }
    }

    fn recv_one(
        &mut self,
        rounds: &mut R,
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
                    Ok((_, oper)) => self.process_one(rounds, oper),
                    Err(RecvTimeoutError::Timeout) => {
                        match rounds.time_update() {
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
                        }
                    }
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
                match rounds.time_update() {
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
                }
            }
        } else {
            trace!(target: "consensus-component-state-thread",
                   "waiting for round result");

            match self.recv.recv() {
                Ok((_, oper)) => self.process_one(rounds, oper),
                Err(err) => {
                    debug!(target: "consensus-component-state-thread",
                           "saw shutdown condition: {}",
                           err);

                    (false, None)
                }
            }
        }
    }

    fn run(
        mut self,
        mut rounds: R
    ) {
        let mut valid = true;
        let mut deadline = None;

        info!(target: "consensus-component-state-thread",
               "state thread starting");

        while self.shutdown.is_live() && valid {
            (valid, deadline) = self.recv_one(&mut rounds, deadline);
        }

        info!(target: "consensus-component-state-thread",
               "state thread exiting")
    }

    pub(crate) fn start(
        self,
        rounds: R
    ) -> JoinHandle<()> {
        spawn(move || self.run(rounds))
    }
}

impl<RoundID, Oper> RoundResultReporter<RoundID, Oper>
    for StateThreadReporter<RoundID, Oper>
where
    RoundID: Send,
    Oper: Send
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
