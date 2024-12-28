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

use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::thread::spawn;
use std::thread::JoinHandle;

use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_consensus_common::round::RoundsAdvance;
use constellation_consensus_common::round::RoundsUpdate;
use constellation_consensus_common::state::RoundResultReporter;
use log::debug;
use log::error;
use log::info;

pub(crate) struct StateThread<R, RoundID, Oper>
where
    R: RoundsAdvance<RoundID> + RoundsUpdate<Oper> + Send,
    RoundID: Clone + Display + Ord + Send,
    Oper: Send {
    rounds: PhantomData<R>,
    recv: Receiver<(RoundID, Oper)>,
    shutdown: ShutdownFlag,
    notify: Notify
}

pub(crate) struct StateThreadReporter<RoundID, Oper> {
    send: Sender<(RoundID, Oper)>
}

impl<R, RoundID, Oper> StateThread<R, RoundID, Oper>
where
    R: 'static + RoundsAdvance<RoundID> + RoundsUpdate<Oper> + Send,
    RoundID: 'static + Clone + Display + Ord + Send,
    Oper: 'static + Send
{
    pub(crate) fn create(
        notify: Notify,
        shutdown: ShutdownFlag
    ) -> (Self, StateThreadReporter<RoundID, Oper>) {
        let (send, recv) = channel();

        (
            StateThread {
                rounds: PhantomData,
                notify: notify,
                shutdown: shutdown,
                recv: recv
            },
            StateThreadReporter { send: send }
        )
    }

    fn run(
        self,
        mut rounds: R
    ) {
        let mut valid = true;

        info!(target: "consensus-component-state-thread",
               "state thread starting");

        while self.shutdown.is_live() && valid {
            info!(target: "consensus-component-state-thread",
                  "waiting for notification");

            match self.recv.recv() {
                Ok((round, oper)) => {
                    debug!(target: "consensus-component-state-thread",
                           "notified of round: {}",
                           round);

                    match rounds.update(oper) {
                        Ok(()) => {
                            if let Err(err) = rounds.advance() {
                                error!(target: "consensus-component-state-thread",
                                       "error advancing to next round: {}",
                                       err);

                                valid = false;
                            }

                            if let Err(err) = self.notify.notify() {
                                error!(target: "consensus-component-state-thread",
                                       "error notifying sender: {}",
                                       err);
                            }
                        }
                        Err(err) => {
                            debug!(target: "consensus-component-state-thread",
                                   "error applying state update: {}",
                                   err);

                            valid = false;
                        }
                    }
                }
                Err(err) => {
                    debug!(target: "consensus-component-state-thread",
                           "saw shutdown condition: {}",
                           err);

                    valid = false;
                }
            }
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
