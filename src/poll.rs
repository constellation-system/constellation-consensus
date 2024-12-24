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
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::marker::PhantomData;
use std::thread::spawn;
use std::thread::JoinHandle;

use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_consensus_common::outbound::Outbound;
use constellation_consensus_common::parties::PartiesMap;
use constellation_consensus_common::round::RoundMsg;
use constellation_consensus_common::round::Rounds;
use constellation_consensus_common::state::RoundResultReporter;
use constellation_streams::stream::PullStream;
use log::debug;
use log::error;
use log::info;
use log::warn;

use crate::component::PartyStreamIdx;

pub(crate) struct PollThread<
    R,
    RoundID,
    Prin,
    P,
    Stream,
    Oper,
    Msg,
    Out,
    Reporter
> where
    RoundID: Clone + Display + Ord + Send,
    R: Rounds<RoundID, PartyStreamIdx, Oper, Msg, Out> + Send,
    P: PartiesMap<RoundID, Out::PartyID, PartyStreamIdx> + Send,
    Reporter: RoundResultReporter<RoundID, Oper> + Send,
    Out: Outbound<RoundID, Msg> + Send,
    Stream: PullStream<(Prin, Msg)> + Send,
    Msg: RoundMsg<RoundID> + Send,
    Prin: Display + Eq + Hash + Send,
    Oper: Send {
    oper: PhantomData<Oper>,
    round_ids: PhantomData<RoundID>,
    parties: PhantomData<P>,
    msg: PhantomData<Msg>,
    out: PhantomData<Out>,
    reporter: Reporter,
    prins: HashMap<Prin, PartyStreamIdx>,
    notify: Notify,
    rounds: R,
    receiver: Stream,
    shutdown: ShutdownFlag
}

impl<R, RoundID, Prin, P, Stream, Oper, Msg, Out, Reporter> Drop
    for PollThread<R, RoundID, Prin, P, Stream, Oper, Msg, Out, Reporter>
where
    RoundID: Clone + Display + Ord + Send,
    R: Rounds<RoundID, PartyStreamIdx, Oper, Msg, Out> + Send,
    P: PartiesMap<RoundID, Out::PartyID, PartyStreamIdx> + Send,
    Reporter: RoundResultReporter<RoundID, Oper> + Send,
    Out: Outbound<RoundID, Msg> + Send,
    Stream: PullStream<(Prin, Msg)> + Send,
    Msg: RoundMsg<RoundID> + Send,
    Oper: Send,
    Prin: Display + Eq + Hash + Send
{
    fn drop(&mut self) {
        if let Err(err) = self.notify.notify() {
            error!(target: "consensus-component-poll-thread",
                   "error notifying sender: {}",
                   err);
        }
    }
}

impl<R, RoundID, Prin, P, Stream, Oper, Msg, Out, Reporter>
    PollThread<R, RoundID, Prin, P, Stream, Oper, Msg, Out, Reporter>
where
    RoundID: 'static + Clone + Display + Ord + Send,
    R: 'static + Rounds<RoundID, PartyStreamIdx, Oper, Msg, Out> + Send,
    P: 'static + PartiesMap<RoundID, Out::PartyID, PartyStreamIdx> + Send,
    Reporter: 'static + RoundResultReporter<RoundID, Oper> + Send,
    Out: 'static + Outbound<RoundID, Msg> + Send,
    Stream: 'static + PullStream<(Prin, Msg)> + Send,
    Msg: 'static + Debug + RoundMsg<RoundID> + Send,
    Prin: 'static + Display + Eq + Hash + Send,
    Oper: 'static + Send
{
    #[inline]
    pub(crate) fn create<I>(
        receiver: Stream,
        reporter: Reporter,
        rounds: R,
        notify: Notify,
        prins: I,
        shutdown: ShutdownFlag
    ) -> Self
    where
        I: Iterator<Item = (PartyStreamIdx, Prin)> {
        PollThread {
            oper: PhantomData,
            round_ids: PhantomData,
            parties: PhantomData,
            msg: PhantomData,
            out: PhantomData,
            reporter: reporter,
            prins: prins.map(|(a, b)| (b, a)).collect(),
            rounds: rounds,
            notify: notify,
            receiver: receiver,
            shutdown: shutdown
        }
    }

    fn run(mut self) {
        let mut valid = true;

        info!(target: "consensus-component-poll-thread",
              "consensus component polling thread starting");

        while valid && self.shutdown.is_live() {
            match self.receiver.pull() {
                Ok((prin, msg)) => match self.prins.get(&prin) {
                    Some(party) => {
                        if let Err(err) =
                            self.rounds.recv(&mut self.reporter, party, msg)
                        {
                            warn!(target: "consensus-component-poll-thread",
                                  "error receiving message from {}: {}",
                                  prin, err)
                        }

                        if let Err(err) = self.notify.notify() {
                            error!(target: "consensus-component-poll-thread",
                                   "error notifying sender: {}",
                                   err);

                            valid = false;
                        }
                    }
                    None => {
                        warn!(target: "consensus-component-poll-thread",
                              "discarding message from unknown principal {}",
                              prin)
                    }
                },
                Err(err) => {
                    error!(target: "consensus-component-poll-thread",
                           "error receiving: {}",
                           err)
                }
            }
        }

        debug!(target: "consensus-component-poll-thread",
               "consensus component polling thread exiting");
    }

    pub(crate) fn start(self) -> JoinHandle<()> {
        spawn(move || self.run())
    }
}
