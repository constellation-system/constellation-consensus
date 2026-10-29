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
use std::fmt::Display;
use std::fmt::Error;
use std::fmt::Formatter;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::Builder;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_common::error::WithMutexPoison;
use constellation_common::net::SharedMsgs;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::retry::next_retry;
use constellation_common::sync::Notify;
use constellation_component_common::consensus_ctl::ConsensusCtlRound;
use constellation_consensus_common::oper::OperBatch;
use constellation_consensus_common::parties::PartyTypes;
use constellation_consensus_common::parties::RoundPartyIDTypes;
use constellation_consensus_common::proto::ConsensusProtoMsgTypes;
use constellation_consensus_common::round::RoundMsg;
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
use mio::Events;
use mio::Poll;
use mio::Token;
use mio::Waker;

use crate::peers::PeerSessionState;
use crate::types::ConsensusPeerTypes;
use crate::types::ConsensusRecvTypes;

const WAKER_TOKEN: Token = Token(0);

pub struct State<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    ProtoTypes::Msg: Send,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    // XXX it should be possible to separate out the outbound mailbox
    // and just use that.
    rounds: Arc<Mutex<Types::Rounds>>,
}

pub(crate) struct StateThread<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    ProtoTypes::Msg: Send,
    Types: ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    poll: Poll,
    state: State<IDTypes, ProtoTypes, Types>,
    round_reqs: Receiver<Vec<Types::HashID>>,
    msgs: Receiver<(MulticastStreamIdx, ProtoTypes::Msg)>,
    peers: PeerSessionState<IDTypes, ProtoTypes, Types>,
    /// Used to notify the sender of new messages.
    sender_notify: Notify,
    /// Used to notify the peers sender of a new round.
    peers_notify: Notify,
    shutdown: ShutdownFlag,
    hash: Types::Hash
}

#[derive(Debug)]
pub(crate) enum StateInitRoundsError<Parties, Advance> {
    Parties { err: Parties },
    Advance { err: Advance },
    MutexPoison
}

#[derive(Debug)]
enum StateRecvError<Recv, Update, Batch, Advance> {
    Recv { err: Recv },
    Update { err: Update },
    Batch { err: Batch },
    Advance { err: Advance },
    IO { err: std::io::Error },
    MutexPoison
}

#[derive(Debug)]
enum WithIOError<Inner> {
    Inner {
        err: Inner
    },
    IO {
        err: std::io::Error
    }
}

impl<IDTypes, ProtoTypes, Types> Clone for State<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    ProtoTypes::Msg: Send,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    #[inline]
    fn clone(&self) -> Self {
        State {
            rounds: self.rounds.clone()
        }
    }
}

impl<IDTypes, ProtoTypes, Types, Msg> SharedMsgs<MulticastStreamIdx, Msg>
    for State<IDTypes, ProtoTypes, Types>
where
    IDTypes: RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    ProtoTypes::Msg: Send,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
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
        self.rounds
            .lock()
            .map_err(|_| WithMutexPoison::MutexPoison)?
            .msgs(live, now)
            .map_err(|err| WithMutexPoison::Inner { err: err })
    }
}

impl<IDTypes, ProtoTypes, Types> State<IDTypes, ProtoTypes, Types>
where
    IDTypes: PartyTypes + RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: ConsensusProtoMsgTypes<IDTypes::RoundID>,
    ProtoTypes::Msg: Send,
    Types: ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    #[inline]
    pub(crate) fn new(
        rounds: Types::Rounds,
    ) -> Self {
        State {
            rounds: Arc::new(Mutex::new(rounds)),
        }
    }

    fn init_rounds(
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

    fn add_hashes(
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
            .map_err(|err| WithMutexPoison::Inner { err: err })
    }

    fn recv(
        &self,
        party: MulticastStreamIdx,
        msg: ProtoTypes::Msg,
    ) -> Result<
        Option<(
            IDTypes::RoundID,
            Types::Oper,
            Option<(IDTypes::RoundID, Option<Instant>)>
        )>,
        StateRecvError<
            <Types::Rounds as RoundsRecv<IDTypes, ProtoTypes,
                                         Types::Oper>>::RecvError,
            Types::UpdateError,
            Types::BatchError,
            Types::AdvanceError
        >
    > {
        let mut guard = self.rounds
            .lock()
            .map_err(|_| StateRecvError::MutexPoison)?;

        if let Some((round, oper)) = guard
            .recv(&party, msg)
            .map_err(|err| StateRecvError::Recv { err: err })? {
            guard.update(&oper)
                .map_err(|err| StateRecvError::Update { err: err })?;

            let newround = guard
                .advance()
                .map_err(|err| StateRecvError::Advance { err: err })?;

            Ok(Some((round, oper, newround)))
        } else {
            Ok(None)
        }
    }
}

impl<IDTypes, ProtoTypes, Types> StateThread<IDTypes, ProtoTypes, Types>
where
    IDTypes: 'static + PartyTypes + RoundPartyIDTypes<PartyID = MulticastStreamIdx>,
    IDTypes::RoundID: From<u128> + Into<u128> + Send,
    ProtoTypes: 'static + ConsensusProtoMsgTypes<IDTypes::RoundID>,
    ProtoTypes::Msg: Send,
    Types::Rounds: RoundsRecv<IDTypes, ProtoTypes, Types::Oper>,
    Types: 'static + ConsensusPeerTypes<IDTypes, ProtoTypes>
        + ConsensusRecvTypes<IDTypes, ProtoTypes>,
    Types::Hash: Send {
    pub(crate) fn new(
        state: State<IDTypes, ProtoTypes, Types>,
        peers: PeerSessionState<IDTypes, ProtoTypes, Types>,
        peers_notify: Notify,
        sender_notify: Notify,
        shutdown: ShutdownFlag,
        hash: Types::Hash
    ) -> Result<(
        Self,
        Sender<Vec<Types::HashID>>,
        Sender<(MulticastStreamIdx, ProtoTypes::Msg)>,
        Notify
    ), std::io::Error> {
        let poll = Poll::new()?;
        let waker = Waker::new(poll.registry(), WAKER_TOKEN)?;
        let waker = Arc::new(waker);
        let notify = Notify::new(waker);
        let (round_reqs_send, round_reqs_recv) = channel();
        let (msgs_send, msgs_recv) = channel();
        let thread = StateThread {
            poll: poll,
            shutdown: shutdown,
            round_reqs: round_reqs_recv,
            sender_notify: sender_notify,
            msgs: msgs_recv,
            state: state,
            peers_notify: peers_notify,
            peers: peers,
            hash: hash
        };

        Ok((thread, round_reqs_send, msgs_send, notify))
    }

    #[inline]
    fn init_rounds(
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
        self.state.init_rounds(codec, self_party, party_data)
    }

    fn recv(
        &self,
        next: &mut Option<Instant>,
        party: MulticastStreamIdx,
        msg: ProtoTypes::Msg,
    ) -> Result<
        (),
        StateRecvError<
            <Types::Rounds as RoundsRecv<IDTypes, ProtoTypes,
                                         Types::Oper>>::RecvError,
            Types::UpdateError,
            Types::BatchError,
            Types::AdvanceError
        >
    > {
        if let Some((round, oper, newround)) = self.state.recv(party, msg)? {
            if let Some((_, when)) = newround {
                *next = next_retry(next, &when);
            }

            if let Some(batch) = oper.take_batch(&self.hash)
                .map_err(|err| StateRecvError::Batch { err: err })? {
                debug!(target: "consensus-component-state",
                       "adding round {} to outbound queue",
                       round);

                let msg = ConsensusCtlRound::new(round, batch, None);

                self.peers.add_outbound(msg)
                    .map_err(|_| StateRecvError::MutexPoison)?;
                self.peers_notify
                    .notify()
                    .map_err(|err| StateRecvError::IO { err: err })?
            }

            self.sender_notify
                .notify()
                .map_err(|err| StateRecvError::IO { err: err })?
        }

        Ok(())
    }

    fn add_hashes(
        &self,
        hashes: Vec<Types::HashID>
    ) -> Result<(), WithIOError<WithMutexPoison<Types::SubmitError>>> {
        debug!(target: "consensus-component-state",
               "submitting {} hashes",
               hashes.len());

        self.state.add_hashes(hashes)
            .map_err(|err| WithIOError::Inner { err: err })?;
        self.sender_notify
            .notify()
            .map_err(|err| WithIOError::IO { err: err })
    }

    fn handle_new_hashes(
        &mut self,
    ) -> bool {
        loop {
            match self.round_reqs.try_recv() {
                Ok(hashes) => if let Err(err) = self.add_hashes(hashes) {
                    error!(target: "consensus-component-state-thread",
                           "error adding hashes: {}",
                           err);

                    return false
                }
                Err(TryRecvError::Disconnected) => return false,
                Err(TryRecvError::Empty) => return true
            }
        }
    }

    fn handle_msgs(
        &mut self,
        next: &mut Option<Instant>,
    ) -> bool {
        debug!(target: "consensus-component-state-thread",
               "handling consensus protocol messages");
        loop {
            match self.msgs.try_recv() {
                Ok((party, msg)) => if let Err(err) = self
                    .recv(next, party, msg) {
                    error!(target: "consensus-component-state-thread",
                           "error handling consensus protocol message: {}",
                           err);

                    return false
                }
                Err(TryRecvError::Disconnected) => return false,
                Err(TryRecvError::Empty) => return true
            }
        }
    }

    fn handle_events(
        &mut self,
        events: &mut Events,
        next: &mut Option<Instant>,
    ) -> bool {
        let mut out = true;

        if events
            .iter()
            .map(|event| {
                let token = event.token();

                if token != WAKER_TOKEN {
                    error!(target: "consensus-component-state-thread",
                           "unknown token {}",
                           token.0);
                }

                token
            }).any(|token| token == WAKER_TOKEN) {


            out &= self.handle_new_hashes();
            out &= self.handle_msgs(next);
        }

        out
    }

    fn run(
        mut self,
    ) {
        let mut next = Some(Instant::now());
        let mut events = Events::with_capacity(1);
        let mut now;

        info!(target: "consensus-component-state-thread",
               "state thread starting");

        while {
            now = Instant::now();

            self.shutdown.is_live() &&
                (next.is_some_and(|next: Instant| next < now) || {
                    let duration = next.map(|next| next - now);

                    if let Some(duration) = &duration {
                        trace!(target: "consensus-component-state-thread",
                               "waiting for poll for {}.{:03}",
                               duration.as_secs(), duration.subsec_millis());
                    } else {
                        trace!(target: "consensus-component-state-thread",
                               "waiting for poll indefinitely");
                    }

                    self.poll.poll(&mut events, duration)
                        .inspect_err(|err| {
                            error!(target: "poll-thread",
                                   "error polling: {}",
                                   err)
                        })
                        .is_ok()
                }) &&
                self.handle_events(&mut events, &mut next)
        } {}

        if let Err(err) = self.shutdown.set() {
            error!(target: "consensus-component-state-thread",
                   "error setting shutdown: {}",
                   err)
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

impl<Inner> Display for WithIOError<Inner>
where
    Inner: Display,
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            WithIOError::Inner { err } => err.fmt(f),
            WithIOError::IO { err } => err.fmt(f),
        }
    }
}

impl<Recv, Update, Batch, Advance> Display
    for StateRecvError<Recv, Update, Batch, Advance>
where
    Recv: Display,
    Batch: Display,
    Update: Display,
    Advance: Display
{
    fn fmt(
        &self,
        f: &mut Formatter<'_>
    ) -> Result<(), Error> {
        match self {
            StateRecvError::IO { err } => err.fmt(f),
            StateRecvError::Recv { err } => err.fmt(f),
            StateRecvError::Batch { err } => err.fmt(f),
            StateRecvError::Update { err } => err.fmt(f),
            StateRecvError::Advance { err } => err.fmt(f),
            StateRecvError::MutexPoison => write!(f, "mutex poisoned")
        }
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
