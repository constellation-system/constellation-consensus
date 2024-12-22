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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Display;
use std::marker::PhantomData;
use std::thread::spawn;
use std::thread::JoinHandle;
use std::time::Instant;

use constellation_channels::resolve::cache::NSNameCachesCtx;
use constellation_common::retry::RetryResult;
use constellation_common::retry::RetryWhen;
use constellation_common::shutdown::ShutdownFlag;
use constellation_common::sync::Notify;
use constellation_consensus_common::outbound::Outbound;
use constellation_consensus_common::parties::PartiesMap;
use constellation_consensus_common::round::RoundMsg;
use constellation_consensus_common::round::Rounds;
use constellation_streams::error::BatchError;
use constellation_streams::stream::PushStreamAdd;
use constellation_streams::stream::PushStreamAddParty;
use constellation_streams::stream::PushStreamParties;
use constellation_streams::stream::PushStreamReportBatchError;
use constellation_streams::stream::PushStreamReportError;
use constellation_streams::stream::PushStreamShared;
use constellation_streams::stream::PushStreamSharedSingle;
use log::debug;
use log::error;
use log::info;
use log::trace;

use crate::component::PartyStreamIdx;

enum SendEntry<Msg, Stream, Ctx>
where
    Stream: PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamAdd<Msg, Ctx>
        + PushStreamAddParty<Ctx>
        + Send,
    Msg: Clone + Send,
    Ctx: NSNameCachesCtx {
    Batch {
        msgs: Vec<Msg>,
        parties: Vec<Stream::PartyID>,
        batches: Stream::StartBatchStreamBatches,
        retry: Stream::StartBatchRetry
    },
    Abort {
        flags: Stream::StreamFlags,
        retry: Stream::AbortBatchRetry
    },
    Parties {
        msgs: Vec<Msg>,
        batch: Stream::BatchID,
        retry: Stream::AddPartiesRetry
    },
    Add {
        msgs: Vec<Msg>,
        msg: Msg,
        flags: Stream::StreamFlags,
        batch: Stream::BatchID,
        retry: Stream::AddRetry
    },
    Finish {
        batch: Stream::BatchID,
        retry: Stream::FinishBatchRetry
    },
    Cancel {
        flags: Stream::StreamFlags,
        batch: Stream::BatchID,
        retry: Stream::CancelBatchRetry
    }
}

pub(crate) struct SendThread<R, RoundID, P, Stream, Oper, Msg, Out, Ctx>
where
    RoundID: Clone + Display + Ord + Send,
    R: Rounds<RoundID, PartyStreamIdx, Oper, Msg, Out> + Send,
    P: PartiesMap<RoundID, Out::PartyID, PartyStreamIdx> + Send,
    Out: Outbound<RoundID, Msg> + Send,
    Stream: PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamSharedSingle<Msg, Ctx>
        + PushStreamShared<PartyID = PartyStreamIdx>
        + PushStreamParties
        + Send,
    Stream::StartBatchStreamBatches: Send,
    Stream::StartBatchRetry: Send,
    Stream::AbortBatchRetry: Send,
    Stream::AddPartiesRetry: Send,
    Stream::AddRetry: Send,
    Stream::FinishBatchRetry: Send,
    Stream::CancelBatchRetry: Send,
    Stream::StreamFlags: Send,
    Stream::PartyID: From<usize> + Send,
    Stream::BatchID: Send,
    Msg: Clone + RoundMsg<RoundID> + Send,
    Ctx: NSNameCachesCtx + Send + Sync,
    Oper: Send {
    round_id: PhantomData<RoundID>,
    parties: PhantomData<P>,
    oper: PhantomData<Oper>,
    out: PhantomData<Out>,
    ctx: Ctx,
    pending: Vec<SendEntry<Msg, Stream, Ctx>>,
    rounds: R,
    notify: Notify,
    shutdown: ShutdownFlag,
    stream: Stream
}

impl<Msg, Stream, Ctx> RetryWhen for SendEntry<Msg, Stream, Ctx>
where
    Stream: PushStreamAdd<Msg, Ctx>
        + PushStreamAddParty<Ctx>
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        > + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        > + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        > + Send,
    Msg: Clone + Send,
    Ctx: NSNameCachesCtx
{
    fn when(&self) -> Instant {
        match self {
            SendEntry::Batch { retry, .. } => retry.when(),
            SendEntry::Abort { retry, .. } => retry.when(),
            SendEntry::Parties { retry, .. } => retry.when(),
            SendEntry::Add { retry, .. } => retry.when(),
            SendEntry::Finish { retry, .. } => retry.when(),
            SendEntry::Cancel { retry, .. } => retry.when()
        }
    }
}

impl<Msg, Stream, Ctx> SendEntry<Msg, Stream, Ctx>
where
    Stream: 'static
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        >
        + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamAddParty<Ctx>
        + PushStreamAdd<Msg, Ctx>
        + Send,
    Stream::PartyID: From<usize>,
    Msg: 'static + Clone + Send,
    Ctx: 'static + NSNameCachesCtx + Send + Sync
{
    fn complete_cancel_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: Stream::CancelBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "consensus-component-send-thread",
               "attempting to recover from error while cancelling message");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_cancel_batch(
                    ctx,
                    &mut flags,
                    &batch_id,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => {
                        trace!(target: "consensus-component-send-thread",
                           "successfully completed cancellation");

                        RetryResult::Success(())
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(SendEntry::Cancel {
                            batch: batch_id,
                            retry: retry,
                            flags: flags
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_cancel_batch(
                        ctx, stream, flags, batch_id, err
                    )
                }
            }
            // Unrecoverable errors occurred canceling the batch.
            (_, Some(permanent)) => {
                error!(target: "consensus-component-send-thread",
                       "unrecoverable error canceling batch: {}",
                       permanent);

                RetryResult::Success(())
            }
            (None, None) => {
                error!(target: "consensus-component-send-thread",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_cancel_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        let mut flags = stream.empty_flags();

        match stream.cancel_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => RetryResult::Success(()),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(SendEntry::Cancel {
                    batch: batch_id,
                    retry: retry,
                    flags: flags
                })
            }
            Err(err) => {
                Self::complete_cancel_batch(ctx, stream, flags, batch_id, err)
            }
        }
    }

    fn complete_finish(
        ctx: &mut Ctx,
        stream: &mut Stream,
        flags: &mut Stream::StreamFlags,
        batch_id: Stream::BatchID,
        err: Stream::FinishBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "consensus-component-send-thread",
               "attempting to recover from error while finishing batch");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_finish_batch(
                    ctx,
                    flags,
                    &batch_id,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(())) => {
                        trace!(target: "consensus-component-send-thread",
                           "successfully finished batch");

                        RetryResult::Success(())
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(SendEntry::Finish {
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => {
                        Self::complete_finish(ctx, stream, flags, batch_id, err)
                    }
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred canceling the batch.
                error!(target: "consensus-component-send-thread",
                       "unrecoverable error finishing batch: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "consensus-component-send-thread",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "consensus-component-send-thread",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_finish_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        let mut flags = stream.empty_flags();

        match stream.finish_batch(ctx, &mut flags, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => RetryResult::Success(()),
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(SendEntry::Finish {
                    batch: batch_id.clone(),
                    retry: retry
                })
            }
            Err(err) => Self::complete_finish(
                ctx,
                stream,
                &mut flags,
                batch_id.clone(),
                err
            )
        }
    }

    fn complete_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID,
        err: Stream::AddError
    ) -> RetryResult<(), Self> {
        trace!(target: "consensus-component-send-thread",
               "attempting to recover from error while adding message");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_add(
                    ctx,
                    &mut flags,
                    &msg,
                    &batch_id,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(())) => {
                        trace!(target: "consensus-component-send-thread",
                           "successfully added message");

                        Self::try_add(ctx, stream, msgs, batch_id)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(SendEntry::Add {
                            msgs: msgs,
                            msg: msg,
                            flags: flags,
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_add(
                        ctx, stream, flags, msgs, msg, batch_id, err
                    )
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "consensus-component-send-thread",
                       "unrecoverable error adding message: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "consensus-component-send-thread",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "consensus-component-send-thread",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_add_msg(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut flags: Stream::StreamFlags,
        msgs: Vec<Msg>,
        msg: Msg,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        match stream.add(ctx, &mut flags, &msg, &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(SendEntry::Add {
                    msgs: msgs,
                    msg: msg,
                    flags: flags,
                    batch: batch_id,
                    retry: retry
                })
            }
            Err(err) => {
                Self::complete_add(ctx, stream, flags, msgs, msg, batch_id, err)
            }
        }
    }

    fn try_add(
        ctx: &mut Ctx,
        stream: &mut Stream,
        mut msgs: Vec<Msg>,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        match msgs.pop() {
            Some(msg) => {
                let flags = stream.empty_flags();

                Self::try_add_msg(ctx, stream, flags, msgs, msg, batch_id)
            }
            None => Self::try_finish_batch(ctx, stream, batch_id)
        }
    }

    fn complete_add_parties(
        ctx: &mut Ctx,
        stream: &mut Stream,
        msgs: Vec<Msg>,
        batch_id: Stream::BatchID,
        err: Stream::AddPartiesError
    ) -> RetryResult<(), Self> {
        trace!(target: "consensus-component-send-thread",
               "attempting to recover from error while adding parties");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_add_parties(ctx, &batch_id, completable) {
                    // It succeeded.
                    Ok(RetryResult::Success(())) => {
                        trace!(target: "consensus-component-send-thread",
                           "successfully added parties");

                        Self::try_add(ctx, stream, msgs, batch_id)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(SendEntry::Parties {
                            msgs: msgs,
                            batch: batch_id,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_add_parties(
                        ctx, stream, msgs, batch_id, err
                    )
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "consensus-component-send-thread",
                       "unrecoverable error adding parties: {}",
                       permanent);

                // Report the failure
                if let Err(err) =
                    stream.report_error_with_batch(&batch_id, &permanent)
                {
                    error!(target: "consensus-component-send-thread",
                           "failed to report errors to stream: {}",
                           err);
                }

                Self::try_cancel_batch(ctx, stream, batch_id)
            }
            (None, None) => {
                error!(target: "consensus-component-send-thread",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_add_parties(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>,
        batch_id: Stream::BatchID
    ) -> RetryResult<(), Self> {
        match stream.add_parties(ctx, parties.into_iter(), &batch_id) {
            // It succeeded.
            Ok(RetryResult::Success(_)) => {
                Self::try_add(ctx, stream, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(SendEntry::Parties {
                    msgs: msgs,
                    batch: batch_id,
                    retry: retry
                })
            }
            Err(err) => {
                Self::complete_add_parties(ctx, stream, msgs, batch_id, err)
            }
        }
    }

    fn complete_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>,
        mut batches: Stream::StartBatchStreamBatches,
        err: Stream::StartBatchError
    ) -> RetryResult<(), Self> {
        trace!(target: "consensus-component-send-thread",
               "attempting to recover from error while creating batch");

        match err.split() {
            (Some(completable), None) => {
                match stream.complete_start_batch(
                    ctx,
                    &mut batches,
                    completable
                ) {
                    // It succeeded.
                    Ok(RetryResult::Success(batch_id)) => {
                        trace!(target: "consensus-component-send-thread",
                           "successfully created batch");

                        Self::try_add_parties(
                            ctx, stream, parties, msgs, batch_id
                        )
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(SendEntry::Batch {
                            msgs: msgs,
                            batches: batches,
                            parties: parties,
                            retry: retry
                        })
                    }
                    // More errors; recurse again.
                    Err(err) => Self::complete_start_batch(
                        ctx, stream, parties, msgs, batches, err
                    )
                }
            }
            // Permanent errors always kill the batch.
            (_, Some(permanent)) => {
                // Unrecoverable errors occurred adding the message.
                error!(target: "consensus-component-send-thread",
                       "unrecoverable error adding parties: {}",
                       permanent);

                // Report the failure
                if let Err(err) = stream.report_error(&permanent) {
                    error!(target: "consensus-component-send-thread",
                           "failed to report errors to stream: {}",
                           err);
                }

                let mut flags = stream.empty_flags();

                stream
                    .abort_start_batch(ctx, &mut flags, permanent)
                    .map_retry(|retry| SendEntry::Abort {
                        flags: flags,
                        retry: retry
                    })
            }
            (None, None) => {
                error!(target: "consensus-component-send-thread",
                       "neither completable nor permanent errors reported");

                RetryResult::Success(())
            }
        }
    }

    fn try_start_batch(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>
    ) -> RetryResult<(), Self> {
        let mut batches = stream.empty_batches();

        match stream.start_batch(ctx, &mut batches) {
            // It succeeded.
            Ok(RetryResult::Success(batch_id)) => {
                Self::try_add_parties(ctx, stream, parties, msgs, batch_id)
            }
            // We got a retry.
            Ok(RetryResult::Retry(retry)) => {
                RetryResult::Retry(SendEntry::Batch {
                    msgs: msgs,
                    batches: batches,
                    parties: parties,
                    retry: retry
                })
            }
            Err(err) => Self::complete_start_batch(
                ctx, stream, parties, msgs, batches, err
            )
        }
    }

    fn exec(
        self,
        ctx: &mut Ctx,
        stream: &mut Stream
    ) -> RetryResult<(), Self> {
        match self {
            SendEntry::Batch {
                msgs,
                parties,
                mut batches,
                retry
            } => match stream.retry_start_batch(ctx, &mut batches, retry) {
                // It succeeded.
                Ok(RetryResult::Success(batch_id)) => {
                    Self::try_add_parties(ctx, stream, parties, msgs, batch_id)
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(SendEntry::Batch {
                        msgs: msgs,
                        batches: batches,
                        parties: parties,
                        retry: retry
                    })
                }
                Err(err) => Self::complete_start_batch(
                    ctx, stream, parties, msgs, batches, err
                )
            },
            SendEntry::Abort { mut flags, retry } => stream
                .retry_abort_start_batch(ctx, &mut flags, retry)
                .map_retry(|retry| SendEntry::Abort {
                    flags: flags,
                    retry: retry
                }),
            SendEntry::Parties { msgs, batch, retry } => {
                match stream.retry_add_parties(ctx, &batch, retry) {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => {
                        Self::try_add(ctx, stream, msgs, batch)
                    }
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(SendEntry::Parties {
                            msgs: msgs,
                            batch: batch,
                            retry: retry
                        })
                    }
                    Err(err) => Self::complete_add_parties(
                        ctx, stream, msgs, batch, err
                    )
                }
            }
            SendEntry::Add {
                msgs,
                msg,
                batch,
                mut flags,
                retry
            } => match stream.retry_add(ctx, &mut flags, &msg, &batch, retry) {
                // It succeeded.
                Ok(RetryResult::Success(_)) => {
                    Self::try_add(ctx, stream, msgs, batch)
                }
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(SendEntry::Add {
                        msgs: msgs,
                        msg: msg,
                        flags: flags,
                        batch: batch,
                        retry: retry
                    })
                }
                Err(err) => Self::complete_add(
                    ctx, stream, flags, msgs, msg, batch, err
                )
            },
            SendEntry::Finish { batch, retry } => {
                let mut flags = stream.empty_flags();

                match stream.retry_finish_batch(ctx, &mut flags, &batch, retry)
                {
                    // It succeeded.
                    Ok(RetryResult::Success(_)) => RetryResult::Success(()),
                    // We got a retry.
                    Ok(RetryResult::Retry(retry)) => {
                        RetryResult::Retry(SendEntry::Finish {
                            batch: batch,
                            retry: retry
                        })
                    }
                    Err(err) => Self::complete_finish(
                        ctx, stream, &mut flags, batch, err
                    )
                }
            }
            SendEntry::Cancel {
                batch,
                retry,
                mut flags
            } => match stream.retry_cancel_batch(ctx, &mut flags, &batch, retry)
            {
                // It succeeded.
                Ok(RetryResult::Success(_)) => RetryResult::Success(()),
                // We got a retry.
                Ok(RetryResult::Retry(retry)) => {
                    RetryResult::Retry(SendEntry::Cancel {
                        batch: batch,
                        retry: retry,
                        flags: flags
                    })
                }
                Err(err) => {
                    Self::complete_cancel_batch(ctx, stream, flags, batch, err)
                }
            }
        }
    }

    #[inline]
    fn from_try_send(
        ctx: &mut Ctx,
        stream: &mut Stream,
        parties: Vec<Stream::PartyID>,
        msgs: Vec<Msg>
    ) -> RetryResult<(), Self> {
        Self::try_start_batch(ctx, stream, parties, msgs)
    }
}

impl<R, RoundID, P, Stream, Oper, Msg, Out, Ctx>
    SendThread<R, RoundID, P, Stream, Oper, Msg, Out, Ctx>
where
    RoundID: 'static + Clone + Display + Ord + Send,
    R: 'static + Rounds<RoundID, PartyStreamIdx, Oper, Msg, Out> + Send,
    P: 'static + PartiesMap<RoundID, Out::PartyID, PartyStreamIdx> + Send,
    Out: 'static + Outbound<RoundID, Msg> + Send,
    Stream: 'static
        + PushStreamReportBatchError<
            <Stream::FinishBatchError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportError<
            <Stream::StartBatchError as BatchError>::Permanent
        >
        + PushStreamReportBatchError<
            <Stream::AddError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamReportBatchError<
            <Stream::AddPartiesError as BatchError>::Permanent,
            Stream::BatchID
        >
        + PushStreamSharedSingle<Msg, Ctx>
        + PushStreamShared<PartyID = PartyStreamIdx>
        + PushStreamParties
        + Send,
    Stream::StartBatchStreamBatches: Send,
    Stream::StartBatchRetry: Send,
    Stream::AbortBatchRetry: Send,
    Stream::AddPartiesRetry: Send,
    Stream::AddRetry: Send,
    Stream::FinishBatchRetry: Send,
    Stream::CancelBatchRetry: Send,
    Stream::StreamFlags: Send,
    Stream::PartyID: From<usize> + Send,
    Stream::BatchID: Send,
    Msg: 'static + Clone + RoundMsg<RoundID> + Send,
    Ctx: 'static + NSNameCachesCtx + Send + Sync,
    Oper: 'static + Send
{
    #[inline]
    pub(crate) fn create(
        ctx: Ctx,
        rounds: R,
        notify: Notify,
        stream: Stream,
        shutdown: ShutdownFlag
    ) -> Self {
        // ISSUE #8: do size hinting.
        SendThread {
            oper: PhantomData,
            round_id: PhantomData,
            parties: PhantomData,
            out: PhantomData,
            pending: Vec::new(),
            rounds: rounds,
            notify: notify,
            shutdown: shutdown,
            stream: stream,
            ctx: ctx
        }
    }

    #[inline]
    pub(crate) fn notify(&self) -> Notify {
        self.notify.clone()
    }

    fn update_from_outbound(&mut self) -> Option<Instant> {
        debug!(target: "consensus-component-send-thread",
               "fetching new outbound messages");

        // ISSUE #8: figure out a good size hint here.
        let mut group_map = HashMap::new();

        let next = match self.rounds.collect_outbound(|parties_map, group| {
            let mut party_idxs: Vec<PartyStreamIdx> =
                group.iter(parties_map).cloned().collect();

            party_idxs.sort();

            match group_map.entry(party_idxs) {
                Entry::Vacant(ent) => {
                    ent.insert(vec![group.msg().clone()]);
                }
                Entry::Occupied(mut ent) => {
                    ent.get_mut().push(group.msg().clone());
                }
            }
        }) {
            Ok(out) => out,
            Err(err) => {
                error!(target: "consensus-component-send-thread",
                       "unrecoverable error collecting outbounds: {}",
                       err);

                return None;
            }
        };

        // Go through each group and try sending it
        for (parties, msgs) in group_map.into_iter() {
            if let RetryResult::Retry(retry) = SendEntry::from_try_send(
                &mut self.ctx,
                &mut self.stream,
                parties,
                msgs
            ) {
                // We got a retry somewhere along the process, store it.
                self.pending.push(retry)
            }
        }

        next
    }

    fn retry_pending(
        &mut self,
        now: Instant
    ) -> Option<Instant> {
        debug!(target: "consensus-component-send-thread",
               "retrying pending operations");

        let mut curr = Vec::with_capacity(self.pending.len());

        // First, sort the array by times, but reverse the order so we
        // can pop the earliest.
        self.pending
            .sort_unstable_by_key(|b| std::cmp::Reverse(b.when()));

        while self.pending.last().map_or(false, |ent| ent.when() <= now) {
            match self.pending.pop() {
                Some(ent) => {
                    curr.push(ent);
                }
                None => {
                    error!(target: "consensus-component-send-thread",
                           "pop should not be empty");
                }
            }
        }

        // The last entry should now be the first time past the present.
        let out = self.pending.last().map(|ent| ent.when());

        // Try running all the entries we collected.
        for ent in curr.into_iter() {
            if let RetryResult::Retry(retry) =
                ent.exec(&mut self.ctx, &mut self.stream)
            {
                // We got a retry somewhere along the process, store it.
                self.pending.push(retry)
            }
        }

        out
    }

    fn run(mut self) {
        let mut next_outbound = Some(Instant::now());
        let mut next_pending = None;
        let mut valid = true;

        info!(target: "consensus-component-send-thread",
              "consensus component send thread starting");

        while valid && self.shutdown.is_live() {
            let now = Instant::now();

            if let Some(when) = next_outbound &&
                when <= now
            {
                next_outbound = self.update_from_outbound()
            }

            if let Some(next) = next_pending &&
                next <= now
            {
                next_pending = self.retry_pending(now)
            }

            match next_pending.map_or(next_outbound, |next| {
                next_outbound.map(|when| when.max(next))
            }) {
                Some(when) => {
                    let now = Instant::now();

                    if now < when {
                        let duration = when - now;

                        trace!(target: "consensus-component-send-thread",
                               "next activity at {}.{:03}",
                               duration.as_secs(), duration.subsec_millis());

                        match self.notify.wait_timeout(duration) {
                            Ok(notify) => {
                                if notify {
                                    next_outbound = Some(now)
                                }
                            },
                            Err(err) => {
                                error!(target: "consensus-component-send-thread",
                                       "error waiting for notification: {}",
                                       err);

                                valid = false
                            }
                        }
                    }
                }
                None => {
                    trace!(target: "consensus-component-send-thread",
                           "waiting for notification indefinitely");

                    match self.notify.wait() {
                        Ok(_) => next_outbound = Some(now),
                        Err(err) => {
                            error!(target: "consensus-component-send-thread",
                                   "error waiting for notification: {}",
                                   err);

                            valid = false
                        }
                    }
                }
            }
        }

        debug!(target: "consensus-component-send-thread",
               "consensus component send thread exiting");
    }

    pub(crate) fn start(self) -> JoinHandle<()> {
        spawn(move || self.run())
    }
}
