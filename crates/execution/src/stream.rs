//! Record-batch stream abstractions and channel adapters.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use ffq_common::Result;
use futures::Stream;
use futures::channel::mpsc;

/// A stream of RecordBatches that also knows its output schema.
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> + Send {
    /// Output schema for every batch yielded by this stream.
    fn schema(&self) -> SchemaRef;
}

/// The standard "stream you can return from operators".
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream>>;

/// Adapter that attaches a schema to any `Stream<Item = Result<RecordBatch>>`.
pub struct StreamAdapter<S> {
    schema: SchemaRef,
    inner: S,
}

impl<S> StreamAdapter<S> {
    /// Create a new schema-attached stream adapter.
    pub fn new(schema: SchemaRef, inner: S) -> Self {
        Self { schema, inner }
    }
}

impl<S> RecordBatchStream for StreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>> + Send + Unpin + 'static,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl<S> Stream for StreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Create an empty stream (useful for stubs or early returns).
pub fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
    let inner = futures::stream::empty::<Result<RecordBatch>>();
    Box::pin(StreamAdapter::new(schema, inner))
}

/// Create a stream backed by a bounded channel.
///
/// Backpressure: when the receiver is slow and the buffer fills up,
/// `sender.send(..).await` will wait until there is capacity again.
pub fn bounded_batch_channel(
    schema: SchemaRef,
    capacity: usize,
) -> (BatchSender, SendableRecordBatchStream) {
    let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(capacity);
    let stream = Box::pin(StreamAdapter::new(schema, rx));
    (BatchSender { tx }, stream)
}

/// Sender side for `bounded_batch_channel`.
#[derive(Clone)]
pub struct BatchSender {
    tx: mpsc::Sender<Result<RecordBatch>>,
}

impl BatchSender {
    /// Send a batch (awaits if the channel buffer is full).
    pub async fn send_batch(&mut self, batch: RecordBatch) -> Result<()> {
        use futures::SinkExt;
        self.tx
            .send(Ok(batch))
            .await
            .map_err(|e| ffq_common::FfqError::Execution(format!("batch channel closed: {e}")))
    }

    /// Send an error and terminate downstream consumption.
    pub async fn send_error(&mut self, err: ffq_common::FfqError) -> Result<()> {
        use futures::SinkExt;
        self.tx
            .send(Err(err))
            .await
            .map_err(|e| ffq_common::FfqError::Execution(format!("batch channel closed: {e}")))
    }
}
