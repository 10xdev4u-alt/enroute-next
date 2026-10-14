//! Reads a run's own spans back out, in the process that made them.
//!
//! The only other consumer of Enroute's spans is the OTLP exporter, so a
//! harness that could not read them here would need a trace backend to see
//! where its time went.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// What one key accumulated over a single run.
#[derive(Debug, Default, Clone)]
pub struct SpanTotals {
    /// How many spans closed under this key.
    pub count: u64,
    /// Summed creation-to-close, so it exceeds the run's wall time for spans
    /// that ran concurrently — that ratio is how much concurrency they got.
    pub wall: Duration,
}

/// Aggregates every span that closes while it is installed, keyed by the
/// span's `op` field, or by its name where it carries none.
///
/// Cloneable and shared, so the subscriber's layer and the harness's handle
/// are one map.
#[derive(Debug, Clone, Default)]
pub struct Collector {
    totals: Arc<Mutex<BTreeMap<String, SpanTotals>>>,
}

impl Collector {
    /// A snapshot of what has closed so far.
    #[must_use]
    pub fn snapshot(&self) -> BTreeMap<String, SpanTotals> {
        self.totals().clone()
    }

    fn totals(&self) -> MutexGuard<'_, BTreeMap<String, SpanTotals>> {
        // The map is only ever inserted into, so a poisoned lock loses
        // measurements, not correctness.
        self.totals.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S> Layer<S> for Collector
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut op = Op::default();
        attrs.record(&mut op);
        let mut extensions = span.extensions_mut();
        extensions.insert(Started(Instant::now()));
        extensions.insert(op);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let extensions = span.extensions();
        let Some(started) = extensions.get::<Started>() else {
            return;
        };
        let elapsed = started.0.elapsed();
        let key = extensions
            .get::<Op>()
            .and_then(|op| op.0.clone())
            .unwrap_or_else(|| span.name().to_owned());
        let mut totals = self.totals();
        let entry = totals.entry(key).or_default();
        entry.count += 1;
        entry.wall += elapsed;
    }
}

/// The span's `op` field.
#[derive(Debug, Default)]
struct Op(Option<String>);

impl Visit for Op {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "op" {
            // A `Display` value arrives here too, and quotes a plain string.
            self.0 = Some(format!("{value:?}").trim_matches('"').to_string());
        }
    }
}

#[derive(Debug)]
struct Started(Instant);
