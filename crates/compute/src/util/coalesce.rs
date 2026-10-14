//! Concurrent calls that share one answer, as etcd's linearizable read loop
//! shares a read index among the reads that wait for it.
//!
//! A call joins the pending round. The round leaves the pending slot just
//! before it asks, so each call that it answers began before the question:
//! the answer is as fresh as a call of its own. A call that comes while a
//! round is asking joins the next round, which asks once the first one is
//! answered. One question is in flight at a time, however many calls wait.

use std::sync::{Arc, Weak};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use parking_lot::Mutex;

/// The answer of a round: every call that shared it gets the same.
pub(crate) type Answer<T> = Result<T, SharedError>;

type Round<T> = Shared<BoxFuture<'static, Answer<T>>>;

/// Calls of one question (a read barrier on one repository, say).
pub(crate) struct Coalesce<T: Clone> {
    /// The round that calls join now; it asks once `asking` is free.
    pending: Mutex<Option<Round<T>>>,
    /// Held by the round that is asking.
    asking: Arc<tokio::sync::Mutex<()>>,
}

impl<T: Clone + Send + Sync + 'static> Coalesce<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Coalesce {
            pending: Mutex::new(None),
            asking: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// The answer of a round that asks `ask` after this call began. `ask`
    /// runs only if this call starts the round.
    pub(crate) async fn call<F>(self: &Arc<Self>, ask: impl FnOnce() -> F) -> Answer<T>
    where
        F: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let round = self
            .pending
            .lock()
            .get_or_insert_with(|| {
                let question = ask().boxed();
                Self::round(Arc::downgrade(self), self.asking.clone(), question)
            })
            .clone();
        round.await
    }

    fn round(
        me: Weak<Self>,
        asking: Arc<tokio::sync::Mutex<()>>,
        question: BoxFuture<'static, anyhow::Result<T>>,
    ) -> Round<T> {
        async move {
            let _asking = asking.lock_owned().await;
            // Calls from now on start the next round.
            if let Some(me) = me.upgrade() {
                me.pending.lock().take();
            }
            question.await.map_err(|e| SharedError(Arc::new(e)))
        }
        .boxed()
        .shared()
    }
}

/// An error that every call of a round gets. Its chain is the original
/// error's, so callers can still tell kinds apart.
#[derive(Clone, Debug)]
pub(crate) struct SharedError(Arc<anyhow::Error>);

impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl std::error::Error for SharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some((*self.0).as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use tokio::sync::Semaphore;
    use tokio::task::JoinHandle;

    use super::*;

    /// A question that counts how often it is asked, and answers with the
    /// count once `open` lets it.
    fn counting(
        asked: Arc<AtomicU64>,
        open: Arc<Semaphore>,
    ) -> impl FnOnce() -> BoxFuture<'static, anyhow::Result<u64>> {
        move || {
            async move {
                let n = asked.fetch_add(1, Ordering::SeqCst) + 1;
                open.acquire().await?.forget();
                Ok(n)
            }
            .boxed()
        }
    }

    /// A call of `c` with a [`counting`] question, in a task of its own.
    fn spawn_call(
        c: &Arc<Coalesce<u64>>,
        asked: &Arc<AtomicU64>,
        open: &Arc<Semaphore>,
    ) -> JoinHandle<u64> {
        let (c, ask) = (c.clone(), counting(asked.clone(), open.clone()));
        tokio::spawn(async move { c.call(ask).await.unwrap() })
    }

    async fn until_asked(asked: &AtomicU64) {
        while asked.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn concurrent_calls_share_two_questions_at_most() {
        let (c, asked, open) = (Coalesce::new(), Arc::default(), Arc::new(Semaphore::new(0)));
        let calls: Vec<_> = (0..50).map(|_| spawn_call(&c, &asked, &open)).collect();
        until_asked(&asked).await;
        // The round asking now, and one for every call that came after it.
        open.add_permits(2);
        for call in calls {
            assert!((1..=2).contains(&call.await.unwrap()));
        }
        assert!(asked.load(Ordering::SeqCst) <= 2);
    }

    #[tokio::test]
    async fn a_call_during_a_question_waits_for_the_next() {
        let (c, asked, open) = (Coalesce::new(), Arc::default(), Arc::new(Semaphore::new(0)));
        let first = spawn_call(&c, &asked, &open);
        until_asked(&asked).await;
        let late: Vec<_> = (0..10).map(|_| spawn_call(&c, &asked, &open)).collect();
        tokio::task::yield_now().await;
        // One question at a time: the next round waits for the first.
        assert_eq!(asked.load(Ordering::SeqCst), 1);
        open.add_permits(2);
        assert_eq!(first.await.unwrap(), 1);
        for call in late {
            assert_eq!(call.await.unwrap(), 2);
        }
        assert_eq!(asked.load(Ordering::SeqCst), 2);
    }

    #[derive(Debug)]
    struct Kind;

    impl std::fmt::Display for Kind {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("kind")
        }
    }

    impl std::error::Error for Kind {}

    fn failing() -> BoxFuture<'static, anyhow::Result<u64>> {
        async { Err(anyhow::Error::new(Kind).context("asking")) }.boxed()
    }

    fn seven() -> BoxFuture<'static, anyhow::Result<u64>> {
        async { Ok(7) }.boxed()
    }

    #[tokio::test]
    async fn every_call_of_a_round_gets_its_error() {
        let c = Coalesce::new();
        let e = anyhow::Error::new(c.call(failing).await.unwrap_err());
        assert!(e.chain().any(<dyn std::error::Error>::is::<Kind>));
        // The failed round is gone: the next call asks again.
        assert_eq!(c.call(seven).await.unwrap(), 7);
    }
}
