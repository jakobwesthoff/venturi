//! A `LISTEN`-based [`Notifier`] over a dedicated PostgreSQL connection.
//!
//! deadpool hides each pooled connection's message stream, so notifications
//! cannot be received on a pooled client. The notifier therefore owns a separate
//! `tokio_postgres` connection, holds its client alive to keep the connection
//! open, and forwards each `NOTIFY` as a wakeup. If the connection drops, the next
//! `recv` reconnects and returns so the worker re-polls (covering any notification
//! missed while disconnected).
//!
//! The dedicated connection is built through a [`ListenFactory`]: a type-erased
//! closure that captures the store's connection config and TLS connector. Erasing
//! the generic `MakeTlsConnect` connector behind the closure lets the non-generic
//! `PostgresStore` rebuild the listening connection with whatever TLS the rest of
//! the adapter uses, so push wakeups work for every deployment, plaintext or TLS.

use crate::error::Error;
use crate::store::Notifier;
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TrySendError;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{AsyncMessage, Client, Socket};

/// Builds a dedicated, listening `tokio_postgres` connection for a channel.
///
/// Given a channel name, the factory connects, starts forwarding that
/// connection's notifications as unit wakeups, issues the `LISTEN`, and returns
/// the live client together with the wakeup receiver. The concrete `Client` and
/// receiver in the return type erase the connector's generic parameters, so the
/// same factory value works for any TLS stack.
pub(crate) type ListenFactory = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<(Client, Receiver<()>), Error>> + Send>>
        + Send
        + Sync,
>;

/// Build a [`ListenFactory`] that connects with `config` and `tls`.
///
/// The generic connector lives only here; the returned factory is fully
/// type-erased. `config` and `tls` are cloned for each connection attempt, so the
/// factory can rebuild the connection on reconnect.
pub(crate) fn make_listen_factory<T>(config: tokio_postgres::Config, tls: T) -> ListenFactory
where
    T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    T::Stream: Send + Sync,
    T::TlsConnect: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    Arc::new(move |channel: String| {
        let config = config.clone();
        let tls = tls.clone();
        Box::pin(async move { listen(config, tls, &channel).await })
    })
}

/// A notifier backed by a dedicated `LISTEN` connection.
pub(crate) struct PgNotifier {
    factory: ListenFactory,
    channel: String,
    // Kept alive so the listening connection stays open; replaced on reconnect.
    client: Client,
    wakeups: Receiver<()>,
}

impl PgNotifier {
    /// Connect a dedicated listener through `factory` and `LISTEN` on `channel`.
    pub(crate) async fn connect(
        factory: ListenFactory,
        channel: String,
    ) -> Result<PgNotifier, Error> {
        let (client, wakeups) = factory(channel.clone()).await?;
        Ok(PgNotifier {
            factory,
            channel,
            client,
            wakeups,
        })
    }

    /// Rebuild the listening connection after a drop, returning once it is back
    /// (or after a short pause if it cannot be re-established yet).
    async fn reconnect(&mut self) {
        match (self.factory)(self.channel.clone()).await {
            Ok((client, wakeups)) => {
                self.client = client;
                self.wakeups = wakeups;
            }
            Err(error) => {
                tracing::warn!(%error, "listen reconnect failed; will rely on polling");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

#[async_trait]
impl Notifier for PgNotifier {
    async fn recv(&mut self) {
        match self.wakeups.recv().await {
            // A real notification.
            Some(()) => {}
            // The connection closed: reconnect and return so the worker re-polls.
            None => self.reconnect().await,
        }
    }
}

/// Open a connection with `config`/`tls`, start forwarding its notifications, and
/// `LISTEN` on `channel`.
async fn listen<T>(
    config: tokio_postgres::Config,
    tls: T,
    channel: &str,
) -> Result<(Client, Receiver<()>), Error>
where
    T: MakeTlsConnect<Socket> + Send + 'static,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    let (client, mut connection) = config.connect(tls).await?;
    // A capacity-of-one channel coalesces wakeups: the worker re-queries the
    // full claimable set on each wake, so a single pending wakeup already covers
    // any number of notifications that arrive while it is busy. Bounding the
    // channel keeps a burst of NOTIFYs from accumulating an unbounded backlog of
    // redundant wakeups.
    let (tx, rx) = tokio::sync::mpsc::channel(1);

    // The connection task drives the protocol and forwards each notification as a
    // unit wakeup. It ends when the connection closes or the receiver is dropped.
    tokio::spawn(async move {
        loop {
            let message = std::future::poll_fn(|cx| connection.poll_message(cx)).await;
            match message {
                Some(Ok(AsyncMessage::Notification(_))) => match tx.try_send(()) {
                    // Delivered, or a wakeup is already pending and subsumes this
                    // one; either way the worker will re-poll.
                    Ok(()) | Err(TrySendError::Full(())) => {}
                    // The receiver is gone, so nothing will consume wakeups.
                    Err(TrySendError::Closed(())) => break,
                },
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    });

    // The channel name is the validated prefix plus a fixed suffix, so it is a
    // safe identifier to interpolate.
    client.batch_execute(&format!("LISTEN {channel}")).await?;
    Ok((client, rx))
}
