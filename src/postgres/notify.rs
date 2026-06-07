//! A `LISTEN`-based [`Notifier`] over a dedicated PostgreSQL connection.
//!
//! deadpool hides each pooled connection's message stream, so notifications
//! cannot be received on a pooled client. The notifier therefore owns a separate
//! `tokio_postgres` connection, holds its client alive to keep the connection
//! open, and forwards each `NOTIFY` as a wakeup. If the connection drops, the next
//! `recv` reconnects and returns so the worker re-polls (covering any
//! notification missed while disconnected). Listening is opt-in and currently
//! `NoTls` only; without it, the worker relies on its bounded poll.

use crate::error::Error;
use crate::store::Notifier;
use async_trait::async_trait;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio_postgres::{AsyncMessage, NoTls};

/// A notifier backed by a dedicated `LISTEN` connection.
pub(crate) struct PgNotifier {
    dsn: String,
    channel: String,
    // Kept alive so the listening connection stays open; replaced on reconnect.
    client: tokio_postgres::Client,
    wakeups: UnboundedReceiver<()>,
}

impl PgNotifier {
    /// Connect a dedicated listener on `dsn` and `LISTEN` on `channel`.
    pub(crate) async fn connect(dsn: &str, channel: &str) -> Result<PgNotifier, Error> {
        let (client, wakeups) = listen(dsn, channel).await?;
        Ok(PgNotifier {
            dsn: dsn.to_owned(),
            channel: channel.to_owned(),
            client,
            wakeups,
        })
    }

    /// Rebuild the listening connection after a drop, returning once it is back
    /// (or after a short pause if it cannot be re-established yet).
    async fn reconnect(&mut self) {
        match listen(&self.dsn, &self.channel).await {
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

/// Open a connection, start forwarding its notifications, and `LISTEN`.
async fn listen(
    dsn: &str,
    channel: &str,
) -> Result<(tokio_postgres::Client, UnboundedReceiver<()>), Error> {
    let (client, mut connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let (tx, rx) = unbounded_channel();

    // The connection task drives the protocol and forwards each notification as a
    // unit wakeup. It ends when the connection closes or the receiver is dropped.
    tokio::spawn(async move {
        loop {
            let message = std::future::poll_fn(|cx| connection.poll_message(cx)).await;
            match message {
                Some(Ok(AsyncMessage::Notification(_))) => {
                    if tx.send(()).is_err() {
                        break;
                    }
                }
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
