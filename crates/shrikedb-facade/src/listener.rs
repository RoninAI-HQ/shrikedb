use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::connection::{handle_connection_async, handle_connection_ex, AsyncDispatcher, CommandDispatcherEx};

pub struct Listener {
    addr: String,
}

impl Listener {
    pub fn new(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }

    /// Run with synchronous dispatch (legacy).
    pub async fn run_sync(&self, dispatcher: CommandDispatcherEx) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        tracing::info!("Listening on {}", self.addr);
        loop {
            let (stream, addr) = listener.accept().await?;
            tracing::debug!("Accepted connection from {}", addr);
            let d = Arc::clone(&dispatcher);
            tokio::spawn(async move {
                handle_connection_ex(stream, d).await;
            });
        }
    }

    /// Run with async dispatch (old multi-shard model).
    pub async fn run_async(&self, dispatcher: AsyncDispatcher) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        tracing::info!("Listening on {}", self.addr);
        loop {
            let (stream, addr) = listener.accept().await?;
            tracing::debug!("Accepted connection from {}", addr);
            let d = Arc::clone(&dispatcher);
            tokio::spawn(async move {
                handle_connection_async(stream, d).await;
            });
        }
    }

    /// Lightweight accept loop that distributes connections round-robin to shard threads.
    pub async fn run_distributed(
        &self,
        shard_senders: Vec<mpsc::Sender<TcpStream>>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        tracing::info!("Listening on {}", self.addr);

        let num_shards = shard_senders.len();
        let mut next = 0usize;

        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    // Round-robin to shard threads
                    let _ = shard_senders[next].send(stream).await;
                    next = (next + 1) % num_shards;
                }
                Err(e) => {
                    tracing::error!("Accept error: {}", e);
                }
            }
        }
    }
}
