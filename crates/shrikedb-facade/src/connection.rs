use bytes::BytesMut;
use std::future::Future;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::conn_context::ConnContext;
use crate::reply_builder::ReplyBuilder;
use crate::resp_parser::{ParseResult, RespArgs, RespExpr, RespParser};

/// Synchronous command handler (Phase 2 style).
pub type CommandDispatcher =
    Arc<dyn Fn(&[RespExpr], &mut ConnContext, &mut ReplyBuilder<'_>) + Send + Sync>;

/// Extended synchronous handler that returns false to close connection.
pub type CommandDispatcherEx =
    Arc<dyn Fn(&[RespExpr], &mut ConnContext, &mut BytesMut) -> bool + Send + Sync>;

/// Async command handler for multi-shard dispatch.
/// Returns (reply_bytes, keep_alive).
pub type AsyncDispatcher =
    Arc<dyn Fn(RespArgs, u16) -> std::pin::Pin<Box<dyn Future<Output = (BytesMut, bool)> + Send>> + Send + Sync>;

/// Handle a connection with synchronous dispatch (legacy).
pub async fn handle_connection(stream: TcpStream, dispatcher: CommandDispatcher) {
    let ex_dispatcher: CommandDispatcherEx = Arc::new(move |args, ctx, buf| {
        let mut rb = ReplyBuilder::new(buf);
        dispatcher(args, ctx, &mut rb);
        true
    });
    handle_connection_ex(stream, ex_dispatcher).await;
}

/// Handle a connection with synchronous extended dispatch.
pub async fn handle_connection_ex(mut stream: TcpStream, dispatcher: CommandDispatcherEx) {
    let mut parser = RespParser::new();
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut ctx = ConnContext::new();

    loop {
        loop {
            match parser.parse(&mut read_buf) {
                ParseResult::Complete(args) => {
                    if !dispatcher(&args, &mut ctx, &mut write_buf) {
                        let _ = stream.write_all(&write_buf).await;
                        return;
                    }
                }
                ParseResult::Incomplete => break,
                ParseResult::Error(e) => {
                    let mut rb = ReplyBuilder::new(&mut write_buf);
                    rb.send_err(&format!("Protocol error: {}", e));
                    let _ = stream.write_all(&write_buf).await;
                    return;
                }
            }
        }

        if !write_buf.is_empty() {
            if stream.write_all(&write_buf).await.is_err() {
                return;
            }
            write_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

/// Handle a connection with async dispatch (multi-shard mode).
pub async fn handle_connection_async(mut stream: TcpStream, dispatcher: AsyncDispatcher) {
    let mut parser = RespParser::new();
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut ctx = ConnContext::new();

    loop {
        // Parse and dispatch all complete commands
        loop {
            match parser.parse(&mut read_buf) {
                ParseResult::Complete(args) => {
                    // Handle SELECT locally since it modifies connection state
                    if is_select_command(&args) {
                        handle_select(&args, &mut ctx, &mut write_buf);
                        continue;
                    }

                    let db_index = ctx.db_index;
                    let (reply, keep_alive) = dispatcher(args, db_index).await;
                    write_buf.extend_from_slice(&reply);
                    if !keep_alive {
                        let _ = stream.write_all(&write_buf).await;
                        return;
                    }
                }
                ParseResult::Incomplete => break,
                ParseResult::Error(e) => {
                    let mut rb = ReplyBuilder::new(&mut write_buf);
                    rb.send_err(&format!("Protocol error: {}", e));
                    let _ = stream.write_all(&write_buf).await;
                    return;
                }
            }
        }

        if !write_buf.is_empty() {
            if stream.write_all(&write_buf).await.is_err() {
                return;
            }
            write_buf.clear();
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

fn is_select_command(args: &[RespExpr]) -> bool {
    if let Some(cmd) = args.first().and_then(|a| a.as_bytes()) {
        cmd.eq_ignore_ascii_case(b"SELECT")
    } else {
        false
    }
}

fn handle_select(args: &[RespExpr], ctx: &mut ConnContext, buf: &mut BytesMut) {
    let mut rb = ReplyBuilder::new(buf);
    if args.len() != 2 {
        rb.send_err("wrong number of arguments for 'select' command");
        return;
    }
    let idx = args[1]
        .as_bytes()
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<u16>().ok());
    match idx {
        Some(n) if n < 16 => {
            ctx.db_index = n;
            rb.send_ok();
        }
        _ => rb.send_err("DB index is out of range"),
    }
}
