use std::collections::HashMap;

use bytes::BytesMut;

use shrikedb_facade::conn_context::ConnContext;
use shrikedb_facade::reply_builder::ReplyBuilder;
use shrikedb_facade::resp_parser::RespExpr;

use crate::engine_shard::EngineShard;

/// Context passed to command handlers.
pub struct CommandContext<'a> {
    /// The parsed command arguments (args[0] is the command name).
    pub args: &'a [RespExpr],
    /// Per-connection context (selected DB, etc.).
    pub conn_ctx: &'a mut ConnContext,
    /// The shard's engine (owns database storage).
    pub shard: &'a mut EngineShard,
    /// Reply builder for writing the response.
    pub reply: ReplyBuilder<'a>,
}

/// Type alias for command handler functions.
pub type CommandHandler = fn(ctx: CommandContext<'_>);

/// Metadata about a command.
pub struct CommandEntry {
    pub name: &'static str,
    pub handler: CommandHandler,
    /// Arity: positive = exact, negative = minimum. E.g., -2 means at least 2 args.
    pub arity: i16,
    /// Index of the first key argument (1-based, 0 = no keys).
    pub first_key: u16,
    /// Index of the last key argument (1-based, 0 = same as first_key, -1 = last arg).
    pub last_key: i16,
    /// Step between key arguments.
    pub key_step: u16,
}

/// Registry mapping command names to their handlers and metadata.
pub struct CommandRegistry {
    commands: HashMap<&'static str, CommandEntry>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self {
            commands: HashMap::new(),
        }
    }

    pub fn register(&mut self, entry: CommandEntry) {
        self.commands.insert(entry.name, entry);
    }

    pub fn find(&self, name: &str) -> Option<&CommandEntry> {
        self.commands.get(name)
    }

    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    /// Dispatch a parsed command. Returns false if the connection should close (QUIT).
    pub fn dispatch(
        &self,
        args: &[RespExpr],
        conn_ctx: &mut ConnContext,
        shard: &mut EngineShard,
        reply_buf: &mut BytesMut,
    ) -> bool {
        if args.is_empty() {
            ReplyBuilder::new(reply_buf).send_err("empty command");
            return true;
        }

        let cmd_name = match args[0].as_bytes() {
            Some(b) => b,
            None => {
                ReplyBuilder::new(reply_buf).send_err("invalid command");
                return true;
            }
        };

        // Convert to uppercase for case-insensitive matching
        let upper: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_uppercase()).collect();
        let upper_str = match std::str::from_utf8(&upper) {
            Ok(s) => s,
            Err(_) => {
                ReplyBuilder::new(reply_buf).send_err("invalid command encoding");
                return true;
            }
        };

        // Handle QUIT specially
        if upper_str == "QUIT" {
            ReplyBuilder::new(reply_buf).send_ok();
            return false;
        }

        match self.find(upper_str) {
            Some(entry) => {
                // Check arity
                let argc = args.len() as i16;
                if entry.arity > 0 && argc != entry.arity {
                    ReplyBuilder::new(reply_buf).send_err(&format!(
                        "wrong number of arguments for '{}' command",
                        upper_str.to_lowercase()
                    ));
                    return true;
                }
                if entry.arity < 0 && argc < -entry.arity {
                    ReplyBuilder::new(reply_buf).send_err(&format!(
                        "wrong number of arguments for '{}' command",
                        upper_str.to_lowercase()
                    ));
                    return true;
                }

                shard.stats.commands_processed += 1;
                let ctx = CommandContext {
                    args,
                    conn_ctx,
                    shard,
                    reply: ReplyBuilder::new(reply_buf),
                };
                (entry.handler)(ctx);
            }
            None => {
                let name_lower = upper_str.to_lowercase();
                ReplyBuilder::new(reply_buf).send_err(&format!(
                    "unknown command '{}', with args beginning with: ",
                    name_lower
                ));
            }
        }
        true
    }
}

impl Default for CommandRegistry {
    fn default() -> Self {
        Self::new()
    }
}
