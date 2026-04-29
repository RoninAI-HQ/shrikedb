use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;

/// Parsed args type: inline storage for up to 6 args (covers most commands).
pub type RespArgs = SmallVec<[RespExpr; 6]>;

/// A parsed RESP expression. Uses `Bytes` for zero-copy sharing of the read buffer.
#[derive(Debug, Clone, PartialEq)]
pub enum RespExpr {
    /// Bulk string or simple string — zero-copy reference into the read buffer.
    String(Bytes),
    /// Integer value.
    Int(i64),
    /// Null bulk string.
    Nil,
    /// Error string.
    Error(Bytes),
    /// Array of expressions.
    Array(Vec<RespExpr>),
}

impl RespExpr {
    /// Get the expression as a byte slice (for String variant).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RespExpr::String(b) => Some(b.as_ref()),
            _ => None,
        }
    }

    /// Get the expression as a str (for String variant, if valid UTF-8).
    pub fn as_str(&self) -> Option<&str> {
        self.as_bytes().and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Create a String variant from a static byte slice.
    pub fn from_static(s: &'static [u8]) -> Self {
        RespExpr::String(Bytes::from_static(s))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    BadArrayLen,
    BadBulkLen,
    BadInteger,
    BadProtocol,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::BadArrayLen => write!(f, "invalid array length"),
            ParseError::BadBulkLen => write!(f, "invalid bulk string length"),
            ParseError::BadInteger => write!(f, "invalid integer"),
            ParseError::BadProtocol => write!(f, "protocol error"),
        }
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug)]
pub enum ParseResult {
    /// Successfully parsed a complete command.
    Complete(RespArgs),
    /// Need more data to finish parsing.
    Incomplete,
    /// Protocol error.
    Error(ParseError),
}

/// Parser state for incremental RESP2 parsing.
#[derive(Debug)]
pub struct RespParser {
    state: State,
    max_arr_len: u32,
    max_bulk_len: u64,
}

#[derive(Debug)]
enum State {
    Init,
    Array {
        expected: usize,
        args: RespArgs,
    },
    BulkString {
        expected: usize,
        args: RespArgs,
        array_expected: usize,
    },
    Inline,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            state: State::Init,
            max_arr_len: 1024 * 1024,
            max_bulk_len: 512 * 1024 * 1024,
        }
    }

    pub fn parse(&mut self, buf: &mut BytesMut) -> ParseResult {
        loop {
            match std::mem::replace(&mut self.state, State::Init) {
                State::Init => {
                    if buf.is_empty() {
                        return ParseResult::Incomplete;
                    }
                    match buf[0] {
                        b'*' => {
                            match consume_line(buf) {
                                Some(line) => {
                                    let len = match parse_int(&line[1..]) {
                                        Some(n) if n >= 0 && n <= self.max_arr_len as i64 => {
                                            n as usize
                                        }
                                        _ => return ParseResult::Error(ParseError::BadArrayLen),
                                    };
                                    if len == 0 {
                                        return ParseResult::Complete(SmallVec::new());
                                    }
                                    self.state = State::Array {
                                        expected: len,
                                        args: SmallVec::with_capacity(len),
                                    };
                                }
                                None => {
                                    self.state = State::Init;
                                    return ParseResult::Incomplete;
                                }
                            }
                        }
                        b'\r' | b'\n' => {
                            let _ = buf.split_to(1);
                            self.state = State::Init;
                        }
                        _ => {
                            self.state = State::Inline;
                        }
                    }
                }
                State::Inline => match consume_line(buf) {
                    Some(line) => {
                        let args = parse_inline_args(&line);
                        if args.is_empty() {
                            self.state = State::Init;
                        } else {
                            return ParseResult::Complete(args);
                        }
                    }
                    None => {
                        self.state = State::Inline;
                        return ParseResult::Incomplete;
                    }
                },
                State::Array {
                    expected,
                    mut args,
                } => {
                    if buf.is_empty() {
                        self.state = State::Array { expected, args };
                        return ParseResult::Incomplete;
                    }
                    match buf[0] {
                        b'$' => {
                            match consume_line(buf) {
                                Some(line) => {
                                    let len = match parse_int(&line[1..]) {
                                        Some(n) if n == -1 => {
                                            args.push(RespExpr::Nil);
                                            if args.len() == expected {
                                                return ParseResult::Complete(args);
                                            }
                                            self.state = State::Array { expected, args };
                                            continue;
                                        }
                                        Some(n)
                                            if n >= 0 && n <= self.max_bulk_len as i64 =>
                                        {
                                            n as usize
                                        }
                                        _ => {
                                            return ParseResult::Error(ParseError::BadBulkLen)
                                        }
                                    };
                                    self.state = State::BulkString {
                                        expected: len,
                                        args,
                                        array_expected: expected,
                                    };
                                }
                                None => {
                                    self.state = State::Array { expected, args };
                                    return ParseResult::Incomplete;
                                }
                            }
                        }
                        b'+' => {
                            match consume_line(buf) {
                                Some(line) => {
                                    args.push(RespExpr::String(Bytes::copy_from_slice(&line[1..])));
                                    if args.len() == expected {
                                        return ParseResult::Complete(args);
                                    }
                                    self.state = State::Array { expected, args };
                                }
                                None => {
                                    self.state = State::Array { expected, args };
                                    return ParseResult::Incomplete;
                                }
                            }
                        }
                        b':' => {
                            match consume_line(buf) {
                                Some(line) => match parse_int(&line[1..]) {
                                    Some(n) => {
                                        args.push(RespExpr::Int(n));
                                        if args.len() == expected {
                                            return ParseResult::Complete(args);
                                        }
                                        self.state = State::Array { expected, args };
                                    }
                                    None => {
                                        return ParseResult::Error(ParseError::BadInteger);
                                    }
                                },
                                None => {
                                    self.state = State::Array { expected, args };
                                    return ParseResult::Incomplete;
                                }
                            }
                        }
                        b'-' => {
                            match consume_line(buf) {
                                Some(line) => {
                                    args.push(RespExpr::Error(Bytes::copy_from_slice(&line[1..])));
                                    if args.len() == expected {
                                        return ParseResult::Complete(args);
                                    }
                                    self.state = State::Array { expected, args };
                                }
                                None => {
                                    self.state = State::Array { expected, args };
                                    return ParseResult::Incomplete;
                                }
                            }
                        }
                        _ => return ParseResult::Error(ParseError::BadProtocol),
                    }
                }
                State::BulkString {
                    expected,
                    mut args,
                    array_expected,
                } => {
                    let needed = expected + 2;
                    if buf.len() < needed {
                        self.state = State::BulkString {
                            expected,
                            args,
                            array_expected,
                        };
                        return ParseResult::Incomplete;
                    }
                    // Zero-copy: freeze the split portion into a Bytes handle
                    let data = buf.split_to(expected).freeze();
                    let _ = buf.split_to(2); // \r\n
                    args.push(RespExpr::String(data));
                    if args.len() == array_expected {
                        return ParseResult::Complete(args);
                    }
                    self.state = State::Array {
                        expected: array_expected,
                        args,
                    };
                }
            }
        }
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse an integer from a byte slice.
fn parse_int(s: &[u8]) -> Option<i64> {
    std::str::from_utf8(s).ok()?.trim().parse().ok()
}

/// Try to consume a \r\n-terminated line. Returns line content as raw Vec<u8>
/// (used only for protocol framing, not for bulk string data).
fn consume_line(buf: &mut BytesMut) -> Option<Vec<u8>> {
    let data = &buf[..];
    for i in 0..data.len().saturating_sub(1) {
        if data[i] == b'\r' && data[i + 1] == b'\n' {
            let line = buf.split_to(i).to_vec();
            let _ = buf.split_to(2);
            return Some(line);
        }
    }
    for i in 0..data.len() {
        if data[i] == b'\n' {
            let end = if i > 0 && data[i - 1] == b'\r' {
                i - 1
            } else {
                i
            };
            let line = buf.split_to(end).to_vec();
            let remaining = i - end + 1;
            let _ = buf.split_to(remaining);
            return Some(line);
        }
    }
    None
}

/// Parse inline command arguments.
fn parse_inline_args(line: &[u8]) -> RespArgs {
    let s = match std::str::from_utf8(line) {
        Ok(s) => s.trim(),
        Err(_) => return SmallVec::new(),
    };
    if s.is_empty() {
        return SmallVec::new();
    }
    s.split_whitespace()
        .map(|arg| RespExpr::String(Bytes::copy_from_slice(arg.as_bytes())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_all(input: &[u8]) -> ParseResult {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from(input);
        parser.parse(&mut buf)
    }

    #[test]
    fn test_inline_ping() {
        match parse_all(b"PING\r\n") {
            ParseResult::Complete(args) => {
                assert_eq!(args.len(), 1);
                assert_eq!(args[0].as_bytes().unwrap(), b"PING");
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_inline_echo() {
        match parse_all(b"ECHO hello\r\n") {
            ParseResult::Complete(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].as_bytes().unwrap(), b"ECHO");
                assert_eq!(args[1].as_bytes().unwrap(), b"hello");
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_resp_ping() {
        match parse_all(b"*1\r\n$4\r\nPING\r\n") {
            ParseResult::Complete(args) => {
                assert_eq!(args.len(), 1);
                assert_eq!(args[0].as_bytes().unwrap(), b"PING");
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_resp_set() {
        match parse_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") {
            ParseResult::Complete(args) => {
                assert_eq!(args.len(), 3);
                assert_eq!(args[0].as_bytes().unwrap(), b"SET");
                assert_eq!(args[1].as_bytes().unwrap(), b"foo");
                assert_eq!(args[2].as_bytes().unwrap(), b"bar");
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_incomplete() {
        match parse_all(b"*3\r\n$3\r\nSET\r\n") {
            ParseResult::Incomplete => {}
            other => panic!("expected Incomplete, got {:?}", other),
        }
    }

    #[test]
    fn test_incremental_parse() {
        let mut parser = RespParser::new();
        let mut buf = BytesMut::from(&b"*2\r\n$4\r\nECHO"[..]);
        match parser.parse(&mut buf) {
            ParseResult::Incomplete => {}
            other => panic!("expected Incomplete, got {:?}", other),
        }
        buf.extend_from_slice(b"\r\n$5\r\nhello\r\n");
        match parser.parse(&mut buf) {
            ParseResult::Complete(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].as_bytes().unwrap(), b"ECHO");
                assert_eq!(args[1].as_bytes().unwrap(), b"hello");
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_pipelining() {
        let mut parser = RespParser::new();
        let mut buf =
            BytesMut::from(&b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"[..]);

        match parser.parse(&mut buf) {
            ParseResult::Complete(args) => {
                assert_eq!(args[0].as_bytes().unwrap(), b"PING");
            }
            other => panic!("expected Complete for PING, got {:?}", other),
        }
        match parser.parse(&mut buf) {
            ParseResult::Complete(args) => {
                assert_eq!(args[0].as_bytes().unwrap(), b"ECHO");
                assert_eq!(args[1].as_bytes().unwrap(), b"hi");
            }
            other => panic!("expected Complete for ECHO, got {:?}", other),
        }
    }

    #[test]
    fn test_null_bulk_string() {
        match parse_all(b"*2\r\n$4\r\nPING\r\n$-1\r\n") {
            ParseResult::Complete(args) => {
                assert_eq!(args.len(), 2);
                assert_eq!(args[0].as_bytes().unwrap(), b"PING");
                assert_eq!(args[1], RespExpr::Nil);
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn test_empty_string() {
        match parse_all(b"*1\r\n$0\r\n\r\n") {
            ParseResult::Complete(args) => {
                assert_eq!(args.len(), 1);
                assert_eq!(args[0].as_bytes().unwrap(), b"");
            }
            other => panic!("expected Complete, got {:?}", other),
        }
    }
}
