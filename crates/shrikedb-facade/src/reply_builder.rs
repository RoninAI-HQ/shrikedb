use bytes::BytesMut;

/// Builds RESP2 protocol responses into a byte buffer.
pub struct ReplyBuilder<'a> {
    buf: &'a mut BytesMut,
}

impl<'a> ReplyBuilder<'a> {
    pub fn new(buf: &'a mut BytesMut) -> Self {
        Self { buf }
    }

    /// +OK\r\n
    pub fn send_ok(&mut self) {
        self.buf.extend_from_slice(b"+OK\r\n");
    }

    /// +PONG\r\n
    pub fn send_pong(&mut self) {
        self.buf.extend_from_slice(b"+PONG\r\n");
    }

    /// +<msg>\r\n
    pub fn send_simple_string(&mut self, msg: &str) {
        self.buf.extend_from_slice(b"+");
        self.buf.extend_from_slice(msg.as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    /// -<msg>\r\n
    pub fn send_error(&mut self, msg: &str) {
        self.buf.extend_from_slice(b"-");
        self.buf.extend_from_slice(msg.as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    /// -ERR <msg>\r\n
    pub fn send_err(&mut self, msg: &str) {
        self.buf.extend_from_slice(b"-ERR ");
        self.buf.extend_from_slice(msg.as_bytes());
        self.buf.extend_from_slice(b"\r\n");
    }

    /// -WRONGTYPE <msg>\r\n
    pub fn send_wrong_type(&mut self, msg: &str) {
        self.buf
            .extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value");
        if !msg.is_empty() {
            self.buf.extend_from_slice(b": ");
            self.buf.extend_from_slice(msg.as_bytes());
        }
        self.buf.extend_from_slice(b"\r\n");
    }

    /// $<len>\r\n<data>\r\n  or  $-1\r\n for None
    pub fn send_bulk_string(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(b"$");
        self.write_int(data.len() as i64);
        self.buf.extend_from_slice(b"\r\n");
        self.buf.extend_from_slice(data);
        self.buf.extend_from_slice(b"\r\n");
    }

    /// $-1\r\n
    pub fn send_null(&mut self) {
        self.buf.extend_from_slice(b"$-1\r\n");
    }

    /// *-1\r\n
    pub fn send_null_array(&mut self) {
        self.buf.extend_from_slice(b"*-1\r\n");
    }

    /// :<n>\r\n
    pub fn send_integer(&mut self, n: i64) {
        self.buf.extend_from_slice(b":");
        self.write_int(n);
        self.buf.extend_from_slice(b"\r\n");
    }

    /// *<len>\r\n  -- caller must then send `len` elements.
    pub fn send_array_len(&mut self, len: usize) {
        self.buf.extend_from_slice(b"*");
        self.write_int(len as i64);
        self.buf.extend_from_slice(b"\r\n");
    }

    /// Convenience: send an array of bulk strings.
    pub fn send_string_array(&mut self, items: &[&[u8]]) {
        self.send_array_len(items.len());
        for item in items {
            self.send_bulk_string(item);
        }
    }

    fn write_int(&mut self, n: i64) {
        let mut itoa_buf = itoa::Buffer::new();
        self.buf
            .extend_from_slice(itoa_buf.format(n).as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ok() {
        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_ok();
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_pong() {
        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_pong();
        assert_eq!(&buf[..], b"+PONG\r\n");
    }

    #[test]
    fn test_error() {
        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_err("unknown command");
        assert_eq!(&buf[..], b"-ERR unknown command\r\n");
    }

    #[test]
    fn test_bulk_string() {
        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_bulk_string(b"hello");
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_null() {
        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_null();
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_integer() {
        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_integer(42);
        assert_eq!(&buf[..], b":42\r\n");
    }

    #[test]
    fn test_array() {
        let mut buf = BytesMut::new();
        let mut rb = ReplyBuilder::new(&mut buf);
        rb.send_array_len(2);
        rb.send_bulk_string(b"foo");
        rb.send_bulk_string(b"bar");
        assert_eq!(&buf[..], b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }
}
