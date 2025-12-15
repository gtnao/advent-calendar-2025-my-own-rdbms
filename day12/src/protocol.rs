use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::{bail, Result};

// PostgreSQL Wire Protocol implementation

const PROTOCOL_VERSION_3: i32 = 196608; // 3.0

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection { stream }
    }

    // Read startup message from client
    pub fn read_startup(&mut self) -> Result<StartupMessage> {
        let len = self.read_i32()? as usize;
        let protocol_version = self.read_i32()?;

        if protocol_version == 80877103 {
            // SSLRequest - we don't support SSL, send 'N'
            self.stream.write_all(b"N")?;
            return self.read_startup(); // Read actual startup
        }

        if protocol_version != PROTOCOL_VERSION_3 {
            bail!("unsupported protocol version: {protocol_version}");
        }

        // Read parameters (null-terminated strings)
        let mut params = Vec::new();
        let remaining = len - 8; // subtract length and version
        let mut buf = vec![0u8; remaining];
        self.stream.read_exact(&mut buf)?;

        let mut i = 0;
        while i < buf.len() && buf[i] != 0 {
            let key_end = buf[i..].iter().position(|&b| b == 0).unwrap_or(buf.len() - i);
            let key = String::from_utf8_lossy(&buf[i..i + key_end]).to_string();
            i += key_end + 1;

            if i >= buf.len() {
                break;
            }

            let val_end = buf[i..].iter().position(|&b| b == 0).unwrap_or(buf.len() - i);
            let val = String::from_utf8_lossy(&buf[i..i + val_end]).to_string();
            i += val_end + 1;

            params.push((key, val));
        }

        Ok(StartupMessage { params })
    }

    // Read a query message
    pub fn read_message(&mut self) -> Result<Option<FrontendMessage>> {
        let msg_type = match self.read_u8() {
            Ok(b) => b,
            Err(_) => return Ok(None), // Connection closed
        };

        let len = self.read_i32()? as usize - 4; // subtract length field itself
        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf)?;

        match msg_type {
            b'Q' => {
                // Query - null terminated string
                let query = String::from_utf8_lossy(&buf[..buf.len() - 1]).to_string();
                Ok(Some(FrontendMessage::Query(query)))
            }
            b'X' => Ok(Some(FrontendMessage::Terminate)),
            _ => {
                println!("[Protocol] Unknown message type: {}", msg_type as char);
                Ok(Some(FrontendMessage::Unknown(msg_type)))
            }
        }
    }

    // Send AuthenticationOk
    pub fn send_auth_ok(&mut self) -> Result<()> {
        self.write_message(b'R', &0i32.to_be_bytes())
    }

    // Send ParameterStatus
    pub fn send_parameter_status(&mut self, name: &str, value: &str) -> Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(value.as_bytes());
        buf.push(0);
        self.write_message(b'S', &buf)
    }

    // Send BackendKeyData
    pub fn send_backend_key_data(&mut self, pid: i32, secret: i32) -> Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&pid.to_be_bytes());
        buf.extend_from_slice(&secret.to_be_bytes());
        self.write_message(b'K', &buf)
    }

    // Send ReadyForQuery
    pub fn send_ready_for_query(&mut self) -> Result<()> {
        self.write_message(b'Z', b"I") // 'I' = idle
    }

    // Send RowDescription
    pub fn send_row_description(&mut self, columns: &[ColumnDesc]) -> Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(columns.len() as i16).to_be_bytes());

        for col in columns {
            buf.extend_from_slice(col.name.as_bytes());
            buf.push(0); // null terminator
            buf.extend_from_slice(&0i32.to_be_bytes()); // table OID
            buf.extend_from_slice(&0i16.to_be_bytes()); // column attr number
            buf.extend_from_slice(&col.type_oid.to_be_bytes()); // type OID
            buf.extend_from_slice(&col.type_size.to_be_bytes()); // type size
            buf.extend_from_slice(&(-1i32).to_be_bytes()); // type modifier
            buf.extend_from_slice(&0i16.to_be_bytes()); // format code (0 = text)
        }

        self.write_message(b'T', &buf)
    }

    // Send DataRow
    pub fn send_data_row(&mut self, values: &[Option<String>]) -> Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as i16).to_be_bytes());

        for val in values {
            match val {
                Some(s) => {
                    buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
                None => {
                    buf.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
                }
            }
        }

        self.write_message(b'D', &buf)
    }

    // Send CommandComplete
    pub fn send_command_complete(&mut self, tag: &str) -> Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(tag.as_bytes());
        buf.push(0);
        self.write_message(b'C', &buf)
    }

    // Send ErrorResponse
    pub fn send_error(&mut self, message: &str) -> Result<()> {
        let mut buf = Vec::new();
        buf.push(b'S'); // Severity
        buf.extend_from_slice(b"ERROR");
        buf.push(0);
        buf.push(b'M'); // Message
        buf.extend_from_slice(message.as_bytes());
        buf.push(0);
        buf.push(0); // terminator
        self.write_message(b'E', &buf)
    }

    // Send EmptyQueryResponse
    pub fn send_empty_query(&mut self) -> Result<()> {
        self.write_message(b'I', &[])
    }

    fn write_message(&mut self, msg_type: u8, data: &[u8]) -> Result<()> {
        let len = (data.len() + 4) as i32;
        self.stream.write_all(&[msg_type])?;
        self.stream.write_all(&len.to_be_bytes())?;
        self.stream.write_all(data)?;
        self.stream.flush()?;
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.stream.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_i32(&mut self) -> Result<i32> {
        let mut buf = [0u8; 4];
        self.stream.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }
}

#[derive(Debug)]
pub struct StartupMessage {
    pub params: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum FrontendMessage {
    Query(String),
    Terminate,
    Unknown(u8),
}

#[derive(Debug)]
pub struct ColumnDesc {
    pub name: String,
    pub type_oid: i32,
    pub type_size: i16,
}

impl ColumnDesc {
    pub fn new_int(name: &str) -> Self {
        ColumnDesc {
            name: name.to_string(),
            type_oid: 23,  // INT4
            type_size: 4,
        }
    }

    pub fn new_varchar(name: &str) -> Self {
        ColumnDesc {
            name: name.to_string(),
            type_oid: 25,  // TEXT
            type_size: -1, // variable
        }
    }

    pub fn new_bool(name: &str) -> Self {
        ColumnDesc {
            name: name.to_string(),
            type_oid: 16,  // BOOL
            type_size: 1,
        }
    }
}
