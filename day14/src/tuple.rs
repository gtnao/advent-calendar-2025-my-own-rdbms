use anyhow::Result;

// Transaction ID type for MVCC
pub type TxnId = u64;

// Invalid transaction ID (used for xmax when tuple is not deleted)
pub const INVALID_TXN_ID: TxnId = 0;

// MVCC tuple header size: xmin (8 bytes) + xmax (8 bytes)
pub const TUPLE_HEADER_SIZE: usize = 16;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Varchar,
    Bool,
}

#[derive(Debug, Clone)]
pub struct Column {
    #[allow(dead_code)]
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int(i32),
    Varchar(String),
    Bool(bool),
}

fn serialize_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => Vec::new(),
        Value::Int(v) => v.to_ne_bytes().to_vec(),
        Value::Varchar(v) => {
            let bytes = v.as_bytes();
            let mut buf = (bytes.len() as u32).to_ne_bytes().to_vec();
            buf.extend_from_slice(bytes);
            buf
        }
        Value::Bool(v) => vec![if *v { 1 } else { 0 }],
    }
}

fn deserialize_value(data: &[u8], data_type: &DataType, is_null: bool) -> Result<(Value, usize)> {
    if is_null {
        return Ok((Value::Null, 0));
    }
    match data_type {
        DataType::Int => {
            let v = i32::from_ne_bytes(data[0..4].try_into()?);
            Ok((Value::Int(v), 4))
        }
        DataType::Varchar => {
            let len = u32::from_ne_bytes(data[0..4].try_into()?) as usize;
            let v = String::from_utf8(data[4..4 + len].to_vec())?;
            Ok((Value::Varchar(v), 4 + len))
        }
        DataType::Bool => {
            let v = data[0] != 0;
            Ok((Value::Bool(v), 1))
        }
    }
}

// Null bitmap size: ceil(num_columns / 8)
fn null_bitmap_size(num_columns: usize) -> usize {
    num_columns.div_ceil(8)
}

pub fn serialize_tuple(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();

    // null bitmap (bit is set when value is NOT null)
    let bitmap_size = null_bitmap_size(values.len());
    let mut bitmap = vec![0u8; bitmap_size];
    for (i, value) in values.iter().enumerate() {
        if !matches!(value, Value::Null) {
            bitmap[i / 8] |= 1 << (i % 8);
        }
    }
    buf.extend(&bitmap);

    for value in values {
        buf.extend(serialize_value(value));
    }
    buf
}

pub fn deserialize_tuple(data: &[u8], schema: &Schema) -> Result<Vec<Value>> {
    let bitmap_size = null_bitmap_size(schema.columns.len());
    let bitmap = &data[0..bitmap_size];
    let mut offset = bitmap_size;

    let mut values = Vec::new();
    for (i, column) in schema.columns.iter().enumerate() {
        // bit is set when value is NOT null
        let is_null = bitmap[i / 8] & (1 << (i % 8)) == 0;
        let (value, len) = deserialize_value(&data[offset..], &column.data_type, is_null)?;
        values.push(value);
        offset += len;
    }
    Ok(values)
}

// ============================================================================
// MVCC Tuple Serialization
// ============================================================================
//
// MVCC tuple format:
//   [xmin: 8 bytes][xmax: 8 bytes][null_bitmap][value1][value2]...
//
// - xmin: Transaction ID that created this tuple
// - xmax: Transaction ID that deleted this tuple (0 if not deleted)

/// Serialize a tuple with MVCC header (xmin/xmax)
pub fn serialize_tuple_mvcc(xmin: TxnId, xmax: TxnId, values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();

    // MVCC header
    buf.extend_from_slice(&xmin.to_le_bytes());
    buf.extend_from_slice(&xmax.to_le_bytes());

    // Tuple data (null bitmap + values)
    let tuple_data = serialize_tuple(values);
    buf.extend(tuple_data);

    buf
}

/// Deserialize a tuple with MVCC header
/// Returns (xmin, xmax, values)
pub fn deserialize_tuple_mvcc(data: &[u8], schema: &Schema) -> Result<(TxnId, TxnId, Vec<Value>)> {
    if data.len() < TUPLE_HEADER_SIZE {
        anyhow::bail!("tuple data too short for MVCC header");
    }

    // Read MVCC header
    let xmin = u64::from_le_bytes(data[0..8].try_into()?);
    let xmax = u64::from_le_bytes(data[8..16].try_into()?);

    // Read tuple data
    let values = deserialize_tuple(&data[TUPLE_HEADER_SIZE..], schema)?;

    Ok((xmin, xmax, values))
}

/// Update xmax of an existing MVCC tuple (for DELETE/UPDATE operations)
#[allow(dead_code)]
pub fn set_tuple_xmax(data: &mut [u8], xmax: TxnId) {
    data[8..16].copy_from_slice(&xmax.to_le_bytes());
}

/// Get xmin from MVCC tuple data
#[allow(dead_code)]
pub fn get_tuple_xmin(data: &[u8]) -> TxnId {
    u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]))
}

/// Get xmax from MVCC tuple data
#[allow(dead_code)]
pub fn get_tuple_xmax(data: &[u8]) -> TxnId {
    u64::from_le_bytes(data[8..16].try_into().unwrap_or([0; 8]))
}
