use anyhow::Result;

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
