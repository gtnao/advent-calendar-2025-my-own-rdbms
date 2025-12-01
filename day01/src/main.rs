use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

use anyhow::Result;

const DATA_FILE: &str = "table.db";

#[derive(Debug, Clone)]
enum DataType {
    Int,
    Varchar,
}

#[derive(Debug, Clone)]
struct Column {
    #[allow(dead_code)] // used in later days
    name: String,
    data_type: DataType,
}

#[derive(Debug, Clone)]
struct Schema {
    columns: Vec<Column>,
}

#[derive(Debug, Clone)]
enum Value {
    Null,
    Int(i32),
    Varchar(String),
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
    }
}

// Null bitmap size: ceil(num_columns / 8)
fn null_bitmap_size(num_columns: usize) -> usize {
    num_columns.div_ceil(8)
}

fn serialize_tuple(values: &[Value], schema: &Schema) -> Vec<u8> {
    let mut buf = Vec::new();

    // null bitmap (bit is set when value is NOT null)
    let bitmap_size = null_bitmap_size(schema.columns.len());
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

fn deserialize_tuple(data: &[u8], schema: &Schema) -> Result<(Vec<Value>, usize)> {
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
    Ok((values, offset))
}

fn insert(values: &[Value], schema: &Schema) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(DATA_FILE)?;
    let data = serialize_tuple(values, schema);
    file.write_all(&data)?;
    file.sync_all()?;
    Ok(())
}

fn scan(schema: &Schema) -> Result<Vec<Vec<Value>>> {
    let mut file = match File::open(DATA_FILE) {
        Ok(f) => f,
        Err(_) => return Ok(Vec::new()),
    };
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    let mut results = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let (values, len) = deserialize_tuple(&data[offset..], schema)?;
        results.push(values);
        offset += len;
    }
    Ok(results)
}

fn main() -> Result<()> {
    let _ = std::fs::remove_file(DATA_FILE);

    let schema = Schema {
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::Varchar,
            },
        ],
    };

    insert(&[Value::Int(1), Value::Varchar("Alice".to_string())], &schema)?;
    insert(&[Value::Int(2), Value::Null], &schema)?;
    insert(&[Value::Null, Value::Varchar("Charlie".to_string())], &schema)?;

    let tuples = scan(&schema)?;
    println!("Scanned {} tuples:", tuples.len());
    for values in tuples {
        println!("  {values:?}");
    }
    Ok(())
}
