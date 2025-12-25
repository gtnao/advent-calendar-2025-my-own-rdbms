use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::bootstrap::{
    DATA_TYPE_BOOL, DATA_TYPE_INT, DATA_TYPE_VARCHAR, PG_ATTRIBUTE_PAGE_ID, PG_ATTRIBUTE_TABLE_ID,
    PG_CLASS_PAGE_ID, PG_CLASS_TABLE_ID, PG_INDEX_PAGE_ID, PG_INDEX_TABLE_ID,
};
use crate::buffer_pool::BufferPoolManager;
use crate::page::NO_NEXT_PAGE;
use crate::tuple::{deserialize_tuple_mvcc, Column, DataType, Schema, Value};

#[derive(Debug, Clone)]
pub struct TableDef {
    pub table_id: u32,
    #[allow(dead_code)]
    pub name: String,
    pub first_page_id: u32,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    #[allow(dead_code)]
    pub index_id: u32,
    pub index_name: String,
    #[allow(dead_code)]
    pub table_id: u32,
    pub column_ids: Vec<usize>, // column indices in the table
    pub meta_page_id: u32,      // B-Tree meta page ID (stores root pointer)
}

pub struct Catalog {
    bpm: Arc<Mutex<BufferPoolManager>>,
}

impl Catalog {
    pub fn new(bpm: Arc<Mutex<BufferPoolManager>>) -> Self {
        Catalog { bpm }
    }

    /// Get pg_class schema (hardcoded to avoid circular dependency)
    fn pg_class_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "table_id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "first_page_id".to_string(),
                    data_type: DataType::Int,
                },
            ],
        }
    }

    /// Get pg_attribute schema (hardcoded to avoid circular dependency)
    fn pg_attribute_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "table_id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "column_name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "data_type".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "nullable".to_string(),
                    data_type: DataType::Bool,
                },
                Column {
                    name: "ordinal_position".to_string(),
                    data_type: DataType::Int,
                },
            ],
        }
    }

    /// Get pg_index schema (hardcoded to avoid circular dependency)
    fn pg_index_schema() -> Schema {
        Schema {
            columns: vec![
                Column {
                    name: "index_id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "index_name".to_string(),
                    data_type: DataType::Varchar,
                },
                Column {
                    name: "table_id".to_string(),
                    data_type: DataType::Int,
                },
                Column {
                    name: "column_ids".to_string(),
                    data_type: DataType::Varchar, // comma-separated
                },
                Column {
                    name: "root_page_id".to_string(),
                    data_type: DataType::Int,
                },
            ],
        }
    }

    /// Read all tuples from a table's pages
    fn read_table_tuples(&self, first_page_id: u32, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        let mut tuples = Vec::new();
        let mut current_page_id = first_page_id;

        while current_page_id != NO_NEXT_PAGE {
            let mut bpm = self.bpm.lock().unwrap();
            let page_arc = bpm.fetch_page(current_page_id)?;
            let page = page_arc.read().unwrap();

            let tuple_count = page.tuple_count();
            for slot_id in 0..tuple_count {
                if let Some(tuple_data) = page.get_tuple(slot_id) {
                    // Deserialize MVCC tuple and check visibility (xmax=0 means not deleted)
                    let (_xmin, xmax, values) = deserialize_tuple_mvcc(tuple_data, schema)?;
                    if xmax == 0 {
                        tuples.push(values);
                    }
                }
            }

            let next_page_id = page.next_page_id();
            drop(page);
            bpm.unpin_page(current_page_id, false)?;
            current_page_id = next_page_id;
        }

        Ok(tuples)
    }

    /// Get table by name
    pub fn get_table(&self, name: &str) -> Option<TableDef> {
        // Special case for system tables
        if name == "pg_class" {
            return Some(self.get_pg_class_def());
        }
        if name == "pg_attribute" {
            return Some(self.get_pg_attribute_def());
        }
        if name == "pg_index" {
            return Some(self.get_pg_index_def());
        }

        // Read from pg_class to find the table
        let pg_class_schema = Self::pg_class_schema();
        let tuples = self
            .read_table_tuples(PG_CLASS_PAGE_ID, &pg_class_schema)
            .ok()?;

        for values in tuples {
            if let (Value::Int(table_id), Value::Varchar(table_name), Value::Int(first_page_id)) =
                (&values[0], &values[1], &values[2])
            {
                if table_name == name {
                    // Found the table, now get its columns
                    let columns = self.get_columns_for_table(*table_id as u32);
                    return Some(TableDef {
                        table_id: *table_id as u32,
                        name: table_name.clone(),
                        first_page_id: *first_page_id as u32,
                        columns,
                    });
                }
            }
        }

        None
    }

    /// Get table by ID
    pub fn get_table_by_id(&self, id: u32) -> Option<TableDef> {
        // Special case for system tables
        if id == PG_CLASS_TABLE_ID {
            return Some(self.get_pg_class_def());
        }
        if id == PG_ATTRIBUTE_TABLE_ID {
            return Some(self.get_pg_attribute_def());
        }
        if id == PG_INDEX_TABLE_ID {
            return Some(self.get_pg_index_def());
        }

        // Read from pg_class to find the table
        let pg_class_schema = Self::pg_class_schema();
        let tuples = self
            .read_table_tuples(PG_CLASS_PAGE_ID, &pg_class_schema)
            .ok()?;

        for values in tuples {
            if let (Value::Int(table_id), Value::Varchar(table_name), Value::Int(first_page_id)) =
                (&values[0], &values[1], &values[2])
            {
                if *table_id as u32 == id {
                    let columns = self.get_columns_for_table(*table_id as u32);
                    return Some(TableDef {
                        table_id: *table_id as u32,
                        name: table_name.clone(),
                        first_page_id: *first_page_id as u32,
                        columns,
                    });
                }
            }
        }

        None
    }

    /// Get table ID by name
    pub fn get_table_id(&self, name: &str) -> Option<u32> {
        self.get_table(name).map(|t| t.table_id)
    }

    /// Get columns for a table from pg_attribute
    fn get_columns_for_table(&self, table_id: u32) -> Vec<ColumnDef> {
        let pg_attribute_schema = Self::pg_attribute_schema();
        let tuples = match self.read_table_tuples(PG_ATTRIBUTE_PAGE_ID, &pg_attribute_schema) {
            Ok(t) => t,
            Err(_) => return vec![],
        };

        let mut columns: Vec<(i32, ColumnDef)> = Vec::new();

        for values in tuples {
            if let (
                Value::Int(tid),
                Value::Varchar(col_name),
                Value::Int(data_type),
                Value::Bool(nullable),
                Value::Int(ordinal),
            ) = (&values[0], &values[1], &values[2], &values[3], &values[4])
            {
                if *tid as u32 == table_id {
                    let dt = match *data_type {
                        DATA_TYPE_INT => DataType::Int,
                        DATA_TYPE_VARCHAR => DataType::Varchar,
                        DATA_TYPE_BOOL => DataType::Bool,
                        _ => DataType::Int, // fallback
                    };
                    columns.push((
                        *ordinal,
                        ColumnDef {
                            name: col_name.clone(),
                            data_type: dt,
                            nullable: *nullable,
                        },
                    ));
                }
            }
        }

        // Sort by ordinal position
        columns.sort_by_key(|(ord, _)| *ord);
        columns.into_iter().map(|(_, col)| col).collect()
    }

    /// Get hardcoded pg_class definition
    fn get_pg_class_def(&self) -> TableDef {
        TableDef {
            table_id: PG_CLASS_TABLE_ID,
            name: "pg_class".to_string(),
            first_page_id: PG_CLASS_PAGE_ID,
            columns: vec![
                ColumnDef {
                    name: "table_id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                    nullable: false,
                },
                ColumnDef {
                    name: "first_page_id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
            ],
        }
    }

    /// Get hardcoded pg_attribute definition
    fn get_pg_attribute_def(&self) -> TableDef {
        TableDef {
            table_id: PG_ATTRIBUTE_TABLE_ID,
            name: "pg_attribute".to_string(),
            first_page_id: PG_ATTRIBUTE_PAGE_ID,
            columns: vec![
                ColumnDef {
                    name: "table_id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "column_name".to_string(),
                    data_type: DataType::Varchar,
                    nullable: false,
                },
                ColumnDef {
                    name: "data_type".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "nullable".to_string(),
                    data_type: DataType::Bool,
                    nullable: false,
                },
                ColumnDef {
                    name: "ordinal_position".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
            ],
        }
    }

    /// Get next available table ID
    pub fn next_table_id(&self) -> Result<u32> {
        let pg_class_schema = Self::pg_class_schema();
        let tuples = self.read_table_tuples(PG_CLASS_PAGE_ID, &pg_class_schema)?;

        let mut max_id: u32 = 2; // pg_class(0), pg_attribute(1), pg_index(2) exist
        for values in tuples {
            if let Value::Int(table_id) = &values[0] {
                max_id = max_id.max(*table_id as u32);
            }
        }
        Ok(max_id + 1)
    }

    /// Get hardcoded pg_index definition
    fn get_pg_index_def(&self) -> TableDef {
        TableDef {
            table_id: PG_INDEX_TABLE_ID,
            name: "pg_index".to_string(),
            first_page_id: PG_INDEX_PAGE_ID,
            columns: vec![
                ColumnDef {
                    name: "index_id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "index_name".to_string(),
                    data_type: DataType::Varchar,
                    nullable: false,
                },
                ColumnDef {
                    name: "table_id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "column_ids".to_string(),
                    data_type: DataType::Varchar,
                    nullable: false,
                },
                ColumnDef {
                    name: "root_page_id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
            ],
        }
    }

    /// Get all indexes for a table
    pub fn get_indexes_for_table(&self, table_id: u32) -> Vec<IndexDef> {
        let pg_index_schema = Self::pg_index_schema();
        let tuples = match self.read_table_tuples(PG_INDEX_PAGE_ID, &pg_index_schema) {
            Ok(t) => t,
            Err(_) => return vec![],
        };

        let mut indexes = Vec::new();
        for values in tuples {
            if let (
                Value::Int(idx_id),
                Value::Varchar(idx_name),
                Value::Int(tid),
                Value::Varchar(col_ids_str),
                Value::Int(meta_page_id),
            ) = (&values[0], &values[1], &values[2], &values[3], &values[4])
            {
                if *tid as u32 == table_id {
                    let column_ids: Vec<usize> = col_ids_str
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    indexes.push(IndexDef {
                        index_id: *idx_id as u32,
                        index_name: idx_name.clone(),
                        table_id: *tid as u32,
                        column_ids,
                        meta_page_id: *meta_page_id as u32,
                    });
                }
            }
        }
        indexes
    }

    /// Get next available index ID
    pub fn next_index_id(&self) -> Result<u32> {
        let pg_index_schema = Self::pg_index_schema();
        let tuples = self.read_table_tuples(PG_INDEX_PAGE_ID, &pg_index_schema)?;

        let mut max_id: u32 = 0;
        for values in tuples {
            if let Value::Int(index_id) = &values[0] {
                max_id = max_id.max(*index_id as u32);
            }
        }
        Ok(max_id + 1)
    }

    /// Get index by name
    pub fn get_index(&self, name: &str) -> Option<IndexDef> {
        let pg_index_schema = Self::pg_index_schema();
        let tuples = self
            .read_table_tuples(PG_INDEX_PAGE_ID, &pg_index_schema)
            .ok()?;

        for values in tuples {
            if let (
                Value::Int(idx_id),
                Value::Varchar(idx_name),
                Value::Int(tid),
                Value::Varchar(col_ids_str),
                Value::Int(meta_page_id),
            ) = (&values[0], &values[1], &values[2], &values[3], &values[4])
            {
                if idx_name == name {
                    let column_ids: Vec<usize> = col_ids_str
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    return Some(IndexDef {
                        index_id: *idx_id as u32,
                        index_name: idx_name.clone(),
                        table_id: *tid as u32,
                        column_ids,
                        meta_page_id: *meta_page_id as u32,
                    });
                }
            }
        }
        None
    }
}

impl TableDef {
    #[allow(dead_code)]
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    #[allow(dead_code)]
    pub fn get_column_id(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn to_schema(&self) -> Schema {
        Schema {
            columns: self
                .columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                })
                .collect(),
        }
    }
}
