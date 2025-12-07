#![allow(dead_code)]

use crate::tuple::{Column, DataType};

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

pub struct Catalog {
    tables: Vec<TableDef>,
}

impl Catalog {
    pub fn new() -> Self {
        // Fixed schema: users(id INT, name VARCHAR)
        let users_table = TableDef {
            name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar,
                    nullable: true,
                },
            ],
        };
        Catalog {
            tables: vec![users_table],
        }
    }

    pub fn get_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|t| t.name == name)
    }

    pub fn get_table_id(&self, name: &str) -> Option<usize> {
        self.tables.iter().position(|t| t.name == name)
    }

    pub fn get_table_by_id(&self, id: usize) -> Option<&TableDef> {
        self.tables.get(id)
    }
}

impl TableDef {
    pub fn get_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn get_column_id(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn to_schema(&self) -> crate::tuple::Schema {
        crate::tuple::Schema {
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
