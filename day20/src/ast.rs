// SQL Abstract Syntax Tree

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
    Begin,
    Commit,
    Rollback,
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub column_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub columns: Vec<SelectColumn>,
    pub from: FromClause,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    Table(TableRef),
    Join {
        left: Box<FromClause>,
        right: TableRef,
        join_type: JoinType,
        condition: Expr,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    Asterisk,
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub values: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub table: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Varchar,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Literal),
    Column {
        table: Option<String>,
        name: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Aggregate {
        func: AggregateFunction,
        arg: Box<AggregateArg>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateArg {
    Asterisk,
    Expr(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Integer(i64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Neg,
}
