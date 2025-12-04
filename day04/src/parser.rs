use anyhow::{bail, Result};

use crate::ast::*;
use crate::lexer::Token;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        self.pos += 1;
        self.tokens.get(self.pos - 1).unwrap_or(&Token::Eof)
    }

    fn expect(&mut self, expected: Token) -> Result<()> {
        let token = self.peek().clone();
        if token == expected {
            self.advance();
            Ok(())
        } else {
            bail!("expected {expected:?}, got {token:?}");
        }
    }

    pub fn parse(&mut self) -> Result<Statement> {
        let stmt = match self.peek() {
            Token::Select => self.parse_select()?,
            Token::Insert => self.parse_insert()?,
            Token::Create => self.parse_create()?,
            _ => bail!("unexpected token: {:?}", self.peek()),
        };

        // Optional semicolon
        if self.peek() == &Token::Semicolon {
            self.advance();
        }

        Ok(stmt)
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(Token::Select)?;

        // Parse column list
        let mut columns = Vec::new();
        loop {
            if self.peek() == &Token::Asterisk {
                self.advance();
                columns.push(SelectColumn::Asterisk);
            } else {
                let expr = self.parse_expr()?;
                columns.push(SelectColumn::Expr(expr));
            }

            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        // FROM clause
        self.expect(Token::From)?;
        let table = self.parse_ident()?;

        // Optional WHERE clause
        let where_clause = if self.peek() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            columns,
            from: table,
            where_clause,
        }))
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table = self.parse_ident()?;
        self.expect(Token::Values)?;
        self.expect(Token::LParen)?;

        let mut values = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            values.push(expr);

            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(Token::RParen)?;

        Ok(Statement::Insert(InsertStatement { table, values }))
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;
        let table = self.parse_ident()?;
        self.expect(Token::LParen)?;

        let mut columns = Vec::new();
        loop {
            let name = self.parse_ident()?;
            let data_type = self.parse_data_type()?;
            columns.push(ColumnDef { name, data_type });

            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(Token::RParen)?;

        Ok(Statement::CreateTable(CreateTableStatement { table, columns }))
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        match self.peek() {
            Token::Int => {
                self.advance();
                Ok(DataType::Int)
            }
            Token::Varchar => {
                self.advance();
                Ok(DataType::Varchar)
            }
            _ => bail!("expected data type, got {:?}", self.peek()),
        }
    }

    fn parse_ident(&mut self) -> Result<String> {
        match self.peek().clone() {
            Token::Ident(s) => {
                self.advance();
                Ok(s)
            }
            _ => bail!("expected identifier, got {:?}", self.peek()),
        }
    }

    // Expression parsing with precedence
    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_comparison()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_additive()?;

        let op = match self.peek() {
            Token::Eq => BinaryOperator::Eq,
            Token::Ne => BinaryOperator::Ne,
            Token::Lt => BinaryOperator::Lt,
            Token::Le => BinaryOperator::Le,
            Token::Gt => BinaryOperator::Gt,
            Token::Ge => BinaryOperator::Ge,
            _ => return Ok(left),
        };
        self.advance();
        let right = self.parse_additive()?;

        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOperator::Add,
                Token::Minus => BinaryOperator::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Asterisk => BinaryOperator::Mul,
                Token::Slash => BinaryOperator::Div,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        match self.peek() {
            Token::Not => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(expr),
                })
            }
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Neg,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            Token::Integer(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(n)))
            }
            Token::String(s) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Token::Ident(s) => {
                self.advance();
                Ok(Expr::Column(s))
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            }
            _ => bail!("unexpected token in expression: {:?}", self.peek()),
        }
    }
}

pub fn parse(sql: &str) -> Result<Statement> {
    let mut lexer = crate::lexer::Lexer::new(sql);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}
