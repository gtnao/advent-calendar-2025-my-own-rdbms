use anyhow::{bail, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Create,
    Table,
    Int,
    Varchar,
    And,
    Or,
    Not,
    Null,
    True,
    False,
    Delete,
    Update,
    Set,
    Begin,
    Commit,
    Rollback,

    // Identifiers and literals
    Ident(String),
    Integer(i64),
    String(String),

    // Symbols
    Asterisk,  // *
    Comma,     // ,
    Semicolon, // ;
    LParen,    // (
    RParen,    // )
    Eq,        // =
    Ne,        // <>
    Lt,        // <
    Le,        // <=
    Gt,        // >
    Ge,        // >=
    Plus,      // +
    Minus,     // -
    Slash,     // /

    Eof,
}

pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Lexer {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.peek();
        self.pos += 1;
        ch
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }

    fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();

        let ch = match self.peek() {
            Some(ch) => ch,
            None => return Ok(Token::Eof),
        };

        // Single-char tokens
        let token = match ch {
            '*' => {
                self.advance();
                Token::Asterisk
            }
            ',' => {
                self.advance();
                Token::Comma
            }
            ';' => {
                self.advance();
                Token::Semicolon
            }
            '(' => {
                self.advance();
                Token::LParen
            }
            ')' => {
                self.advance();
                Token::RParen
            }
            '+' => {
                self.advance();
                Token::Plus
            }
            '-' => {
                self.advance();
                Token::Minus
            }
            '/' => {
                self.advance();
                Token::Slash
            }
            '=' => {
                self.advance();
                Token::Eq
            }
            '<' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Token::Le
                } else if self.peek() == Some('>') {
                    self.advance();
                    Token::Ne
                } else {
                    Token::Lt
                }
            }
            '>' => {
                self.advance();
                if self.peek() == Some('=') {
                    self.advance();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }
            '\'' => self.read_string()?,
            _ if ch.is_ascii_digit() => self.read_number()?,
            _ if ch.is_ascii_alphabetic() || ch == '_' => self.read_ident_or_keyword()?,
            _ => bail!("unexpected character: {ch}"),
        };

        Ok(token)
    }

    fn read_string(&mut self) -> Result<Token> {
        self.advance(); // consume opening quote
        let mut s = String::new();
        loop {
            match self.peek() {
                Some('\'') => {
                    self.advance();
                    break;
                }
                Some(ch) => {
                    self.advance();
                    s.push(ch);
                }
                None => bail!("unterminated string literal"),
            }
        }
        Ok(Token::String(s))
    }

    fn read_number(&mut self) -> Result<Token> {
        let mut s = String::new();
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                s.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        let n: i64 = s.parse()?;
        Ok(Token::Integer(n))
    }

    fn read_ident_or_keyword(&mut self) -> Result<Token> {
        let mut s = String::new();
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                s.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        let token = match s.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "INT" => Token::Int,
            "INTEGER" => Token::Int,
            "VARCHAR" => Token::Varchar,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "NULL" => Token::Null,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "DELETE" => Token::Delete,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "BEGIN" => Token::Begin,
            "COMMIT" => Token::Commit,
            "ROLLBACK" => Token::Rollback,
            _ => Token::Ident(s),
        };
        Ok(token)
    }
}
