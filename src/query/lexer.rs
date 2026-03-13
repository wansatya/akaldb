//! Lexer (tokenizer) for CQL (Context Query Language).
//!
//! Converts raw CQL text into a stream of tokens for the parser.
//! The lexer is intentionally simple — no regex, no external parser libraries.
//! This keeps the binary small and avoids heavy dependencies per SPECS.md goals.
//!
//! ## Token Types
//!
//! - **Keywords**: FIND, PATH, WHERE, TIME, COUNT, GROUP, BY, AND
//! - **Operators**: =, !=, >, <, >=, <=
//! - **Symbols**: -> (arrow for path traversal)
//! - **Literals**: strings (quoted or unquoted identifiers), integers, floats
//!
//! ## Design Notes
//!
//! - Case-insensitive keywords (FIND = find = Find)
//! - Unquoted words are treated as identifiers/string values
//! - Numbers are parsed as integers first; if a `.` follows, as floats
//! - Whitespace is consumed but not emitted as tokens

use std::fmt;
use std::iter::Peekable;
use std::str::Chars;

// =============================================================================
// Token Types
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Find,
    Path,
    Where,
    Time,
    Count,
    Group,
    By,
    And,
    Latest,
    Earliest,

    // Operators
    Eq,         // =
    NotEq,      // !=
    Gt,         // >
    Lt,         // <
    Gte,        // >=
    Lte,        // <=

    // Symbols
    Arrow,      // ->
    Dot,        // .

    // Literals
    Ident(String),      // Unquoted identifier (also used as string values)
    StringLit(String),  // Quoted string "..."
    Integer(i64),       // Integer literal
    Float(f64),         // Float literal
    Bool(bool),         // true/false

    // End of input
    Eof,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Find => write!(f, "FIND"),
            Token::Path => write!(f, "PATH"),
            Token::Where => write!(f, "WHERE"),
            Token::Time => write!(f, "TIME"),
            Token::Count => write!(f, "COUNT"),
            Token::Group => write!(f, "GROUP"),
            Token::By => write!(f, "BY"),
            Token::And => write!(f, "AND"),
            Token::Latest => write!(f, "latest"),
            Token::Earliest => write!(f, "earliest"),
            Token::Eq => write!(f, "="),
            Token::NotEq => write!(f, "!="),
            Token::Gt => write!(f, ">"),
            Token::Lt => write!(f, "<"),
            Token::Gte => write!(f, ">="),
            Token::Lte => write!(f, "<="),
            Token::Arrow => write!(f, "->"),
            Token::Dot => write!(f, "."),
            Token::Ident(s) => write!(f, "{}", s),
            Token::StringLit(s) => write!(f, "\"{}\"", s),
            Token::Integer(n) => write!(f, "{}", n),
            Token::Float(n) => write!(f, "{}", n),
            Token::Bool(b) => write!(f, "{}", b),
            Token::Eof => write!(f, "EOF"),
        }
    }
}

// =============================================================================
// Lexer Errors
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum LexError {
    UnexpectedChar(char, usize),
    UnterminatedString(usize),
    InvalidNumber(String, usize),
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LexError::UnexpectedChar(c, pos) => write!(f, "unexpected character '{}' at position {}", c, pos),
            LexError::UnterminatedString(pos) => write!(f, "unterminated string starting at position {}", pos),
            LexError::InvalidNumber(s, pos) => write!(f, "invalid number '{}' at position {}", s, pos),
        }
    }
}

impl std::error::Error for LexError {}

// =============================================================================
// Lexer
// =============================================================================

/// Tokenizes a CQL query string into a vector of tokens.
///
/// This is a single-pass lexer that processes the input left-to-right.
/// No lookahead beyond one character is needed, keeping it simple and fast.
pub fn tokenize(input: &str) -> Result<Vec<Token>, LexError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    let mut pos = 0usize;

    while let Some(&ch) = chars.peek() {
        match ch {
            // Skip whitespace
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
                pos += 1;
            }

            // Operators and symbols
            '=' => {
                chars.next();
                pos += 1;
                tokens.push(Token::Eq);
            }
            '!' => {
                chars.next();
                pos += 1;
                if chars.peek() == Some(&'=') {
                    chars.next();
                    pos += 1;
                    tokens.push(Token::NotEq);
                } else {
                    return Err(LexError::UnexpectedChar('!', pos - 1));
                }
            }
            '>' => {
                chars.next();
                pos += 1;
                if chars.peek() == Some(&'=') {
                    chars.next();
                    pos += 1;
                    tokens.push(Token::Gte);
                } else {
                    tokens.push(Token::Gt);
                }
            }
            '<' => {
                chars.next();
                pos += 1;
                if chars.peek() == Some(&'=') {
                    chars.next();
                    pos += 1;
                    tokens.push(Token::Lte);
                } else {
                    tokens.push(Token::Lt);
                }
            }
            '-' => {
                chars.next();
                pos += 1;
                if chars.peek() == Some(&'>') {
                    chars.next();
                    pos += 1;
                    tokens.push(Token::Arrow);
                } else {
                    // Could be a negative number
                    if chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                        let num = lex_number(&mut chars, &mut pos, true)?;
                        tokens.push(num);
                    } else {
                        return Err(LexError::UnexpectedChar('-', pos - 1));
                    }
                }
            }
            '.' => {
                chars.next();
                pos += 1;
                tokens.push(Token::Dot);
            }

            // Quoted string literals
            '"' | '\'' => {
                let quote = ch;
                chars.next();
                let start_pos = pos;
                pos += 1;
                let s = lex_string(&mut chars, &mut pos, quote, start_pos)?;
                tokens.push(Token::StringLit(s));
            }

            // Numbers
            '0'..='9' => {
                let num = lex_number(&mut chars, &mut pos, false)?;
                tokens.push(num);
            }

            // Identifiers and keywords
            'a'..='z' | 'A'..='Z' | '_' => {
                let ident = lex_identifier(&mut chars, &mut pos);
                let token = match ident.to_uppercase().as_str() {
                    "FIND" => Token::Find,
                    "PATH" => Token::Path,
                    "WHERE" => Token::Where,
                    "TIME" => Token::Time,
                    "COUNT" => Token::Count,
                    "GROUP" => Token::Group,
                    "BY" => Token::By,
                    "AND" => Token::And,
                    "LATEST" => Token::Latest,
                    "EARLIEST" => Token::Earliest,
                    "TRUE" => Token::Bool(true),
                    "FALSE" => Token::Bool(false),
                    _ => Token::Ident(ident),
                };
                tokens.push(token);
            }

            _ => return Err(LexError::UnexpectedChar(ch, pos)),
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

/// Lex a quoted string (supports both single and double quotes).
fn lex_string(
    chars: &mut Peekable<Chars>,
    pos: &mut usize,
    quote: char,
    start_pos: usize,
) -> Result<String, LexError> {
    let mut s = String::new();
    loop {
        match chars.next() {
            Some(c) if c == quote => {
                *pos += 1;
                return Ok(s);
            }
            Some('\\') => {
                *pos += 1;
                // Simple escape sequences
                match chars.next() {
                    Some('n') => { s.push('\n'); *pos += 1; }
                    Some('t') => { s.push('\t'); *pos += 1; }
                    Some('\\') => { s.push('\\'); *pos += 1; }
                    Some(c) if c == quote => { s.push(c); *pos += 1; }
                    Some(c) => { s.push('\\'); s.push(c); *pos += 1; }
                    None => return Err(LexError::UnterminatedString(start_pos)),
                }
            }
            Some(c) => {
                s.push(c);
                *pos += 1;
            }
            None => return Err(LexError::UnterminatedString(start_pos)),
        }
    }
}

/// Lex an identifier (alphanumeric + underscores).
fn lex_identifier(chars: &mut Peekable<Chars>, pos: &mut usize) -> String {
    let mut ident = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_alphanumeric() || ch == '_' {
            ident.push(ch);
            chars.next();
            *pos += 1;
        } else {
            break;
        }
    }
    ident
}

/// Lex a number (integer or float).
fn lex_number(
    chars: &mut Peekable<Chars>,
    pos: &mut usize,
    negative: bool,
) -> Result<Token, LexError> {
    let start_pos = *pos;
    let mut num_str = if negative {
        "-".to_string()
    } else {
        String::new()
    };
    let mut is_float = false;

    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            num_str.push(ch);
            chars.next();
            *pos += 1;
        } else if ch == '.' && !is_float {
            is_float = true;
            num_str.push(ch);
            chars.next();
            *pos += 1;
        } else {
            break;
        }
    }

    if is_float {
        num_str
            .parse::<f64>()
            .map(Token::Float)
            .map_err(|_| LexError::InvalidNumber(num_str, start_pos))
    } else {
        num_str
            .parse::<i64>()
            .map(Token::Integer)
            .map_err(|_| LexError::InvalidNumber(num_str, start_pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_query() {
        let tokens = tokenize("FIND Company").unwrap();
        assert_eq!(tokens, vec![
            Token::Find,
            Token::Ident("Company".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_find_with_where() {
        let tokens = tokenize("FIND Company WHERE complaints.category = Ghosting").unwrap();
        assert_eq!(tokens, vec![
            Token::Find,
            Token::Ident("Company".into()),
            Token::Where,
            Token::Ident("complaints".into()),
            Token::Dot,
            Token::Ident("category".into()),
            Token::Eq,
            Token::Ident("Ghosting".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_path_query() {
        let tokens = tokenize("PATH Company -> Complaint -> Evidence").unwrap();
        assert_eq!(tokens, vec![
            Token::Path,
            Token::Ident("Company".into()),
            Token::Arrow,
            Token::Ident("Complaint".into()),
            Token::Arrow,
            Token::Ident("Evidence".into()),
            Token::Eof,
        ]);
    }

    #[test]
    fn test_operators() {
        let tokens = tokenize("a = 1 AND b != 2 AND c > 3 AND d < 4 AND e >= 5 AND f <= 6").unwrap();
        assert!(tokens.contains(&Token::Eq));
        assert!(tokens.contains(&Token::NotEq));
        assert!(tokens.contains(&Token::Gt));
        assert!(tokens.contains(&Token::Lt));
        assert!(tokens.contains(&Token::Gte));
        assert!(tokens.contains(&Token::Lte));
    }

    #[test]
    fn test_quoted_string() {
        let tokens = tokenize(r#"FIND Company WHERE name = "Company X""#).unwrap();
        assert!(tokens.contains(&Token::StringLit("Company X".into())));
    }

    #[test]
    fn test_negative_number() {
        let tokens = tokenize("WHERE score > -5").unwrap();
        assert!(tokens.contains(&Token::Integer(-5)));
    }

    #[test]
    fn test_float() {
        let tokens = tokenize("WHERE rating > 3.5").unwrap();
        assert!(tokens.contains(&Token::Float(3.5)));
    }

    #[test]
    fn test_time_keyword() {
        let tokens = tokenize("FIND Contract TIME latest").unwrap();
        assert!(tokens.contains(&Token::Time));
        assert!(tokens.contains(&Token::Latest));
    }

    #[test]
    fn test_count_group_by() {
        let tokens = tokenize("FIND Worker WHERE work_hours > 50 GROUP BY industry").unwrap();
        assert!(tokens.contains(&Token::Group));
        assert!(tokens.contains(&Token::By));
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let tokens = tokenize("find company where x = 1").unwrap();
        assert_eq!(tokens[0], Token::Find);
        assert_eq!(tokens[2], Token::Where);
    }
}
