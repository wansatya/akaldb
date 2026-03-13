//! CQL Parser — converts token stream into AST.
//!
//! Implements a recursive descent parser for CQL. This is the simplest
//! parser architecture that handles all CQL constructs without ambiguity.
//!
//! ## Grammar (informal BNF)
//!
//! ```text
//! query       ::= find_query | path_query
//! find_query  ::= "FIND" ident [where_clause] [time_clause] [count_clause] [group_clause]
//! path_query  ::= "PATH" ident ("->" ident)+ [where_clause]
//! where_clause::= "WHERE" condition ("AND" condition)*
//! condition   ::= property operator value
//! property    ::= ident ("." ident)*
//! operator    ::= "=" | "!=" | ">" | "<" | ">=" | "<="
//! value       ::= ident | string_lit | integer | float | bool
//! time_clause ::= "TIME" ("latest" | "earliest")
//! count_clause::= "COUNT" ident operator integer
//! group_clause::= "GROUP" "BY" ident
//! ```

use super::ast::*;
use super::lexer::Token;
use std::fmt;

// =============================================================================
// Parser Errors
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Expected a specific token but found something else
    Expected(String, Token),
    /// Unexpected end of input
    UnexpectedEof,
    /// PATH query requires at least 2 labels
    PathTooShort,
    /// Generic parse error with message
    Message(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Expected(expected, found) => {
                write!(f, "expected {}, found '{}'", expected, found)
            }
            ParseError::UnexpectedEof => write!(f, "unexpected end of input"),
            ParseError::PathTooShort => write!(f, "PATH query requires at least 2 labels"),
            ParseError::Message(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

// =============================================================================
// Parser
// =============================================================================

/// Recursive descent parser for CQL.
///
/// Consumes tokens one by one, building the AST. Each `parse_*` method corresponds
/// to a grammar rule. No backtracking is needed because CQL keywords unambiguously
/// determine which production to use.
pub struct Parser {
    tokens: Vec<Token>,
    /// Current position in the token stream
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    /// Parse the complete query.
    pub fn parse(&mut self) -> Result<Query, ParseError> {
        match self.peek() {
            Token::Find => self.parse_find().map(Query::Find),
            Token::Path => self.parse_path().map(Query::Path),
            other => Err(ParseError::Expected(
                "FIND or PATH".to_string(),
                other.clone(),
            )),
        }
    }

    // =========================================================================
    // FIND query
    // =========================================================================

    fn parse_find(&mut self) -> Result<FindQuery, ParseError> {
        self.expect(Token::Find)?;
        let label = self.expect_ident()?;

        let mut query = FindQuery {
            label,
            conditions: Vec::new(),
            time_order: None,
            count_filter: None,
            group_by: None,
        };

        // Parse optional clauses in any order (WHERE, TIME, COUNT, GROUP BY)
        loop {
            match self.peek() {
                Token::Where => {
                    self.advance();
                    query.conditions = self.parse_conditions()?;
                }
                Token::Time => {
                    self.advance();
                    query.time_order = Some(self.parse_time_order()?);
                }
                Token::Count => {
                    self.advance();
                    query.count_filter = Some(self.parse_count_filter()?);
                }
                Token::Group => {
                    self.advance();
                    self.expect(Token::By)?;
                    query.group_by = Some(self.expect_ident()?);
                }
                Token::Eof => break,
                _ => break,
            }
        }

        Ok(query)
    }

    // =========================================================================
    // PATH query
    // =========================================================================

    fn parse_path(&mut self) -> Result<PathQuery, ParseError> {
        self.expect(Token::Path)?;

        let mut labels = vec![self.expect_ident()?];

        // Parse: -> Label -> Label -> ...
        while self.peek() == &Token::Arrow {
            self.advance();
            labels.push(self.expect_ident()?);
        }

        if labels.len() < 2 {
            return Err(ParseError::PathTooShort);
        }

        let conditions = if self.peek() == &Token::Where {
            self.advance();
            self.parse_conditions()?
        } else {
            Vec::new()
        };

        Ok(PathQuery { labels, conditions })
    }

    // =========================================================================
    // WHERE conditions
    // =========================================================================

    /// Parse one or more conditions separated by AND.
    fn parse_conditions(&mut self) -> Result<Vec<Condition>, ParseError> {
        let mut conditions = vec![self.parse_condition()?];

        while self.peek() == &Token::And {
            self.advance();
            conditions.push(self.parse_condition()?);
        }

        Ok(conditions)
    }

    /// Parse a single condition: property operator value
    fn parse_condition(&mut self) -> Result<Condition, ParseError> {
        let property = self.parse_property_path()?;
        let operator = self.parse_operator()?;
        let value = self.parse_value()?;

        Ok(Condition {
            property,
            operator,
            value,
        })
    }

    /// Parse a dotted property path: `complaints.category` or just `name`
    fn parse_property_path(&mut self) -> Result<String, ParseError> {
        let mut path = self.expect_ident()?;

        while self.peek() == &Token::Dot {
            self.advance();
            let part = self.expect_ident()?;
            path.push('.');
            path.push_str(&part);
        }

        Ok(path)
    }

    /// Parse a comparison operator.
    fn parse_operator(&mut self) -> Result<Operator, ParseError> {
        let token = self.advance();
        match token {
            Token::Eq => Ok(Operator::Eq),
            Token::NotEq => Ok(Operator::NotEq),
            Token::Gt => Ok(Operator::Gt),
            Token::Lt => Ok(Operator::Lt),
            Token::Gte => Ok(Operator::Gte),
            Token::Lte => Ok(Operator::Lte),
            other => Err(ParseError::Expected("operator".to_string(), other)),
        }
    }

    /// Parse a value (string, number, bool).
    fn parse_value(&mut self) -> Result<Value, ParseError> {
        let token = self.advance();
        match token {
            Token::Ident(s) => Ok(Value::String(s)),
            Token::StringLit(s) => Ok(Value::String(s)),
            Token::Integer(n) => Ok(Value::Integer(n)),
            Token::Float(n) => Ok(Value::Float(n)),
            Token::Bool(b) => Ok(Value::Bool(b)),
            other => Err(ParseError::Expected("value".to_string(), other)),
        }
    }

    // =========================================================================
    // TIME clause
    // =========================================================================

    fn parse_time_order(&mut self) -> Result<TimeOrder, ParseError> {
        let token = self.advance();
        match token {
            Token::Latest => Ok(TimeOrder::Latest),
            Token::Earliest => Ok(TimeOrder::Earliest),
            other => Err(ParseError::Expected("latest or earliest".to_string(), other)),
        }
    }

    // =========================================================================
    // COUNT clause
    // =========================================================================

    fn parse_count_filter(&mut self) -> Result<CountFilter, ParseError> {
        let relation = self.expect_ident()?;
        let operator = self.parse_operator()?;
        let value = match self.advance() {
            Token::Integer(n) => n,
            other => return Err(ParseError::Expected("integer".to_string(), other)),
        };

        Ok(CountFilter {
            relation,
            operator,
            value,
        })
    }

    // =========================================================================
    // Token helpers
    // =========================================================================

    /// Peek at the current token without consuming it.
    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    /// Consume and return the current token.
    fn advance(&mut self) -> Token {
        let token = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        token
    }

    /// Expect a specific token, consuming it. Returns error if mismatch.
    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        let token = self.advance();
        if token == expected {
            Ok(())
        } else {
            Err(ParseError::Expected(format!("{}", expected), token))
        }
    }

    /// Expect and consume an identifier token, returning its string value.
    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            other => Err(ParseError::Expected("identifier".to_string(), other)),
        }
    }
}

// =============================================================================
// Convenience parse function
// =============================================================================

/// Parse a CQL query string into an AST.
///
/// This is the main entry point for CQL parsing. It tokenizes and parses in one call.
///
/// # Example
/// ```
/// use akaldb::query::parser::parse_cql;
/// use akaldb::query::ast::*;
///
/// let query = parse_cql("FIND Company WHERE name = \"Acme\"").unwrap();
/// match query {
///     Query::Find(fq) => {
///         assert_eq!(fq.label, "Company");
///         assert_eq!(fq.conditions.len(), 1);
///     }
///     _ => panic!("expected FIND query"),
/// }
/// ```
pub fn parse_cql(input: &str) -> Result<Query, Box<dyn std::error::Error>> {
    let tokens = super::lexer::tokenize(input)?;
    let mut parser = Parser::new(tokens);
    Ok(parser.parse()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_find() {
        let query = parse_cql("FIND Company").unwrap();
        match query {
            Query::Find(fq) => {
                assert_eq!(fq.label, "Company");
                assert!(fq.conditions.is_empty());
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_find_with_where() {
        let query = parse_cql("FIND Company WHERE category = Ghosting").unwrap();
        match query {
            Query::Find(fq) => {
                assert_eq!(fq.label, "Company");
                assert_eq!(fq.conditions.len(), 1);
                assert_eq!(fq.conditions[0].property, "category");
                assert_eq!(fq.conditions[0].operator, Operator::Eq);
                assert_eq!(fq.conditions[0].value, Value::String("Ghosting".into()));
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_find_with_dotted_property() {
        let query = parse_cql("FIND Company WHERE complaints.category = Ghosting").unwrap();
        match query {
            Query::Find(fq) => {
                assert_eq!(fq.conditions[0].property, "complaints.category");
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_find_with_multiple_conditions() {
        let query = parse_cql("FIND Worker WHERE role = Engineer AND work_hours > 50").unwrap();
        match query {
            Query::Find(fq) => {
                assert_eq!(fq.conditions.len(), 2);
                assert_eq!(fq.conditions[0].property, "role");
                assert_eq!(fq.conditions[1].property, "work_hours");
                assert_eq!(fq.conditions[1].operator, Operator::Gt);
                assert_eq!(fq.conditions[1].value, Value::Integer(50));
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_find_with_time() {
        let query = parse_cql("FIND Contract WHERE clause_type = Renewal TIME latest").unwrap();
        match query {
            Query::Find(fq) => {
                assert_eq!(fq.time_order, Some(TimeOrder::Latest));
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_find_with_count() {
        let query = parse_cql("FIND Company COUNT complaints > 3").unwrap();
        match query {
            Query::Find(fq) => {
                let cf = fq.count_filter.unwrap();
                assert_eq!(cf.relation, "complaints");
                assert_eq!(cf.operator, Operator::Gt);
                assert_eq!(cf.value, 3);
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_find_with_group_by() {
        let query = parse_cql("FIND Worker WHERE work_hours > 50 GROUP BY industry").unwrap();
        match query {
            Query::Find(fq) => {
                assert_eq!(fq.group_by, Some("industry".into()));
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_path() {
        let query = parse_cql("PATH Company -> Complaint -> Evidence").unwrap();
        match query {
            Query::Path(pq) => {
                assert_eq!(pq.labels, vec!["Company", "Complaint", "Evidence"]);
                assert!(pq.conditions.is_empty());
            }
            _ => panic!("expected PATH query"),
        }
    }

    #[test]
    fn test_parse_path_with_where() {
        let query = parse_cql("PATH Company -> Complaint -> Evidence WHERE category = Scam").unwrap();
        match query {
            Query::Path(pq) => {
                assert_eq!(pq.labels.len(), 3);
                assert_eq!(pq.conditions.len(), 1);
                assert_eq!(pq.conditions[0].property, "category");
            }
            _ => panic!("expected PATH query"),
        }
    }

    #[test]
    fn test_parse_quoted_string_value() {
        let query = parse_cql(r#"FIND Company WHERE name = "Company X""#).unwrap();
        match query {
            Query::Find(fq) => {
                assert_eq!(fq.conditions[0].value, Value::String("Company X".into()));
            }
            _ => panic!("expected FIND query"),
        }
    }

    #[test]
    fn test_parse_error_no_keyword() {
        let result = parse_cql("SELECT * FROM table");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_path_too_short() {
        let result = parse_cql("PATH Company");
        assert!(result.is_err());
    }
}
