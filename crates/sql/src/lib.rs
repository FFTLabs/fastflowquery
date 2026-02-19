#![deny(missing_docs)]

//! Minimal SQL parsing facade used by planner/frontend code.
//!
//! Architecture role:
//! - wraps `sqlparser` invocation and normalizes parse errors into FFQ error types
//! - keeps parser dependency details out of higher-level crates
//!
//! Key API:
//! - [`parse_sql`]
//!
//! Feature flags:
//! - none.

use ffq_common::{FfqError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse one SQL string into `sqlparser` statements using the generic dialect.
///
/// Returns [`ffq_common::FfqError::Planning`] when parsing fails.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    let dialect = GenericDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| FfqError::Planning(e.to_string()))
}
