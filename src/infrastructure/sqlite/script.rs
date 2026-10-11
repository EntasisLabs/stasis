//! Split a schema batch into single statements.
//!
//! Workers `sql.exec` is documented to reject transaction-control statements
//! (`BEGIN`, `COMMIT`, `SAVEPOINT`) and storage `PRAGMA`s. The shared DDL has
//! none of those; this splitter is the check that keeps it that way, and it
//! feeds one statement per exec call.

use crate::domain::errors::Result;

use super::codec::sql_err;

const REJECTED: &[&str] = &[
    "BEGIN",
    "COMMIT",
    "END",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "PRAGMA",
];

pub fn split_sql_script(script: &str) -> Result<Vec<String>> {
    let mut statements = Vec::new();
    for part in script.split(';') {
        let statement = part.trim();
        if statement.is_empty() {
            continue;
        }
        let keyword = statement
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_start_matches('(');
        if REJECTED
            .iter()
            .any(|rejected| keyword.eq_ignore_ascii_case(rejected))
        {
            return Err(sql_err(
                "sqlite script",
                format!(
                    "statement `{keyword}` is not portable to Durable Object SQL; use SqlExecutor::transaction"
                ),
            ));
        }
        statements.push(statement.to_string());
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::sqlite::schema::MIGRATION_V1;

    #[test]
    fn migration_v1_splits_without_transaction_or_pragma() {
        let statements = split_sql_script(MIGRATION_V1).expect("split");
        assert!(statements.len() > 20);
        assert!(
            statements
                .iter()
                .any(|sql| sql.contains("CREATE TABLE job"))
        );
        assert!(
            statements
                .iter()
                .any(|sql| sql.contains("CREATE TABLE inbound_trigger"))
        );
        assert!(
            statements
                .iter()
                .any(|sql| sql.contains("CREATE TABLE resource_lease"))
        );
        assert!(
            statements
                .iter()
                .any(|sql| sql.contains("CREATE TABLE workflow_revision"))
        );
    }

    #[test]
    fn begin_is_rejected() {
        let err = split_sql_script("BEGIN IMMEDIATE; CREATE TABLE t (id INTEGER);").unwrap_err();
        assert!(err.to_string().contains("BEGIN"));
    }
}
