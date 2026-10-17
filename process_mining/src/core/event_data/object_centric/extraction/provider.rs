//! Pulling rows out of a data source, one table at a time.

use std::fmt::Debug;
use std::ops::ControlFlow;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::compile::SqlDialect;
use super::value::Value;

/// A source of rows for the tables a blueprint's [`Source`](super::blueprint::NodeOp::Source)
/// nodes name.
///
/// `scan` is push-based: it calls `f` once per row rather than returning a collection, so a
/// `Source -> Filter` chain never holds more than one row. Implementations are expected to stream
/// from their backend the same way rather than collecting into a `Vec` first.
pub trait RowProvider: Debug {
    /// Call `f` once for every row of `table`, restricted to `columns` and in that order.
    ///
    /// A column named in `columns` that the table does not have is an error, not a `Null`
    /// fill-in: callers only ever ask for columns a [`Catalog`](super::catalog::Catalog) or a
    /// prior successful call already confirmed exist.
    ///
    /// An empty `columns` is a legitimate request for zero-length rows, not a request for
    /// every column: a mapping whose target names only constants still needs one call per row.
    ///
    /// A [`ControlFlow::Break`] from `f` must abandon the scan and return `Ok(())` rather than
    /// reading the table to the end.
    ///
    /// # Errors
    /// Returns [`ProviderError`] if `table` is unknown, a column in `columns` does not exist on
    /// it, or the underlying source fails while reading.
    fn scan(
        &self,
        table: &str,
        columns: &[&str],
        f: &mut dyn FnMut(&[Value]) -> ControlFlow<()>,
    ) -> Result<(), ProviderError>;

    /// The SQL dialect this provider can execute through [`Self::scan_query`], or `None`
    /// (the default) if it cannot run SQL at all.
    ///
    /// A CSV- or API-backed provider says `None` here and is never handed a query. The answer is
    /// a hint, not a promise: the executor falls back to row-level execution whenever a provider
    /// that claimed a dialect answers [`ProviderError::QueryUnsupported`].
    ///
    /// # Declaring a dialect is a statement about the catalog, not only about the engine
    ///
    /// The SQL handed to [`Self::scan_query`] comes from the same emitter
    /// [`compile`](super::compile()) uses, which decides literal coercion, join-key comparability
    /// and identity rendering from the [`Catalog`](super::catalog::Catalog)'s declared column
    /// types. That reproduces this extractor's row-level semantics only where the source's runtime
    /// values really have the declared kinds. A dynamically typed engine such as `SQLite`, which
    /// stores a type per cell, must keep the default `None`, or a pushed-down join could return
    /// different rows than the row-level path.
    fn query_dialect(&self) -> Option<SqlDialect> {
        None
    }

    /// Run `sql` and call `f` once per result row, under the same push-based,
    /// [`ControlFlow`]-honouring contract as [`Self::scan`].
    ///
    /// `sql` is a single `SELECT` in this provider's [`Self::query_dialect`] whose result columns
    /// are exactly `columns`, in that order.
    ///
    /// The default refuses with [`ProviderError::QueryUnsupported`], which is also the answer for
    /// a query this provider happens not to be able to run. The caller carries on without it.
    ///
    /// # Errors
    /// Returns [`ProviderError::QueryUnsupported`] if this provider cannot execute SQL (the
    /// default), or [`ProviderError::Backend`] if the query itself failed.
    fn scan_query(
        &self,
        sql: &str,
        columns: &[&str],
        f: &mut dyn FnMut(&[Value]) -> ControlFlow<()>,
    ) -> Result<(), ProviderError> {
        let _ = (sql, columns, f);
        Err(ProviderError::QueryUnsupported)
    }
}

/// Why a [`RowProvider::scan`] call failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ProviderError {
    /// `table` has no entry in this provider.
    UnknownTable {
        /// The table name.
        table: String,
    },
    /// A requested column is not present on `table`.
    UnknownColumn {
        /// The table name.
        table: String,
        /// The column name.
        column: String,
    },
    /// This provider cannot execute SQL: it reports no [`RowProvider::query_dialect`], or the
    /// query it was handed is one it cannot run. Not fatal: the caller falls back to executing
    /// the node row by row.
    QueryUnsupported,
    /// The underlying source failed while reading.
    Backend {
        /// The table being read when the failure happened.
        table: String,
        /// The backend's error message.
        message: String,
    },
}

impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderError::UnknownTable { table } => write!(f, "unknown table '{table}'"),
            ProviderError::UnknownColumn { table, column } => {
                write!(f, "table '{table}' has no column '{column}'")
            }
            ProviderError::QueryUnsupported => {
                write!(f, "this provider cannot execute SQL queries")
            }
            ProviderError::Backend { table, message } => {
                write!(f, "reading '{table}' failed: {message}")
            }
        }
    }
}

impl std::error::Error for ProviderError {}

/// The first `limit` rows of `table`, restricted to `columns`, for showing a person what the
/// data looks like while they build a blueprint against it.
///
/// Built on [`RowProvider::scan`], so it stops row transfer by breaking out of the scan rather
/// than bounding the query. A backend that can express the bound itself will do better.
///
/// A `NULL` cell comes back as `None`, distinct from an empty string.
///
/// # Errors
/// Returns whatever [`RowProvider::scan`] reports for an unknown table or column.
pub fn preview_rows(
    provider: &dyn RowProvider,
    table: &str,
    columns: &[&str],
    limit: usize,
) -> Result<super::catalog::TablePreview, ProviderError> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    if limit > 0 {
        provider.scan(table, columns, &mut |values| {
            rows.push(values.iter().map(Value::display_string).collect());
            if rows.len() >= limit {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?;
    }
    Ok(super::catalog::TablePreview {
        columns: columns.iter().map(|c| (*c).to_string()).collect(),
        rows,
    })
}

/// Every distinct non-null value of `table.column`, as text, for populating
/// [`ExtractionCatalog::with_domain`](super::catalog::ExtractionCatalog::with_domain).
///
/// This reads the whole column, because a partial answer would silently drop rows: the compiler
/// lowers a dynamic type name from the domain. A provider whose backend has `SELECT DISTINCT`
/// should prefer that. See
/// [`DbconRowProvider::distinct_values`](super::DbconRowProvider::distinct_values).
///
/// Values come back in first-seen order.
///
/// # Errors
/// Returns whatever [`RowProvider::scan`] reports for an unknown table or column.
pub fn distinct_column_values(
    provider: &dyn RowProvider,
    table: &str,
    column: &str,
) -> Result<Vec<String>, ProviderError> {
    let mut seen = std::collections::HashSet::new();
    let mut values = Vec::new();
    provider.scan(table, &[column], &mut |row| {
        if let Some(v) = row.first().and_then(Value::display_string) {
            if seen.insert(v.clone()) {
                values.push(v);
            }
        }
        ControlFlow::Continue(())
    })?;
    Ok(values)
}

/// The [`ProviderError`] a `SQLite` error message names, if it names one.
///
/// `SQLite` distinguishes a missing table from a missing column only in its message text, so every
/// provider reading a `SQLite` file has to parse it. Shared so they cannot disagree.
pub(crate) fn sqlite_message_error(table: &str, message: &str) -> Option<ProviderError> {
    if message.contains("no such table") {
        return Some(ProviderError::UnknownTable {
            table: table.to_string(),
        });
    }
    message
        .split("no such column: ")
        .nth(1)
        .map(|column| ProviderError::UnknownColumn {
            table: table.to_string(),
            column: column.to_string(),
        })
}
