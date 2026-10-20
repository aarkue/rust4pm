//! Schema and column-domain facts about the data sources a blueprint reads.
//!
//! Deliberately not part of a blueprint: embedding a schema snapshot in the artifact makes it
//! bloated and lets it go stale silently. The caller supplies these, either discovered live or
//! loaded from a pinned snapshot.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::value::ValueKind;

/// One column's declared shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ColumnSchema {
    /// Column name.
    pub name: String,
    /// The source's own type name, verbatim, for example `INTEGER` or `timestamp`.
    pub col_type: String,
    /// Whether the source permits `NULL` here.
    pub nullable: bool,
}

/// The `col_type` to record for a column whose kind nothing was able to establish, chosen so
/// [`ColumnSchema::declared_kind`] reads it back as `None`.
pub(crate) const UNTYPED_COL_TYPE: &str = "UNKNOWN";

impl ColumnSchema {
    /// Map [`ColumnSchema::col_type`] to the [`ValueKind`] it denotes, for literal coercion in
    /// `Predicate::prepare`.
    ///
    /// `col_type` comes verbatim from a real database and its spelling, case and decoration vary
    /// by engine and by driver, so the match is case-insensitive and per word, splitting on
    /// every non-alphanumeric character and ignoring a trailing width. The first word that names
    /// a kind decides:
    ///
    /// - `bool`, `boolean`: [`ValueKind::Boolean`]
    /// - `date`, or any word holding `timestamp` or `datetime`: [`ValueKind::Timestamp`]
    ///   (`timestamp`, `TIMESTAMPTZ`, `DATE`, `SMALLDATETIME`)
    /// - `int`, `integer`, `bigint`, `smallint`, `tinyint`, `mediumint`, `serial`, `bigserial`,
    ///   `smallserial`: [`ValueKind::Integer`] (`INTEGER`, `int4`, `INT UNSIGNED`)
    /// - `float`, `double`, `real`, `numeric`, `decimal`: [`ValueKind::Float`]
    ///   (`DOUBLE PRECISION`, `NUMERIC(10,2)`)
    /// - `char`, `character`, `varchar`, `nchar`, `nvarchar`, `bpchar`, `text`, `citext`, `clob`,
    ///   `string`: [`ValueKind::Text`] (`VARCHAR(45)`, `character varying`)
    /// - anything else: `None`, which disables coercion for that column rather than guessing.
    ///
    /// Matched per word rather than by substring, since `interval` and `point` contain `int` and
    /// `daterange` contains `date`. A wrong kind here is silent: it coerces literals and drives
    /// the compiler's `CAST`.
    #[must_use]
    pub fn declared_kind(&self) -> Option<ValueKind> {
        let lowered = self.col_type.to_ascii_lowercase();
        lowered
            .split(|c: char| !c.is_ascii_alphanumeric())
            .find_map(word_kind)
    }
}

/// The [`ValueKind`] one already-lowercased word of a `col_type` names, if any. See
/// [`ColumnSchema::declared_kind`].
fn word_kind(word: &str) -> Option<ValueKind> {
    const INTEGER: &[&str] = &[
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "mediumint",
        "serial",
        "bigserial",
        "smallserial",
    ];
    const FLOAT: &[&str] = &["float", "double", "real", "numeric", "decimal"];
    const TEXT: &[&str] = &[
        "char",
        "character",
        "varchar",
        "nchar",
        "nvarchar",
        "bpchar",
        "text",
        "citext",
        "clob",
        "string",
    ];

    // A trailing width is part of the spelling, not of the type: `int4`, `float8`, `varchar2`.
    let base = word.trim_end_matches(|c: char| c.is_ascii_digit());
    if base == "bool" || base == "boolean" {
        Some(ValueKind::Boolean)
    } else if word == "date" || word.contains("timestamp") || word.contains("datetime") {
        Some(ValueKind::Timestamp)
    } else if INTEGER.contains(&base) {
        Some(ValueKind::Integer)
    } else if FLOAT.contains(&base) {
        Some(ValueKind::Float)
    } else if TEXT.contains(&base) {
        Some(ValueKind::Text)
    } else {
        None
    }
}

/// One table's declared shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TableSchema {
    /// Table name.
    pub name: String,
    /// Columns, keyed by name.
    pub columns: BTreeMap<String, ColumnSchema>,
}

impl TableSchema {
    /// Build a schema from `(name, type, nullable)` triples.
    pub fn new<I, S>(name: &str, columns: I) -> Self
    where
        I: IntoIterator<Item = (S, S, bool)>,
        S: Into<String>,
    {
        let columns = columns
            .into_iter()
            .map(|(n, t, nullable)| {
                let n = n.into();
                (
                    n.clone(),
                    ColumnSchema {
                        name: n,
                        col_type: t.into(),
                        nullable,
                    },
                )
            })
            .collect();
        Self {
            name: name.to_string(),
            columns,
        }
    }
}

/// What the compiler and extractor know about the sources a blueprint names.
pub trait Catalog: Debug {
    /// Whether `source_id` has any entry in the catalog, known or not.
    ///
    /// Lets a caller tell a source-id typo (no entry at all) apart from a known source with an
    /// unknown table, which [`Catalog::table`] alone cannot distinguish.
    fn has_source(&self, source_id: &str) -> bool;

    /// The schema of `table` in `source_id`, if known.
    fn table(&self, source_id: &str, table: &str) -> Option<&TableSchema>;

    /// The distinct values of a column, when the caller has determined them.
    ///
    /// Used to name per-type views when a type is read from a column. `None` means "not
    /// determined", which is different from a known-empty domain.
    fn column_domain(
        &self,
        source_id: &str,
        table: &str,
        column: &str,
    ) -> Option<&BTreeSet<String>>;
}

/// The concrete, serializable [`Catalog`].
///
/// This is the form that crosses a bindings boundary, that an editor holds and sends back, and
/// that gets pinned to disk so a compile can be reproduced against a schema that has since
/// changed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExtractionCatalog {
    /// Table schemas, keyed by source id then table name.
    pub tables: BTreeMap<String, BTreeMap<String, TableSchema>>,
    /// Column domains, keyed by source id, then table name, then column name.
    pub domains: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeSet<String>>>>,
    /// A handful of real rows per table, keyed by source id then table name, to show a person
    /// what the data looks like.
    ///
    /// Deliberately unreachable through the [`Catalog`] trait: unlike
    /// [`domains`](ExtractionCatalog::domains), a preview is incomplete, so compiling from one
    /// would emit views only for the types that happened to appear first.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub previews: BTreeMap<String, BTreeMap<String, TablePreview>>,
}

/// A few real rows of one table, for display only.
///
/// Rows are aligned to [`TablePreview::columns`] so a wide table can be read across. A cell is
/// `None` for SQL `NULL`, distinct from `Some(String::new())`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TablePreview {
    /// Column names, in the order the rows are aligned to.
    pub columns: Vec<String>,
    /// Rows, each the same length as `columns`.
    pub rows: Vec<Vec<Option<String>>>,
}

impl TablePreview {
    /// Distinct non-null values seen for `column`, in first-seen order, capped at `limit`.
    #[must_use]
    pub fn column_values(&self, column: &str, limit: usize) -> Vec<&str> {
        if limit == 0 {
            return Vec::new();
        }
        let Some(idx) = self.columns.iter().position(|c| c == column) else {
            return Vec::new();
        };
        let mut seen = Vec::new();
        for row in &self.rows {
            let Some(Some(v)) = row.get(idx) else {
                continue;
            };
            if !seen.contains(&v.as_str()) {
                seen.push(v.as_str());
                if seen.len() == limit {
                    break;
                }
            }
        }
        seen
    }
}

impl ExtractionCatalog {
    /// An empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a table schema, replacing any schema already recorded for that name.
    #[must_use]
    pub fn with_table(mut self, source_id: &str, schema: TableSchema) -> Self {
        self.tables
            .entry(source_id.to_string())
            .or_default()
            .insert(schema.name.clone(), schema);
        self
    }

    /// Record the distinct values of a column.
    #[must_use]
    pub fn with_domain<I: IntoIterator<Item = String>>(
        mut self,
        source_id: &str,
        table: &str,
        column: &str,
        values: I,
    ) -> Self {
        self.domains
            .entry(source_id.to_string())
            .or_default()
            .entry(table.to_string())
            .or_default()
            .insert(column.to_string(), values.into_iter().collect());
        self
    }

    /// Record preview rows for a table, replacing any already held.
    #[must_use]
    pub fn with_preview(mut self, source_id: &str, table: &str, preview: TablePreview) -> Self {
        self.previews
            .entry(source_id.to_string())
            .or_default()
            .insert(table.to_string(), preview);
        self
    }

    /// The preview rows for a table, if any were fetched.
    #[must_use]
    pub fn preview(&self, source_id: &str, table: &str) -> Option<&TablePreview> {
        self.previews.get(source_id)?.get(table)
    }
}

impl Catalog for ExtractionCatalog {
    fn has_source(&self, source_id: &str) -> bool {
        self.tables.contains_key(source_id)
    }

    fn table(&self, source_id: &str, table: &str) -> Option<&TableSchema> {
        self.tables.get(source_id)?.get(table)
    }

    fn column_domain(
        &self,
        source_id: &str,
        table: &str,
        column: &str,
    ) -> Option<&BTreeSet<String>> {
        self.domains.get(source_id)?.get(table)?.get(column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog() -> ExtractionCatalog {
        ExtractionCatalog::new()
            .with_table(
                "erp",
                TableSchema::new(
                    "orders",
                    [("id", "INTEGER", false), ("state", "TEXT", true)],
                ),
            )
            .with_domain(
                "erp",
                "orders",
                "state",
                ["draft".to_string(), "sale".to_string()],
            )
    }

    #[test]
    fn looks_up_tables_and_columns() {
        let c = catalog();
        let t = c.table("erp", "orders").expect("table present");
        assert_eq!(
            t.columns.get("id").map(|c| c.col_type.as_str()),
            Some("INTEGER")
        );
        assert!(c.table("erp", "missing").is_none());
        assert!(c.table("other", "orders").is_none());
    }

    #[test]
    fn has_source_distinguishes_an_unknown_source_from_an_unknown_table() {
        let c = catalog();
        assert!(c.has_source("erp"));
        assert!(!c.has_source("nope"));
    }

    #[test]
    fn distinguishes_an_unknown_domain_from_an_empty_one() {
        let c = catalog().with_domain("erp", "orders", "empty_col", Vec::<String>::new());
        assert_eq!(
            c.column_domain("erp", "orders", "state").map(BTreeSet::len),
            Some(2)
        );
        assert!(c.column_domain("erp", "orders", "id").is_none());
        assert_eq!(
            c.column_domain("erp", "orders", "empty_col")
                .map(BTreeSet::len),
            Some(0),
            "a recorded-but-empty domain must not read back as unrecorded"
        );
    }

    fn kind_of(col_type: &str) -> Option<ValueKind> {
        ColumnSchema {
            name: "c".into(),
            col_type: col_type.into(),
            nullable: false,
        }
        .declared_kind()
    }

    #[test]
    fn declared_kind_maps_common_col_types_case_insensitively_per_word() {
        assert_eq!(kind_of("INTEGER"), Some(ValueKind::Integer));
        assert_eq!(kind_of("int4"), Some(ValueKind::Integer));
        assert_eq!(kind_of("BIGINT"), Some(ValueKind::Integer));
        assert_eq!(kind_of("INT UNSIGNED"), Some(ValueKind::Integer));
        assert_eq!(kind_of("TEXT"), Some(ValueKind::Text));
        assert_eq!(kind_of("VARCHAR"), Some(ValueKind::Text));
        assert_eq!(kind_of("VARCHAR(45)"), Some(ValueKind::Text));
        assert_eq!(kind_of("character varying"), Some(ValueKind::Text));
        assert_eq!(kind_of("timestamp"), Some(ValueKind::Timestamp));
        assert_eq!(kind_of("TIMESTAMPTZ"), Some(ValueKind::Timestamp));
        assert_eq!(
            kind_of("TIMESTAMP(3) WITHOUT TIME ZONE"),
            Some(ValueKind::Timestamp)
        );
        assert_eq!(kind_of("SMALLDATETIME"), Some(ValueKind::Timestamp));
        assert_eq!(kind_of("DOUBLE"), Some(ValueKind::Float));
        assert_eq!(kind_of("DOUBLE PRECISION"), Some(ValueKind::Float));
        assert_eq!(kind_of("REAL"), Some(ValueKind::Float));
        assert_eq!(kind_of("NUMERIC(10,2)"), Some(ValueKind::Float));
        assert_eq!(kind_of("BOOLEAN"), Some(ValueKind::Boolean));
        assert_eq!(kind_of("bool"), Some(ValueKind::Boolean));
    }

    #[test]
    fn a_type_that_merely_contains_a_kind_word_declares_nothing() {
        for col_type in [
            "interval",
            "point",
            "int4range",
            "daterange",
            "numrange",
            "GEOMETRY",
            "tsvector",
            UNTYPED_COL_TYPE,
        ] {
            assert_eq!(kind_of(col_type), None, "{col_type} must declare no kind");
        }
    }

    #[test]
    fn column_values_are_distinct_in_first_seen_order_and_respect_the_limit() {
        let preview = TablePreview {
            columns: vec!["state".to_string()],
            rows: vec![
                vec![Some("draft".to_string())],
                vec![None],
                vec![Some("sale".to_string())],
                vec![Some("draft".to_string())],
                vec![Some("done".to_string())],
            ],
        };
        assert_eq!(
            preview.column_values("state", 10),
            ["draft", "sale", "done"]
        );
        assert_eq!(preview.column_values("state", 2), ["draft", "sale"]);
        assert!(preview.column_values("state", 0).is_empty());
        assert!(preview.column_values("missing", 10).is_empty());
    }

    #[test]
    fn round_trips_through_json() {
        let c = catalog();
        let json = serde_json::to_string(&c).expect("serialize");
        let back: ExtractionCatalog = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            back.table("erp", "orders").map(|t| t.columns.len()),
            Some(2)
        );
        assert_eq!(
            back.column_domain("erp", "orders", "state")
                .map(BTreeSet::len),
            Some(2)
        );
    }
}
