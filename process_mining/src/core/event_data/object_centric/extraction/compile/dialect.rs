//! The one place a SQL string is shaped.
//!
//! Every fragment the emitter produces goes through [`SqlDialect`], so adding a second engine is
//! a matter of adding match arms here rather than hunting `format!` calls through the compiler.
//! Nothing outside this module writes a quote, a cast or a function name by hand.

use chrono::{DateTime, FixedOffset};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::core::event_data::object_centric::extraction::value::{Value, ValueKind};
use crate::core::event_data::object_centric::OCELAttributeType;

/// Which SQL engine the emitted statements target.
///
/// The two are not equally evidenced: `DuckDb` is checked row-for-row against the extractor by
/// the differential suite, while `Postgres` is only covered by unit tests over the emitted text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[non_exhaustive]
pub enum SqlDialect {
    /// `DuckDB`, the engine the differential tests run against.
    #[default]
    DuckDb,
    /// `PostgreSQL` 12+. Emitted from the same fragments as `DuckDb`, see the type-level note.
    Postgres,
}

impl SqlDialect {
    /// Quote an identifier, doubling any embedded quote character.
    #[must_use]
    pub fn quote_ident(self, name: &str) -> String {
        match self {
            // SQL-standard double quoting in both.
            SqlDialect::DuckDb | SqlDialect::Postgres => {
                format!("\"{}\"", name.replace('"', "\"\""))
            }
        }
    }

    /// Quote a text value as a SQL string literal, doubling any embedded apostrophe.
    #[must_use]
    pub fn string_literal(self, value: &str) -> String {
        match self {
            SqlDialect::DuckDb | SqlDialect::Postgres => format!("'{}'", value.replace('\'', "''")),
        }
    }

    /// `CREATE VIEW <name> AS\n<body>`. `name` is always the bare view name, never pre-quoted.
    #[must_use]
    pub fn create_view(self, name: &str, body: &str) -> String {
        format!("CREATE VIEW {} AS\n{body}", self.quote_ident(name))
    }

    /// `CREATE TABLE <name> AS\n<body>`. See [`Self::create_view`].
    #[must_use]
    pub fn create_table_as(self, name: &str, body: &str) -> String {
        format!("CREATE TABLE {} AS\n{body}", self.quote_ident(name))
    }

    /// What terminates one statement in a multi-statement script.
    #[must_use]
    pub fn statement_separator(self) -> &'static str {
        ";"
    }

    /// A parenthesized subquery in `FROM` position, always aliased: `PostgreSQL` and `MySQL` both
    /// reject an unaliased derived table, and `DuckDB` merely tolerates it.
    pub(crate) fn derived_table(self, sql: &str, alias: &str) -> String {
        format!("(\n{sql}\n) AS {alias}")
    }

    /// An always-true predicate.
    pub(crate) fn true_predicate(self) -> &'static str {
        "TRUE"
    }

    /// An always-false predicate.
    pub(crate) fn false_predicate(self) -> &'static str {
        "FALSE"
    }

    /// Concatenate already-rendered `SELECT` branches into one `UNION ALL` chain.
    ///
    /// Never plain `UNION`: deduplicating here would drop rows the extractor keeps, and each
    /// relation applies its own `DISTINCT` where the extractor's own identity rules call for one.
    pub(crate) fn union_all(self, selects: &[String]) -> String {
        selects.join("\n  UNION ALL\n")
    }

    /// `SELECT DISTINCT <cols> FROM <from>`, where `from` is an already-rendered `FROM` target.
    pub(crate) fn distinct_select(self, cols: &[String], from: &str) -> String {
        format!("SELECT DISTINCT {} FROM {from}", cols.join(", "))
    }

    /// Concatenate already-rendered expression fragments with the dialect's text concatenation
    /// operator, parenthesized as one expression.
    ///
    /// `NULL`-propagating on purpose: a `Template` whose placeholder is `NULL` evaluates to
    /// `None` in the extractor, and `||` reproduces that without a per-part fallback.
    pub(crate) fn concat(self, parts: &[String]) -> String {
        match self {
            // `||` is standard and means text concatenation in both.
            SqlDialect::DuckDb | SqlDialect::Postgres => format!("({})", parts.join(" || ")),
        }
    }

    /// `COALESCE(<parts>)`.
    pub(crate) fn coalesce(self, parts: &[String]) -> String {
        format!("COALESCE({})", parts.join(", "))
    }

    /// Force a possibly-`NULL` boolean expression to `FALSE`.
    ///
    /// The extractor evaluates predicates in two-valued logic, so `NOT (col = 'x')` is true there
    /// when `col` is `NULL` while SQL makes it `NULL`. Every compiled predicate goes through this
    /// so `NOT` composes over a total boolean.
    pub(crate) fn total_bool(self, expr: &str) -> String {
        format!("COALESCE({expr}, {})", self.false_predicate())
    }

    /// `CAST(<expr> AS <text type>)`.
    pub(crate) fn cast_to_text(self, expr: &str) -> String {
        match self {
            SqlDialect::DuckDb => format!("CAST({expr} AS VARCHAR)"),
            SqlDialect::Postgres => format!("CAST({expr} AS TEXT)"),
        }
    }

    /// `CAST(NULL AS <text type>)`, for a placeholder column in an empty relation.
    pub(crate) fn null_text(self) -> String {
        self.cast_to_text("NULL")
    }

    /// `CAST(NULL AS <timestamp type>)`. See [`Self::null_text`].
    pub(crate) fn null_timestamp(self) -> String {
        match self {
            SqlDialect::DuckDb | SqlDialect::Postgres => "CAST(NULL AS TIMESTAMPTZ)".to_string(),
        }
    }

    /// A boolean column at an identity position, rendered exactly as
    /// [`Value::canonical_string`] renders it, rather than trusting the engine's own
    /// boolean-to-text cast.
    ///
    /// `NULL`-propagating: a `CASE WHEN {expr} THEN .. ELSE .. END` would take the `ELSE` branch
    /// for `NULL` and mint the literal id `false`, past the caller's `IS NOT NULL` and `<> ''`
    /// guards. The `CASE <expr> WHEN ..` form also names `expr` only once.
    pub(crate) fn bool_to_text(self, expr: &str) -> String {
        format!("CASE {expr} WHEN TRUE THEN 'true' WHEN FALSE THEN 'false' END")
    }

    /// The column type an event/object attribute of `t` is stored as.
    pub(crate) fn attribute_sql_type(self, t: OCELAttributeType) -> &'static str {
        match self {
            SqlDialect::DuckDb => match t {
                OCELAttributeType::Integer => "BIGINT",
                OCELAttributeType::Float => "DOUBLE",
                OCELAttributeType::Boolean => "BOOLEAN",
                OCELAttributeType::Time => "TIMESTAMPTZ",
                OCELAttributeType::String | OCELAttributeType::Null => "VARCHAR",
            },
            SqlDialect::Postgres => match t {
                OCELAttributeType::Integer => "BIGINT",
                OCELAttributeType::Float => "DOUBLE PRECISION",
                OCELAttributeType::Boolean => "BOOLEAN",
                OCELAttributeType::Time => "TIMESTAMPTZ",
                OCELAttributeType::String | OCELAttributeType::Null => "TEXT",
            },
        }
    }

    /// `CAST(NULL AS <attribute type>)`, so a `UNION ALL` branch that does not carry an
    /// attribute still contributes a column of the right type.
    pub(crate) fn null_attribute(self, t: OCELAttributeType) -> String {
        format!("CAST(NULL AS {})", self.attribute_sql_type(t))
    }

    /// An instant literal, always with an explicit UTC offset so no session time zone can
    /// reinterpret it.
    pub(crate) fn timestamp_literal(self, ts: &DateTime<FixedOffset>) -> String {
        match self {
            SqlDialect::DuckDb | SqlDialect::Postgres => {
                format!("CAST('{}' AS TIMESTAMPTZ)", ts.to_utc().to_rfc3339())
            }
        }
    }

    /// The Unix epoch, the instant a static object attribute is stamped with.
    pub(crate) fn epoch_timestamp(self) -> String {
        match self {
            SqlDialect::DuckDb | SqlDialect::Postgres => {
                "CAST('1970-01-01T00:00:00+00:00' AS TIMESTAMPTZ)".to_string()
            }
        }
    }

    /// Read a timestamp column as an absolute instant.
    ///
    /// `naive` says the column has no offset of its own (`TIMESTAMP`, `DATE`, `DATETIME`), in which
    /// case it is anchored to UTC explicitly: the extractor's providers read such a column at UTC,
    /// and a session-dependent reading would silently shift it.
    pub(crate) fn timestamp_column(self, expr: &str, naive: bool) -> String {
        match self {
            // `timezone(zone, timestamp) -> timestamptz` is spelled and typed the same in both.
            SqlDialect::DuckDb | SqlDialect::Postgres => {
                if naive {
                    format!("timezone('UTC', CAST({expr} AS TIMESTAMP))")
                } else {
                    format!("CAST({expr} AS TIMESTAMPTZ)")
                }
            }
        }
    }

    /// A `TIMESTAMPTZ` expression rendered as RFC 3339 text (UTC, `Z` suffix), independent of the
    /// session time zone.
    ///
    /// `strftime` alone formats in the session's local time zone, so `timezone('UTC', ..)` converts
    /// to a naive `TIMESTAMP` holding the UTC reading first. The microsecond field is always
    /// emitted so the text round-trips exactly through
    /// [`parse_timestamp`](crate::core::event_data::timestamp_utils::parse_timestamp).
    pub(crate) fn timestamptz_to_iso_text(self, expr: &str) -> String {
        match self {
            SqlDialect::DuckDb => {
                format!("strftime(timezone('UTC', {expr}), '%Y-%m-%dT%H:%M:%S.%fZ')")
            }
            // `timezone('UTC', timestamptz)` yields the naive UTC reading, which `to_char` renders
            // verbatim; `US` is microseconds, six digits zero-padded, matching `%f`.
            SqlDialect::Postgres => {
                format!("to_char(timezone('UTC', {expr}), 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
            }
        }
    }

    /// Strip surrounding whitespace. See the dialect note in the [module docs](super).
    pub(crate) fn trim(self, expr: &str) -> String {
        format!("trim({expr})")
    }

    /// One row per delimiter-separated part of `expr`.
    pub(crate) fn split_to_rows(self, expr: &str, delimiter: &str) -> String {
        match self {
            SqlDialect::DuckDb => format!(
                "unnest(string_split({expr}, {}))",
                self.string_literal(delimiter)
            ),
            SqlDialect::Postgres => format!(
                "unnest(string_to_array({expr}, {}))",
                self.string_literal(delimiter)
            ),
        }
    }

    /// One row per value a regular expression extracts from `expr`: every capture group of every
    /// match when `groups` is non-zero, each whole match otherwise.
    ///
    /// Mirrors [`PreparedSplit::split`](crate::core::event_data::object_centric::extraction::expr::SplitSpec)'s
    /// regex branch. The emission order differs from Rust's, which does not matter: every
    /// relation this feeds is `DISTINCT` and compared as a set.
    pub(crate) fn regex_split_to_rows(self, expr: &str, pattern: &str, groups: usize) -> String {
        match self {
            SqlDialect::DuckDb => {
                let pat = self.string_literal(pattern);
                if groups == 0 {
                    return format!("unnest(regexp_extract_all({expr}, {pat}))");
                }
                // `list_concat` takes exactly two lists, so several groups fold into nested calls.
                let group_list = |i: usize| format!("regexp_extract_all({expr}, {pat}, {i})");
                let mut list = group_list(groups);
                for i in (1..groups).rev() {
                    list = format!("list_concat({}, {list})", group_list(i));
                }
                format!("unnest({list})")
            }
            // `regexp_matches(.., 'g')` yields one `text[]` per match: the whole match without
            // groups, the capture groups otherwise, matching what DuckDB's branch produces.
            // The `ARRAY(..)` keeps the expression set-returning: a bare `(SELECT .. FROM ..)` in
            // a `SELECT` list is a scalar subquery and errors on more than one match.
            SqlDialect::Postgres => {
                let pat = self.string_literal(pattern);
                let m = format!("regexp_matches({expr}, {pat}, 'g')");
                if groups == 0 {
                    format!("unnest(ARRAY(SELECT g[1] FROM {m} AS g))")
                } else {
                    format!("unnest(ARRAY(SELECT unnest(g) FROM {m} AS g))")
                }
            }
        }
    }

    /// Whether a text expression matches a regular expression, unanchored.
    pub(crate) fn regex_match(self, expr: &str, pattern: &str) -> String {
        match self {
            SqlDialect::DuckDb => {
                format!("regexp_matches({expr}, {})", self.string_literal(pattern))
            }
            // `~`, not `regexp_matches`: in PostgreSQL that name is the set-returning function
            // above, not a predicate.
            SqlDialect::Postgres => format!("({expr} ~ {})", self.string_literal(pattern)),
        }
    }

    /// A [`Value`] as a typed SQL literal. `None` for [`Value::Null`], which has no literal
    /// form the compiler ever needs.
    pub(crate) fn value_literal(self, v: &Value) -> Option<String> {
        match v {
            Value::Null => None,
            Value::Text(s) => Some(self.string_literal(s)),
            Value::Integer(i) => Some(format!("CAST({i} AS BIGINT)")),
            Value::Float(f) => Some({
                let ty = match self {
                    SqlDialect::DuckDb => "DOUBLE",
                    SqlDialect::Postgres => "DOUBLE PRECISION",
                };
                if f.is_nan() {
                    format!("CAST('NaN' AS {ty})")
                } else if f.is_infinite() {
                    let sign = if *f < 0.0 { "-" } else { "" };
                    format!("CAST('{sign}Infinity' AS {ty})")
                } else {
                    // `{f:?}` is the shortest representation that round-trips.
                    format!("CAST('{f:?}' AS {ty})")
                }
            }),
            Value::Boolean(b) => Some(if *b { "TRUE" } else { "FALSE" }.to_string()),
            Value::Timestamp(ts) => Some(self.timestamp_literal(ts)),
        }
    }

    /// The SQL type a column of `kind` is read as, used to give an otherwise untyped `NULL` a
    /// type in a `UNION ALL` branch.
    pub(crate) fn kind_sql_type(self, kind: ValueKind) -> &'static str {
        match self {
            SqlDialect::DuckDb => match kind {
                ValueKind::Text => "VARCHAR",
                ValueKind::Integer => "BIGINT",
                ValueKind::Float => "DOUBLE",
                ValueKind::Boolean => "BOOLEAN",
                ValueKind::Timestamp => "TIMESTAMPTZ",
            },
            SqlDialect::Postgres => match kind {
                ValueKind::Text => "TEXT",
                ValueKind::Integer => "BIGINT",
                ValueKind::Float => "DOUBLE PRECISION",
                ValueKind::Boolean => "BOOLEAN",
                ValueKind::Timestamp => "TIMESTAMPTZ",
            },
        }
    }
}
