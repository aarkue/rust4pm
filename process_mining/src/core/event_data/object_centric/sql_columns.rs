//! Attribute column names for the SQL schemas OCEL is written to, shared by the SQL writers and
//! the extraction SQL compiler, which builds without either database.
#[cfg(any(feature = "ocel-duckdb", feature = "extraction-blueprint"))]
use std::borrow::Cow;
use std::collections::HashSet;

/// The fixed columns of an OCEL 2.0 SQL `event_<type>`/`object_<type>` table. Importers skip all
/// three on either kind of table, so no attribute column may reuse one.
pub(crate) const OCEL_SQL_BASE_COLS: [&str; 3] = ["ocel_id", "ocel_time", "ocel_changed_field"];

/// `(table_name, attr_name, column_name)` for each attribute of an OCEL 2.0 SQL type table whose
/// column is not named like the attribute. Absent when there is none.
pub(crate) const T_OCEL_ATTR_COLUMNS: &str = "ocel_attr_columns";

/// `(attr_name, column_name)` for each event attribute whose `events` column is not the one
/// [`event_attr_column`] gives it. Absent when there is none.
#[cfg(any(feature = "ocel-duckdb", feature = "extraction-blueprint"))]
pub(crate) const T_EVENT_ATTR_COLUMNS: &str = "event_attr_columns";

/// The fixed columns of the wide `events` table, which no attribute column may reuse.
#[cfg(any(feature = "ocel-duckdb", feature = "extraction-blueprint"))]
pub(crate) const EVENT_BASE_COLS: [&str; 3] = ["id", "ocel_type", "time"];

/// Column names for one table (or table names for one schema), unique as `DuckDB` and `SQLite`
/// compare them: case-insensitively.
#[derive(Debug, Clone)]
pub(crate) struct ColumnNames {
    taken: HashSet<String>,
}

impl ColumnNames {
    /// Names for a table whose fixed columns are `base_cols`.
    pub(crate) fn new(base_cols: &[&str]) -> Self {
        Self {
            taken: base_cols.iter().map(|c| c.to_lowercase()).collect(),
        }
    }

    /// `column`, or `column_<n>` with the smallest `n >= 2` that no column claimed before has.
    pub(crate) fn claim(&mut self, column: &str) -> String {
        let mut claimed = column.to_string();
        let mut n = 1;
        while !self.taken.insert(claimed.to_lowercase()) {
            n += 1;
            claimed = format!("{column}_{n}");
        }
        claimed
    }

    /// Claim the `events` column of attribute `name`. `Some` if it is not
    /// [`event_attr_column`]'s, and so must be recorded in [`T_EVENT_ATTR_COLUMNS`].
    #[cfg(any(feature = "ocel-duckdb", feature = "extraction-blueprint"))]
    pub(crate) fn claim_event_column(&mut self, name: &str) -> (String, Option<(String, String)>) {
        let column = event_attr_column(name);
        let claimed = self.claim(&column);
        let renamed = (claimed != column).then(|| (name.to_string(), claimed.clone()));
        (claimed, renamed)
    }
}

/// The column of each attribute in `names` on one OCEL 2.0 SQL type table, in order. A column
/// that differs from its attribute's name must be recorded in [`T_OCEL_ATTR_COLUMNS`].
pub(crate) fn ocel_sql_attr_columns<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut columns = ColumnNames::new(&OCEL_SQL_BASE_COLS);
    names.into_iter().map(|n| columns.claim(n)).collect()
}

/// Whether `name` clashes with a `base_cols` entry, directly or as an already-suffixed form of
/// one, which keeps the `_attr` suffixing reversible. Case-insensitive, like `DuckDB`
/// identifiers: an attribute `Time` cannot have a column next to `time` either.
#[cfg(any(feature = "ocel-duckdb", feature = "extraction-blueprint"))]
pub(crate) fn collides_with_base_col(name: &str, base_cols: &[&str]) -> bool {
    let name = name.trim_end_matches("_attr");
    base_cols.iter().any(|b| b.eq_ignore_ascii_case(name))
}

/// Suffix `name` with `_attr` if it collides with `base_cols`. Because the collision check
/// strips all trailing `_attr` repeats before comparing, this single pass is already
/// collision-free: e.g. `id` and `id_attr` map to `id_attr` and `id_attr_attr` respectively.
#[cfg(any(feature = "ocel-duckdb", feature = "extraction-blueprint"))]
pub(crate) fn suffixed_column<'a>(name: &'a str, base_cols: &[&str]) -> Cow<'a, str> {
    if collides_with_base_col(name, base_cols) {
        Cow::Owned(format!("{name}_attr"))
    } else {
        Cow::Borrowed(name)
    }
}

/// The `events` column an event attribute is stored in. `id`/`ocel_type`/`time` belong to the
/// table itself, so an attribute using one of those names takes an `_attr` suffix instead.
#[cfg(any(feature = "ocel-duckdb", feature = "extraction-blueprint"))]
pub(crate) fn event_attr_column(name: &str) -> Cow<'_, str> {
    suffixed_column(name, &EVENT_BASE_COLS)
}
