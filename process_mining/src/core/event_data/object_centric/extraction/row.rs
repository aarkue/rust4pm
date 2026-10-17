//! Column-indexed access to a row of [`Value`]s.
//!
//! Internal to the module: rows are a processing detail, not part of the blueprint's shape.

use std::collections::HashMap;

use super::value::Value;

/// Maps a column name to its position in a row.
pub(crate) type ColumnIndex<'a> = HashMap<&'a str, usize>;

/// One row, addressable by column name.
#[derive(Debug)]
pub(crate) struct Row<'a> {
    /// Cell values, positionally aligned with `index`.
    pub(crate) values: &'a [Value],
    /// Column name to position.
    pub(crate) index: &'a ColumnIndex<'a>,
}

impl<'a> Row<'a> {
    /// The value in `column`, or `None` if the row has no such column.
    pub(crate) fn get(&self, column: &str) -> Option<&'a Value> {
        self.index.get(column).and_then(|&i| self.values.get(i))
    }
}

/// Build a [`ColumnIndex`] from an ordered column list.
pub(crate) fn build_column_index<'a>(columns: &[&'a str]) -> ColumnIndex<'a> {
    columns
        .iter()
        .enumerate()
        .map(|(i, &name)| (name, i))
        .collect()
}

/// Build a [`Row`] from name/value pairs and hand it to `f`.
///
/// Test-only: a `Row` borrows its values and index, so tests would otherwise repeat three
/// lines of setup each.
#[cfg(test)]
pub(crate) fn with_row<R>(pairs: &[(&str, Value)], f: impl FnOnce(&Row<'_>) -> R) -> R {
    let names: Vec<&str> = pairs.iter().map(|(n, _)| *n).collect();
    let values: Vec<Value> = pairs.iter().map(|(_, v)| v.clone()).collect();
    let index = build_column_index(&names);
    f(&Row {
        values: &values,
        index: &index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_the_value_and_none_for_unknown_columns() {
        with_row(
            &[("a", Value::Integer(1)), ("b", Value::Text("x".into()))],
            |row| {
                assert_eq!(row.get("a"), Some(&Value::Integer(1)));
                assert_eq!(row.get("b"), Some(&Value::Text("x".into())));
                assert_eq!(row.get("missing"), None);
            },
        );
    }
}
