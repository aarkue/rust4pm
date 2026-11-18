use crate::core::event_data::object_centric::io::OCELImportOptions;
use crate::core::event_data::object_centric::ocel_struct::{OCELAttributeValue, OCEL};

use super::super::import::{import_ocel, resolve_columns, Col, SqlRow, SqlSource};
use super::super::*;
use super::schema::value::{duck_value_to_datetime, duck_value_to_ocel};
use crate::utils::control::Control;
use ::duckdb::types::Value as DuckValue;
use ::duckdb::{Connection, Row, Statement};
use chrono::{DateTime, FixedOffset};

impl SqlSource for Connection {
    type Row<'r> = Row<'r>;

    fn db_path(&self) -> Option<&str> {
        self.path().and_then(|p| p.to_str())
    }

    fn has_table(&self, name: &str) -> Result<bool, OCELIOError> {
        Ok(self.query_row(
            "SELECT count(*) > 0 FROM information_schema.tables \
             WHERE table_catalog = current_database() AND table_schema = current_schema() \
             AND table_name = ?",
            [name],
            |r| r.get(0),
        )?)
    }

    /// The name is bound as a value because `PRAGMA table_info` parses its argument as a
    /// qualified name, which fails for a type `v1.2`.
    fn table_columns(&self, table: &str) -> Result<Vec<(String, String)>, OCELIOError> {
        // `lower` on both sides, as `DuckDB` resolves a table name case-insensitively.
        let mut s = self.prepare(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_catalog = current_database() AND table_schema = current_schema() \
             AND lower(table_name) = lower(?) ORDER BY ordinal_position",
        )?;
        let columns = s
            .query_map([table], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        Ok(columns)
    }

    /// A column read as a string attribute but not of a text type (a `UUID`, a `LIST`, ...) is
    /// selected as its `VARCHAR` text, so it is kept as the text `DuckDB` gives it.
    fn select_list(columns: &[(String, String)]) -> String {
        columns
            .iter()
            .map(|(name, ty)| {
                let q = quoted_ident(name);
                if sql_type_to_ocel(ty) == OCELAttributeType::String {
                    format!("CAST({q} AS VARCHAR) AS {q}")
                } else {
                    q
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn as_text(column: &str) -> String {
        format!("CAST({column} AS VARCHAR)")
    }

    fn for_each_row<'w>(
        &self,
        sql: &str,
        wanted: &[&'w str],
        mut each: impl FnMut(&Row<'_>, &[Col<'w>]) -> Result<(), OCELIOError>,
    ) -> Result<(), OCELIOError> {
        let mut s = self.prepare(sql)?;
        let mut rows = s.query([])?;
        // `DuckDB` knows the result's columns only once the statement ran.
        let names = rows
            .as_ref()
            .map(Statement::column_names)
            .unwrap_or_default();
        let cols = resolve_columns(&names, wanted);
        while let Some(r) = rows.next()? {
            each(r, &cols)?;
        }
        Ok(())
    }

    fn query_count(&self, sql: &str) -> Result<i64, OCELIOError> {
        Ok(self.query_row(sql, [], |r| r.get(0))?)
    }
}

impl SqlRow for Row<'_> {
    fn text(&self, col: Col<'_>) -> Result<String, OCELIOError> {
        Ok(self.get(col.index()?)?)
    }

    fn opt_text(&self, col: Col<'_>) -> Result<Option<String>, OCELIOError> {
        Ok(self.get(col.index()?)?)
    }

    fn time(&self, col: Col<'_>) -> Result<DateTime<FixedOffset>, OCELIOError> {
        let value = self.get::<_, DuckValue>(col.index()?)?;
        duck_value_to_datetime(&value).ok_or_else(|| col.invalid_time(&value))
    }

    /// `DuckDB` columns are typed, so the cell's value decides, whatever the declared type.
    fn attribute(
        &self,
        col: Col<'_>,
        _: OCELAttributeType,
        _: &str,
    ) -> Result<Option<OCELAttributeValue>, OCELIOError> {
        Ok(match self.get::<_, DuckValue>(col.index()?)? {
            DuckValue::Null => None,
            v => Some(duck_value_to_ocel(v)),
        })
    }
}

/// Import an [`OCEL`] from the `DuckDB` database `con`, in either layout: the OCEL 2.0 per-type
/// tables, or the consolidated schema [`stream_ocel_file_to_duckdb`] writes. `options` apply to
/// the per-type tables.
pub fn import_ocel_duckdb(
    con: &Connection,
    options: &OCELImportOptions,
) -> Result<OCEL, OCELIOError> {
    import_any_layout(con, options, &Control::new())
}

fn import_any_layout(
    con: &Connection,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<OCEL, OCELIOError> {
    if super::schema::reader::is_consolidated_schema(con)? {
        return super::schema::reader::read_ocel_from_duckdb(con);
    }
    import_ocel(con, options, ctl)
}

/// Read either `DuckDB` layout from `path`: the OCEL 2.0 per-type tables, reporting to `ctl`,
/// or the consolidated schema.
pub(crate) fn import_duckdb_path(
    path: &std::path::Path,
    options: &OCELImportOptions,
    ctl: &Control<'_>,
) -> Result<OCEL, OCELIOError> {
    import_any_layout(&open_read_only(path)?, options, ctl)
}

/// Open an existing `DuckDB` file read-only. [`Connection::open`] creates a missing file, and
/// a writable connection may write to the file (e.g., replay its WAL) while reading.
pub(crate) fn open_read_only<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<Connection, ::duckdb::Error> {
    let config = ::duckdb::Config::default().access_mode(::duckdb::AccessMode::ReadOnly)?;
    Connection::open_with_flags(path, config)
}

#[cfg(test)]
mod missing_changed_field_tests {
    use ::duckdb::Connection;
    use chrono::DateTime;

    use crate::core::event_data::object_centric::{
        io::OCELImportOptions,
        ocel_sql::import_ocel_duckdb,
        ocel_struct::{OCELAttributeValue, OCELObjectAttribute},
    };

    /// A non-conforming `DuckDB` file: object type `Truck` has no `ocel_changed_field` column,
    /// but does carry a misspelled `ocel_change_field` one whose value names a real attribute.
    fn build_fixture(path: &std::path::Path) {
        let con = Connection::open(path).unwrap();
        con.execute_batch(
            "
            CREATE TABLE event_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE object_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE event_Ev (ocel_id TEXT PRIMARY KEY, ocel_time TIMESTAMP);
            CREATE TABLE object_Truck (
                ocel_id TEXT PRIMARY KEY,
                ocel_time TIMESTAMP,
                driver TEXT,
                ocel_change_field TEXT
            );
            INSERT INTO object_map_type VALUES ('Truck', 'Truck');
            INSERT INTO event_map_type VALUES ('Ev', 'Ev');
            INSERT INTO event_Ev VALUES ('e1', '2020-01-01T00:00:00+00:00');
            INSERT INTO object_Truck VALUES ('t1', '2020-01-01T00:00:00+00:00', 'Alice', NULL);
            INSERT INTO object_Truck VALUES ('t2', '2020-01-02T00:00:00+00:00', 'Bob', 'driver');
            ",
        )
        .unwrap();
    }

    /// The column is only missing where there is no attribute change to record, so a file
    /// without it is read rather than refused.
    #[test]
    fn default_import_reads_a_file_without_the_changed_field_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.duckdb");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let ocel = import_ocel_duckdb(&con, &Default::default())
            .expect("a file without the column must still be read");
        assert_eq!(ocel.objects.len(), 2, "both Truck objects must be present");
    }

    /// Holding the file to the specification is opt-in, and names what is wrong with it.
    #[test]
    fn strict_import_rejects_missing_changed_field_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.duckdb");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let err = import_ocel_duckdb(
            &con,
            &OCELImportOptions {
                strict: true,
                ..Default::default()
            },
        )
        .unwrap_err();
        let msg = err.to_string();

        assert!(
            msg.contains("Truck"),
            "message should name the object type: {msg}"
        );
        assert!(
            msg.contains("object_Truck"),
            "message should name the table: {msg}"
        );
        assert!(
            msg.contains("ocel_changed_field"),
            "message should name the missing column: {msg}"
        );
        assert!(
            msg.contains("does not conform"),
            "message should say the file is non-conforming: {msg}"
        );
        assert!(
            msg.contains("ocel_change_field"),
            "message should mention the misspelled column it found: {msg}"
        );
    }

    #[test]
    fn a_file_without_the_changed_field_column_reads_as_initial_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("no-changed-field.duckdb");
        build_fixture(&path);

        let con = Connection::open(&path).unwrap();
        let ocel = import_ocel_duckdb(
            &con,
            &OCELImportOptions {
                strict: false,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(ocel.objects.len(), 2, "both Truck objects must be present");

        let t1 = ocel.objects.iter().find(|o| o.id == "t1").unwrap();
        assert_eq!(t1.object_type, "Truck");
        assert_eq!(
            t1.attributes,
            vec![OCELObjectAttribute {
                name: "driver".to_string(),
                value: OCELAttributeValue::String("Alice".to_string()),
                time: DateTime::UNIX_EPOCH.into(),
            }],
            "t1's ocel_change_field cell is NULL, so it contributes no attribute; \
             it must not be invented as a change record"
        );

        let t2 = ocel.objects.iter().find(|o| o.id == "t2").unwrap();
        assert_eq!(t2.object_type, "Truck");
        let mut t2_attrs = t2.attributes.clone();
        t2_attrs.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            t2_attrs,
            vec![
                OCELObjectAttribute {
                    name: "driver".to_string(),
                    value: OCELAttributeValue::String("Bob".to_string()),
                    time: DateTime::UNIX_EPOCH.into(),
                },
                OCELObjectAttribute {
                    name: "ocel_change_field".to_string(),
                    value: OCELAttributeValue::String("driver".to_string()),
                    time: DateTime::UNIX_EPOCH.into(),
                },
            ],
            "no attribute-change row must be invented from the misspelled column"
        );

        let truck_type = ocel
            .object_types
            .iter()
            .find(|t| t.name == "Truck")
            .unwrap();
        let mut attr_names: Vec<_> = truck_type
            .attributes
            .iter()
            .map(|a| a.name.clone())
            .collect();
        attr_names.sort();
        assert_eq!(attr_names, vec!["driver", "ocel_change_field"]);
    }
}

#[cfg(test)]
mod object_table_tests {
    use ::duckdb::Connection;

    use crate::core::event_data::object_centric::ocel_sql::{
        import_ocel_duckdb, object_table_fixture,
    };

    #[test]
    fn objects_listed_only_in_the_object_table_are_imported() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch(object_table_fixture::SQL).unwrap();
        let ocel = import_ocel_duckdb(&con, &Default::default()).unwrap();
        object_table_fixture::check(&ocel);
    }
}

#[cfg(test)]
mod column_type_tests {
    use ::duckdb::Connection;

    use crate::core::event_data::object_centric::{
        ocel_sql::import_ocel_duckdb, ocel_struct::OCELAttributeValue as V,
    };

    /// Column types other tools write, which the importer must read rather than fail on.
    #[test]
    fn columns_of_any_type_are_read() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch(
            "
            CREATE TABLE event_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE object_map_type (ocel_type_map TEXT, ocel_type TEXT);
            CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT, ocel_qualifier TEXT);
            CREATE TABLE event_Ev (
                ocel_id TEXT, ocel_time TIMESTAMPTZ,
                big BIGINT, huge HUGEINT, dec DECIMAL(10, 2), r REAL,
                tz TIMESTAMPTZ, d DATE, u UUID, l INTEGER[]
            );
            INSERT INTO event_map_type VALUES ('Ev', 'Ev');
            INSERT INTO event_Ev VALUES (
                'e1', '2020-01-01 00:00:00+00',
                3000000000, 170141183460469231731687303715884105727, 12.5, 0.5,
                '2020-01-02 03:04:05+00', '2020-01-03',
                '00000000-0000-0000-0000-000000000001', [1, 2]
            );
            ",
        )
        .unwrap();
        let ocel = import_ocel_duckdb(&con, &Default::default()).unwrap();
        let mut types: Vec<_> = ocel.event_types[0]
            .attributes
            .iter()
            .map(|a| (a.name.as_str(), a.value_type.as_str()))
            .collect();
        types.sort_unstable();
        assert_eq!(
            types,
            [
                ("big", "integer"),
                ("d", "time"),
                ("dec", "float"),
                ("huge", "integer"),
                ("l", "string"),
                ("r", "float"),
                ("tz", "time"),
                ("u", "string"),
            ]
        );
        let value = |name: &str| {
            ocel.events[0]
                .attributes
                .iter()
                .find(|a| a.name == name)
                .map(|a| a.value.clone())
                .unwrap()
        };
        let time = |s| V::Time(chrono::DateTime::parse_from_rfc3339(s).unwrap());
        assert_eq!(value("big"), V::Integer(3_000_000_000));
        assert_eq!(
            value("huge"),
            V::String("170141183460469231731687303715884105727".into())
        );
        assert_eq!(value("dec"), V::Float(12.5));
        assert_eq!(value("r"), V::Float(0.5));
        assert_eq!(value("tz"), time("2020-01-02T03:04:05+00:00"));
        assert_eq!(value("d"), time("2020-01-03T00:00:00+00:00"));
        assert_eq!(
            value("u"),
            V::String("00000000-0000-0000-0000-000000000001".into())
        );
        assert_eq!(value("l"), V::String("[1, 2]".into()));
        assert_eq!(
            V::Time(ocel.events[0].time),
            time("2020-01-01T00:00:00+00:00")
        );
    }
}
