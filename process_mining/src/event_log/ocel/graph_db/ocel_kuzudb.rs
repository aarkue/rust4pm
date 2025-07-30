use std::{fs::File, path::Path};

use polars::{error::PolarsResult, frame::DataFrame, io::SerWriter, prelude::CsvWriter};

use crate::OCEL;

///
/// Error encountered while parsing XES
///
#[derive(Debug)]
pub enum KuzuDBExportError {
    /// Error orignating in kuzu
    KuzuDBError(kuzu::Error),
    /// General IO Error (e.g., when creating the database file)
    IOError(std::io::Error),
    #[cfg(feature = "dataframes")]
    /// Error originiating in Polars (for `DataFrame` conversion used as an intermediate step)
    PolarsError(polars::prelude::PolarsError),
}

impl std::fmt::Display for KuzuDBExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to export to kuzudb: {self:?}")
    }
}

impl std::error::Error for KuzuDBExportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            KuzuDBExportError::KuzuDBError(e) => Some(e),
            KuzuDBExportError::IOError(e) => Some(e),
            #[cfg(feature = "dataframes")]
            KuzuDBExportError::PolarsError(e) => Some(e),
        }
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl From<std::io::Error> for KuzuDBExportError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e)
    }
}

impl From<kuzu::Error> for KuzuDBExportError {
    fn from(e: kuzu::Error) -> Self {
        Self::KuzuDBError(e)
    }
}

#[cfg(feature = "dataframes")]
impl From<polars::prelude::PolarsError> for KuzuDBExportError {
    fn from(e: polars::prelude::PolarsError) -> Self {
        Self::PolarsError(e)
    }
}

#[cfg(feature = "dataframes")]
/// Export an [`OCEL`] as a [kuzu](https://github.com/kuzudb/kuzu) database
///
/// This export function does not create different node types for different event/object types
///
/// Instead `Event` and `Object` nodes are added, and they both have an attribute `type`.
///
/// For E2O relationships, the `E2O` relation is used, pointing from events to objects, with an additional relationship qualifier.
///
/// **Limitations**: This function is work-in-progress, currently some aspects (O2O relationships, object attribute changes) are not recorded.
pub fn export_ocel_to_kuzudb_generic<P: AsRef<Path>>(
    db_path: P,
    ocel: &OCEL,
) -> Result<(), KuzuDBExportError> {
    use kuzu::{Connection, Database, SystemConfig};

    use crate::ocel::dataframe::{
        ocel_to_dataframes, OCEL_EVENT_ID_KEY, OCEL_EVENT_TIMESTAMP_KEY, OCEL_EVENT_TYPE_KEY,
        OCEL_OBJECT_ID_2_KEY, OCEL_OBJECT_ID_KEY, OCEL_OBJECT_TYPE_KEY, OCEL_QUALIFIER_KEY,
    };

    let db = Database::new(db_path, SystemConfig::default())?;
    let conn = Connection::new(&db)?;
    let tmp = tempfile::tempdir()?;
    let path = tmp.path();
    let mut df = ocel_to_dataframes(ocel);
    df.export_events_csv(
        path.join("events.csv"),
        &[
            OCEL_EVENT_ID_KEY,
            OCEL_EVENT_TYPE_KEY,
            OCEL_EVENT_TIMESTAMP_KEY,
        ],
    )?;
    df.export_objects_csv(
        path.join("objects.csv"),
        &[OCEL_OBJECT_ID_KEY, OCEL_OBJECT_TYPE_KEY],
    )?;
    df.export_e2o_csv(
        path.join("e2o.csv"),
        &[OCEL_EVENT_ID_KEY, OCEL_OBJECT_ID_KEY, OCEL_QUALIFIER_KEY],
    )?;
    df.export_o2o_csv(
        path.join("o2o.csv"),
        &[OCEL_OBJECT_ID_KEY, OCEL_OBJECT_ID_2_KEY, OCEL_QUALIFIER_KEY],
    )?;
    conn.query("CREATE NODE TABLE Event(id STRING PRIMARY KEY, type STRING, time TIMESTAMP);")?;
    conn.query("CREATE NODE TABLE Object(id STRING PRIMARY KEY, type STRING);")?;
    conn.query("CREATE REL TABLE E2O(FROM Event to Object, qualifier STRING);")?;
    conn.query("CREATE REL TABLE O2O(FROM Object to Object, qualifier STRING);")?;
    conn.query(&format!(
        "COPY Event FROM '{}' (header=true);",
        path.join("events.csv").to_string_lossy()
    ))?;
    conn.query(&format!(
        "COPY Object FROM '{}' (header=true);",
        path.join("objects.csv").to_string_lossy()
    ))?;
    conn.query(&format!(
        "COPY E2O FROM '{}' (header=true)",
        path.join("e2o.csv").to_string_lossy()
    ))?;
    conn.query(&format!(
        "COPY O2O FROM '{}' (header=true)",
        path.join("o2o.csv").to_string_lossy()
    ))?;
    Ok(())
}

fn export_df_to_csv<P: AsRef<Path>>(df: &mut DataFrame, export_path: P) -> PolarsResult<()> {
    let f = File::create(export_path)?;
    let mut csvw = CsvWriter::new(f);
    // let df = &mut self.objects;
    // if !columns_to_include.is_empty() {
    //     csvw.finish(&mut df.select(columns_to_include.iter().copied())?)?;
    // } else {
    csvw.finish(df)?;
    Ok(())
    // }
}
use polars::prelude::*;
/// WIP
#[cfg(feature = "dataframes")]
pub fn export_ocel_to_kuzudb_typed<P: AsRef<Path>>(
    db_path: P,
    ocel: &OCEL,
) -> Result<(), KuzuDBExportError> {
    use kuzu::{Connection, Database, SystemConfig};

    use crate::ocel::dataframe::{
        ocel_to_dataframes, OCEL_EVENT_ID_KEY, OCEL_EVENT_TIMESTAMP_KEY, OCEL_EVENT_TYPE_KEY,
        OCEL_OBJECT_ID_2_KEY, OCEL_OBJECT_ID_KEY, OCEL_OBJECT_TYPE_KEY, OCEL_QUALIFIER_KEY,
    };

    let db = Database::new(db_path, SystemConfig::default())?;
    let conn = Connection::new(&db)?;
    let tmp = tempfile::tempdir()?;
    let path = tmp.path();
    let mut df = ocel_to_dataframes(ocel);
    for ot in &ocel.object_types {
        let mut obs = df
            .objects
            .clone()
            .lazy()
            .filter(col(OCEL_OBJECT_TYPE_KEY).eq(lit(ot.name.as_str())))
            .collect()?;
        export_df_to_csv(&mut obs, path.join("export.csv"))?;
        let cols = obs.get_column_names();
        println!("{} got cols: {cols:?}", ot.name);
        // remove_file(path.join("export.csv"))
    }
    for et in &ocel.event_types {
        let mut evs = df
            .events
            .clone()
            .lazy()
            .filter(col(OCEL_EVENT_TYPE_KEY).eq(lit(et.name.as_str())))
            .collect()?;
        export_df_to_csv(&mut evs, path.join("export.csv"))?;
        let cols = evs.get_column_names();
        println!("{} got cols: {cols:?}", et.name);
    }
    df.export_events_csv(
        path.join("events.csv"),
        &[
            OCEL_EVENT_ID_KEY,
            OCEL_EVENT_TYPE_KEY,
            OCEL_EVENT_TIMESTAMP_KEY,
        ],
    )?;
    df.export_objects_csv(
        path.join("objects.csv"),
        &[OCEL_OBJECT_ID_KEY, OCEL_OBJECT_TYPE_KEY],
    )?;
    df.export_e2o_csv(
        path.join("e2o.csv"),
        &[OCEL_EVENT_ID_KEY, OCEL_OBJECT_ID_KEY, OCEL_QUALIFIER_KEY],
    )?;
    df.export_o2o_csv(
        path.join("o2o.csv"),
        &[OCEL_OBJECT_ID_KEY, OCEL_OBJECT_ID_2_KEY, OCEL_QUALIFIER_KEY],
    )?;
    conn.query("CREATE NODE TABLE Event(id STRING PRIMARY KEY, type STRING, time TIMESTAMP);")?;
    conn.query("CREATE NODE TABLE Object(id STRING PRIMARY KEY, type STRING);")?;
    conn.query("CREATE REL TABLE E2O(FROM Event to Object, qualifier STRING);")?;
    conn.query("CREATE REL TABLE O2O(FROM Object to Object, qualifier STRING);")?;
    conn.query(&format!(
        "COPY Event FROM '{}' (header=true);",
        path.join("events.csv").to_string_lossy()
    ))?;
    conn.query(&format!(
        "COPY Object FROM '{}' (header=true);",
        path.join("objects.csv").to_string_lossy()
    ))?;
    conn.query(&format!(
        "COPY E2O FROM '{}' (header=true)",
        path.join("e2o.csv").to_string_lossy()
    ))?;
    conn.query(&format!(
        "COPY O2O FROM '{}' (header=true)",
        path.join("o2o.csv").to_string_lossy()
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, time::Instant};

    use chrono::DateTime;
    use kuzu::{Connection, Database};

    use crate::{
        import_ocel_xml_file, ocel::graph_db::ocel_kuzudb::export_ocel_to_kuzudb_generic,
        utils::test_utils::get_test_data_path,
    };

    use super::{export_ocel_to_kuzudb_typed, KuzuDBExportError};

    #[test]
    fn test_kuzudb_export() {
        let export_path = get_test_data_path()
            .join("export")
            .join("order-management-ocel.kuzu");
        let _er = remove_file(&export_path);
        let ocel = import_ocel_xml_file(
            get_test_data_path()
                .join("ocel")
                .join("order-management.xml"),
        );
        export_ocel_to_kuzudb_generic(export_path, &ocel).unwrap();
    }

    #[test]
    fn test_typed_kuzudb_export() {
        let export_path = get_test_data_path()
            .join("export")
            .join("order-management-typed-ocel.kuzu");
        let _er = remove_file(&export_path);
        let ocel = import_ocel_xml_file(
            get_test_data_path()
                .join("ocel")
                .join("order-management.xml"),
        );
        export_ocel_to_kuzudb_typed(export_path, &ocel).unwrap();
    }

    #[test]
    fn perf_test_kuzu() -> Result<(), KuzuDBExportError> {
        let export_path = get_test_data_path().join("export").join("stress-ocel.kuzu");
        let _er = remove_file(&export_path);
        let db = Database::new(export_path, kuzu::SystemConfig::default()).unwrap();
        let conn = Connection::new(&db)?;
        conn.query("CREATE NODE TABLE Event(id STRING PRIMARY KEY, type STRING, time TIMESTAMP);")?;
        let now = Instant::now();
        for i in 0..10_000 {
            // println!("{i}");
            let query = format!(
                "CREATE (e:Event {{id: {}, type: 'Pay Order', time: timestamp('{}')}});",
                i,
                DateTime::UNIX_EPOCH.to_rfc3339(),
            );
            // conn.query(&query)?;
            println!("{query}");
        }
        println!("{:?}", now.elapsed());
        Ok(())
        // let ocel = import_ocel_xml_file(get_test_data_path().join("ocel").join("ocel2-p2p.xml"));
        // export_ocel_to_kuzudb_typed(export_path, &ocel).unwrap();
    }
}
