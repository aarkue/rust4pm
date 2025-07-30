use crate::ocel::{linked_ocel::LinkedOCELAccess, ocel_struct::OCELAttributeType};
use polars::{error::PolarsResult, frame::DataFrame, io::SerWriter, prelude::CsvWriter};
use std::{fs::File, path::Path};

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
    csvw.finish(df)?;
    Ok(())
}

fn clean_type_name(name: &str) -> String {
    name.replace(" ", "")
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}
fn ocel_attribute_type_to_kuzu_dtype(attr_type: &str) -> &'static str {
    match OCELAttributeType::from_type_str(attr_type) {
        OCELAttributeType::String => "STRING",
        OCELAttributeType::Time => "TIMESTAMP",
        OCELAttributeType::Integer => "INT64",
        OCELAttributeType::Float => "DOUBLE",
        OCELAttributeType::Boolean => "BOOLEAN",
        OCELAttributeType::Null => "NULL",
    }
}
/// WIP
#[cfg(feature = "dataframes")]
pub fn export_ocel_to_kuzudb_typed<'a, P: AsRef<Path>>(
    db_path: P,
    locel: &'a impl LinkedOCELAccess<'a>,
) -> Result<(), KuzuDBExportError> {
    use std::fs::remove_file;

    use itertools::Itertools;
    use kuzu::{Connection, Database, SystemConfig};

    use crate::ocel::dataframe::{
        e2o_to_df_for_types, event_type_to_df, o2o_to_df_for_types, object_type_to_df,
    };

    let db = Database::new(db_path, SystemConfig::default())?;
    let conn = Connection::new(&db)?;
    let tmp = tempfile::tempdir()?;
    let path = tmp.path();
    let mut all_ev_table_names = Vec::new();
    for ev_type in locel.get_ev_types() {
        let mut ev_df = event_type_to_df(locel, ev_type)?;
        if let Some(etype) = locel.get_ev_type(ev_type) {
            export_df_to_csv(&mut ev_df, path.join("tmp.csv"))?;
            let clean_name = clean_type_name(ev_type);

            let attribute_fields_str = etype
                .attributes
                .iter()
                .map(|a| {
                    format!(
                        "`{}` {}",
                        a.name,
                        ocel_attribute_type_to_kuzu_dtype(&a.value_type)
                    )
                })
                .join(", ");
            let q = format!(
                "CREATE NODE TABLE `{}`(id STRING PRIMARY KEY, time TIMESTAMP {} {});",
                clean_name,
                if attribute_fields_str.is_empty() {
                    ""
                } else {
                    ", "
                },
                attribute_fields_str
            );
            println!("Query for event type {ev_type}: {q}");
            conn.query(&q)?;

            conn.query(&format!(
                "COPY {} FROM '{}' (header=true);",
                clean_name,
                path.join("tmp.csv").to_string_lossy()
            ))?;
            all_ev_table_names.push(clean_name);
            remove_file(path.join("tmp.csv"))?;
        }
    }
    let mut all_ob_table_names = Vec::new();
    for ob_type in locel.get_ob_types() {
        let mut ob_df = object_type_to_df(locel, ob_type)?;
        export_df_to_csv(&mut ob_df, path.join("tmp.csv"))?;
        let clean_name = clean_type_name(ob_type);
        let q = format!("CREATE NODE TABLE `{clean_name}`(id STRING PRIMARY KEY);",);
        println!("Query for object type {ob_type}: {q}");
        conn.query(&q)?;

        conn.query(&format!(
            "COPY {} FROM '{}' (header=true);",
            clean_name,
            path.join("tmp.csv").to_string_lossy()
        ))?;
        all_ob_table_names.push(clean_name);
        remove_file(path.join("tmp.csv"))?;
    }
    conn.query(&format!(
        "CREATE REL TABLE E2O ({}, qualifier STRING)",
        all_ev_table_names
            .iter()
            .cartesian_product(all_ob_table_names.iter())
            .map(|(ev_type, ob_type)| format!("FROM `{ev_type}` TO `{ob_type}`"))
            .join(", "),
    ))?;
    for ev_type in locel.get_ev_types() {
        for ob_type in locel.get_ob_types() {
            let mut e2o_df = e2o_to_df_for_types(locel, ev_type, ob_type)?;
            export_df_to_csv(&mut e2o_df, path.join("tmp.csv"))?;
            conn.query(&format!(
                "COPY E2O FROM '{}' (header=true, from='{}', to='{}');",
                path.join("tmp.csv").to_string_lossy(),
                clean_type_name(ev_type),
                clean_type_name(ob_type)
            ))?;
            remove_file(path.join("tmp.csv"))?;
        }
    }

    conn.query(&format!(
        "CREATE REL TABLE O2O ({}, qualifier STRING)",
        all_ob_table_names
            .iter()
            .cartesian_product(all_ob_table_names.iter())
            .map(|(ev_type, ob_type)| format!("FROM `{ev_type}` TO `{ob_type}`"))
            .join(", "),
    ))?;
    for from_ob_type in locel.get_ob_types() {
        for to_ob_type in locel.get_ob_types() {
            let mut o2o_df = o2o_to_df_for_types(locel, from_ob_type, to_ob_type)?;
            export_df_to_csv(&mut o2o_df, path.join("tmp.csv"))?;
            conn.query(&format!(
                "COPY O2O FROM '{}' (header=true, from='{}', to='{}');",
                path.join("tmp.csv").to_string_lossy(),
                clean_type_name(from_ob_type),
                clean_type_name(to_ob_type)
            ))?;
            remove_file(path.join("tmp.csv"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{fs::remove_file, time::Instant};

    use chrono::DateTime;
    use kuzu::{Connection, Database};

    use crate::{
        import_ocel_xml_file,
        ocel::{
            graph_db::ocel_kuzudb::export_ocel_to_kuzudb_generic, linked_ocel::IndexLinkedOCEL,
        },
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

        let locel = IndexLinkedOCEL::from(ocel);
        let now = Instant::now();
        export_ocel_to_kuzudb_typed(export_path, &locel).unwrap();
        println!("Export took {:?}", now.elapsed());
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
