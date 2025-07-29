use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::Path,
};

use chrono::DateTime;
use polars::{
    error::PolarsResult,
    frame::DataFrame,
    io::SerWriter,
    prelude::{AnyValue, CsvWriter, SortMultipleOptions, TimeUnit},
    series::Series,
};

use crate::{ocel::ocel_struct::OCELAttributeValue, OCEL};

#[cfg(test)]
mod tests;
/// Multiple [`DataFrame`]s that, combined, hold information of an [`OCEL`]
#[derive(Debug, Clone)]
pub struct OCELDataFrames {
    /// Objects in the [`OCEL`]
    ///
    /// (containing columns [`OCEL_OBJECT_ID_KEY`] and [`OCEL_OBJECT_TYPE_KEY`])
    pub objects: DataFrame,
    /// Events in the [`OCEL`]
    ///
    /// (containing columns [`OCEL_EVENT_ID_KEY`], [`OCEL_EVENT_TYPE_KEY`], and [`OCEL_EVENT_TIMESTAMP_KEY`])
    pub events: DataFrame,
    /// Event-to-Object (E2O) Relationships in the [`OCEL`]
    ///
    /// (containing columns [`OCEL_EVENT_ID_KEY`], [`OCEL_EVENT_TYPE_KEY`], [`OCEL_EVENT_TIMESTAMP_KEY`], [`OCEL_OBJECT_ID_KEY`], [`OCEL_OBJECT_TYPE_KEY`], and [`OCEL_QUALIFIER_KEY`])
    pub e2o: DataFrame,
    /// Object-to-Object (O2O) Relationships in the [`OCEL`]
    ///
    /// (containing columns [`OCEL_OBJECT_ID_KEY`], [`OCEL_OBJECT_ID_2_KEY`], and [`OCEL_QUALIFIER_KEY`])
    pub o2o: DataFrame,
    /// Object attribute changes in the [`OCEL`]
    ///
    /// (containing columns [`OCEL_OBJECT_ID_KEY`], [`OCEL_OBJECT_TYPE_KEY`], [`OCEL_CHANGED_FIELD_KEY`], and [`OCEL_EVENT_TIMESTAMP_KEY`], as well as columns for all object attributes)
    pub object_changes: DataFrame,
}

impl OCELDataFrames {
    /// Export the objects `DataFrame` as a CSV file in the given path
    ///
    /// The column names that should be exported can also be specified.
    /// If the passed slice is empty, all columns are exported.
    ///
    /// Example
    ///
    /// ```
    /// use process_mining::{ocel, OCEL};
    /// use process_mining::event_log::ocel::dataframe::*;
    /// use process_mining::utils::test_utils::get_test_data_path;
    /// let ocel = ocel![
    ///     events:
    ///     ("place", ["c:1", "o:1", "i:1", "i:2"]),
    ///     ("pack", ["o:1", "i:2", "e:1"]),
    ///     o2o:
    ///     ("o:1", "i:1")
    /// ];
    /// let mut ocel_dfs = ocel_to_dataframes(&ocel);
    /// ocel_dfs.export_objects_csv(get_test_data_path().join("export").join("ocel-objects.csv"),&[OCEL_OBJECT_ID_KEY]).expect("Object CSV Export Failed");
    /// ```
    pub fn export_objects_csv<P: AsRef<Path>>(
        &mut self,
        export_path: P,
        columns_to_include: &[&str],
    ) -> PolarsResult<()> {
        let f = File::create(export_path)?;
        let mut csvw = CsvWriter::new(f);
        let df = &mut self.objects;
        if !columns_to_include.is_empty() {
            csvw.finish(&mut df.select(columns_to_include.iter().copied())?)?;
        } else {
            csvw.finish(df)?;
        }
        Ok(())
    }

    /// Export the events `DataFrame` as a CSV file in the given path
    ///
    /// The column names that should be exported can also be specified.
    /// If the passed slice is empty, all columns are exported.
    ///
    /// Example
    ///
    /// ```
    /// use process_mining::{ocel, OCEL};
    /// use process_mining::event_log::ocel::dataframe::*;
    /// use process_mining::utils::test_utils::get_test_data_path;
    /// let ocel = ocel![
    ///     events:
    ///     ("place", ["c:1", "o:1", "i:1", "i:2"]),
    ///     ("pack", ["o:1", "i:2", "e:1"]),
    ///     o2o:
    ///     ("o:1", "i:1")
    /// ];
    /// let mut ocel_dfs = ocel_to_dataframes(&ocel);
    /// ocel_dfs.export_events_csv(get_test_data_path().join("export").join("ocel-events.csv"),&[]).expect("Event CSV Export Failed");
    /// ```
    pub fn export_events_csv<P: AsRef<Path>>(
        &mut self,
        export_path: P,
        columns_to_include: &[&str],
    ) -> PolarsResult<()> {
        let f = File::create(export_path)?;
        let mut csvw = CsvWriter::new(f);
        let df = &mut self.events;
        if !columns_to_include.is_empty() {
            csvw.finish(&mut df.select(columns_to_include.iter().copied())?)?;
        } else {
            csvw.finish(df)?;
        }
        Ok(())
    }

    /// Export the event-to-object (E2O) `DataFrame` as a CSV file in the given path
    ///
    /// The column names that should be exported can also be specified.
    /// If the passed slice is empty, all columns are exported.
    ///
    /// Example
    ///
    /// ```
    /// use process_mining::{ocel, OCEL};
    /// use process_mining::event_log::ocel::dataframe::*;
    /// use process_mining::utils::test_utils::get_test_data_path;
    /// let ocel = ocel![
    ///     events:
    ///     ("place", ["c:1", "o:1", "i:1", "i:2"]),
    ///     ("pack", ["o:1", "i:2", "e:1"]),
    ///     o2o:
    ///     ("o:1", "i:1")
    /// ];
    /// let mut ocel_dfs = ocel_to_dataframes(&ocel);
    /// ocel_dfs.export_e2o_csv(get_test_data_path().join("export").join("ocel-e2o.csv"),&[]).expect("E2O CSV Export Failed");
    /// ```
    pub fn export_e2o_csv<P: AsRef<Path>>(
        &mut self,
        export_path: P,
        columns_to_include: &[&str],
    ) -> PolarsResult<()> {
        let f = File::create(export_path)?;
        let mut csvw = CsvWriter::new(f);
        let df = &mut self.e2o;
        if !columns_to_include.is_empty() {
            csvw.finish(&mut df.select(columns_to_include.iter().copied())?)?;
        } else {
            csvw.finish(df)?;
        }
        Ok(())
    }

    /// Export the object-to-object (O2O) `DataFrame` as a CSV file in the given path
    ///
    /// The column names that should be exported can also be specified.
    /// If the passed slice is empty, all columns are exported.
    ///
    /// Example
    ///
    /// ```
    /// use process_mining::{ocel, OCEL};
    /// use process_mining::event_log::ocel::dataframe::*;
    /// use process_mining::utils::test_utils::get_test_data_path;
    /// let ocel = ocel![
    ///     events:
    ///     ("place", ["c:1", "o:1", "i:1", "i:2"]),
    ///     ("pack", ["o:1", "i:2", "e:1"]),
    ///     o2o:
    ///     ("o:1", "i:1")
    /// ];
    /// let mut ocel_dfs = ocel_to_dataframes(&ocel);
    /// ocel_dfs.export_o2o_csv(get_test_data_path().join("export").join("ocel-o2o.csv"),&[]).expect("O2O CSV Export Failed");
    /// ```
    pub fn export_o2o_csv<P: AsRef<Path>>(
        &mut self,
        export_path: P,
        columns_to_include: &[&str],
    ) -> PolarsResult<()> {
        let f = File::create(export_path)?;
        let mut csvw = CsvWriter::new(f);
        let df = &mut self.o2o;
        if !columns_to_include.is_empty() {
            csvw.finish(&mut df.select(columns_to_include.iter().copied())?)?;
        } else {
            csvw.finish(df)?;
        }
        Ok(())
    }

    /// Export the object attribute changes `DataFrame` as a CSV file in the given path
    ///
    /// The column names that should be exported can also be specified.
    /// If the passed slice is empty, all columns are exported.
    ///
    /// Example
    ///
    /// ```
    /// use process_mining::{ocel, OCEL};
    /// use process_mining::event_log::ocel::dataframe::*;
    /// use process_mining::utils::test_utils::get_test_data_path;
    /// let ocel = ocel![
    ///     events:
    ///     ("place", ["c:1", "o:1", "i:1", "i:2"]),
    ///     ("pack", ["o:1", "i:2", "e:1"]),
    ///     o2o:
    ///     ("o:1", "i:1")
    /// ];
    /// let mut ocel_dfs = ocel_to_dataframes(&ocel);
    /// ocel_dfs.export_object_changes_csv(get_test_data_path().join("export").join("ocel-object-changes.csv"),&[]).expect("Object Changes CSV Export Failed");
    /// ```
    pub fn export_object_changes_csv<P: AsRef<Path>>(
        &mut self,
        export_path: P,
        columns_to_include: &[&str],
    ) -> PolarsResult<()> {
        let f = File::create(export_path)?;
        let mut csvw = CsvWriter::new(f);
        let df = &mut self.object_changes;
        if !columns_to_include.is_empty() {
            csvw.finish(&mut df.select(columns_to_include.iter().copied())?)?;
        } else {
            csvw.finish(df)?;
        }
        Ok(())
    }
}

fn ocel_attribute_val_to_any_value(val: &OCELAttributeValue) -> AnyValue<'_> {
    match val {
        OCELAttributeValue::String(s) => AnyValue::StringOwned(s.into()),
        OCELAttributeValue::Time(t) => AnyValue::Datetime(
            t.timestamp_nanos_opt().unwrap(),
            TimeUnit::Nanoseconds,
            None,
        ),
        OCELAttributeValue::Integer(i) => AnyValue::Int64(*i),
        OCELAttributeValue::Float(f) => AnyValue::Float64(*f),
        OCELAttributeValue::Boolean(b) => AnyValue::Boolean(*b),
        OCELAttributeValue::Null => AnyValue::Null,
    }
}
/// Event ID Key in `DataFrame` (e.g., pay_order-12345)
pub const OCEL_EVENT_ID_KEY: &str = "ocel:eid";
/// Event Type Key in `DataFrame` (e.g., pay order)
pub const OCEL_EVENT_TYPE_KEY: &str = "ocel:activity";
/// Event/Object Changes Timestamp Key in `DataFrame` (e.g., 2025-05-05-12:34Z)
pub const OCEL_EVENT_TIMESTAMP_KEY: &str = "ocel:timestamp";
/// Object ID Key in `DataFrame` (e.g., pay_order-12345)
pub const OCEL_OBJECT_ID_KEY: &str = "ocel:oid";
/// Second Object ID Key (e.g., for O2O Relationsips) in `DataFrame` (e.g., pay_order-12345)
pub const OCEL_OBJECT_ID_2_KEY: &str = "ocel:oid_2";
/// Object Type Key in `DataFrame` (e.g., orders)
pub const OCEL_OBJECT_TYPE_KEY: &str = "ocel:type";
/// Qualifier Key in `DataFrame` (e.g., places)
pub const OCEL_QUALIFIER_KEY: &str = "ocel:qualifier";
/// Changed Field Key in `DataFrame` (e.g., prices)
pub const OCEL_CHANGED_FIELD_KEY: &str = "ocel:field";

/// Convert an [`OCEL`] to a set of [`DataFrame`]s ([`OCELDataFrames`])
///
/// See [`OCELDataFrames`] for the structure of the Dataframes
pub fn ocel_to_dataframes(ocel: &OCEL) -> OCELDataFrames {
    let object_attributes: HashSet<String> = ocel
        .object_types
        .iter()
        .flat_map(|ot| &ot.attributes)
        .map(|at| at.name.clone())
        .collect();
    let actual_object_attributes: HashSet<String> = ocel
        .objects
        .iter()
        .flat_map(|o| o.attributes.iter().map(|oa| oa.name.clone()))
        .collect();
    // println!("Object attributes: {:?}; Actual object attributes: {:?}", object_attributes.len(), actual_object_attributes.len());
    if !object_attributes.is_superset(&actual_object_attributes) {
        eprintln!(
            "Warning: Global object attributes is not a superset of actual object attributes"
        );
    }
    let object_attributes_initial: HashSet<String> = object_attributes
        .clone()
        .into_iter()
        .filter(|a| {
            ocel.objects.iter().any(|o| {
                o.attributes
                    .iter()
                    .any(|oa| &oa.name == a && oa.time == DateTime::UNIX_EPOCH)
            })
        })
        .collect();
    let objects_df = DataFrame::from_iter(
        object_attributes_initial
            .into_iter()
            .map(|name| {
                Series::from_any_values(
                    (&name).into(),
                    ocel.objects
                        .iter()
                        .map(|o| {
                            let attr = o
                                .attributes
                                .iter()
                                .find(|a| a.name == name && a.time == DateTime::UNIX_EPOCH);
                            let val = match attr {
                                Some(v) => &v.value,
                                None => &OCELAttributeValue::Null,
                            };
                            ocel_attribute_val_to_any_value(val)
                        })
                        .collect::<Vec<_>>()
                        .as_ref(),
                    false,
                )
                .unwrap()
            })
            .chain(vec![
                Series::from_any_values(
                    OCEL_OBJECT_ID_KEY.into(),
                    &ocel
                        .objects
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.id.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_OBJECT_TYPE_KEY.into(),
                    &ocel
                        .objects
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.object_type.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
            ]),
    );

    let all_evs_with_rels: Vec<_> = ocel
        .events
        .iter()
        .flat_map(|e| e.relationships.iter().map(move |r| (e, r)))
        .collect();

    let obj_id_to_type_map: HashMap<&String, &String> = ocel
        .objects
        .iter()
        .map(|o| (&o.id, &o.object_type))
        .collect();

    let mut e2o_df = DataFrame::from_iter(vec![
        Series::from_any_values(
            OCEL_EVENT_ID_KEY.into(),
            &all_evs_with_rels
                .iter()
                .map(|(e, _r)| AnyValue::StringOwned(e.id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_EVENT_TYPE_KEY.into(),
            &all_evs_with_rels
                .iter()
                .map(|(e, _r)| AnyValue::StringOwned(e.event_type.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_EVENT_TIMESTAMP_KEY.into(),
            &all_evs_with_rels
                .iter()
                .map(|(e, _r)| {
                    AnyValue::Datetime(
                        e.time.timestamp_nanos_opt().unwrap(),
                        TimeUnit::Nanoseconds,
                        None,
                    )
                })
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_OBJECT_ID_KEY.into(),
            &all_evs_with_rels
                .iter()
                .map(|(_e, r)| AnyValue::StringOwned(r.object_id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_OBJECT_TYPE_KEY.into(),
            &all_evs_with_rels
                .iter()
                .map(|(_e, r)| {
                    if let Some(obj_type) = obj_id_to_type_map.get(&r.object_id) {
                        AnyValue::StringOwned((*obj_type).into())
                    } else {
                        // eprintln!(
                        //     "Invalid object id in E2O reference: Event: {}, Object: {}",
                        //     _e.id, r.object_id
                        // );
                        AnyValue::Null
                    }
                })
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_QUALIFIER_KEY.into(),
            &all_evs_with_rels
                .iter()
                .map(|(_e, r)| AnyValue::StringOwned(r.qualifier.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
    ]);

    let all_obj_with_rels: Vec<_> = ocel
        .objects
        .iter()
        .flat_map(|o| o.relationships.iter().map(move |r| (o, r)))
        .collect();

    let o2o_df = DataFrame::from_iter(vec![
        Series::from_any_values(
            OCEL_OBJECT_ID_KEY.into(),
            &all_obj_with_rels
                .iter()
                .map(|(o, _r)| AnyValue::StringOwned(o.id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_OBJECT_ID_2_KEY.into(),
            &all_obj_with_rels
                .iter()
                .map(|(_o, r)| AnyValue::StringOwned(r.object_id.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
        Series::from_any_values(
            OCEL_QUALIFIER_KEY.into(),
            &all_obj_with_rels
                .iter()
                .map(|(_o, r)| AnyValue::StringOwned(r.qualifier.clone().into()))
                .collect::<Vec<_>>(),
            false,
        )
        .unwrap(),
    ]);

    let mut object_changes_df = DataFrame::from_iter(
        object_attributes
            .into_iter()
            .map(|name| {
                Series::from_any_values(
                    (&name).into(),
                    ocel.objects
                        .iter()
                        .flat_map(|o| {
                            o.attributes.iter()
                            // .filter(|a| a.time != DateTime::UNIX_EPOCH)
                        })
                        .map(|a| {
                            if a.name == name {
                                ocel_attribute_val_to_any_value(&a.value)
                            } else {
                                AnyValue::Null
                            }
                        })
                        .collect::<Vec<_>>()
                        .as_ref(),
                    false,
                )
                .unwrap()
            })
            .chain(vec![
                Series::from_any_values(
                    OCEL_OBJECT_ID_KEY.into(),
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| vec![o.id.clone(); o.attributes.len()])
                        .map(|o_id| AnyValue::StringOwned(o_id.into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_OBJECT_TYPE_KEY.into(),
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| vec![o.object_type.clone(); o.attributes.len()])
                        .map(|o_type| AnyValue::StringOwned(o_type.into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_CHANGED_FIELD_KEY.into(),
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| {
                            o.attributes
                                .iter()
                                // .filter(|oa| oa.time != DateTime::UNIX_EPOCH)
                                .map(|oa| oa.name.clone())
                        })
                        .map(|chngd_field_name| AnyValue::StringOwned(chngd_field_name.into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_EVENT_TIMESTAMP_KEY.into(),
                    &ocel
                        .objects
                        .iter()
                        .flat_map(|o| {
                            o.attributes
                                .iter()
                                // .filter(|oa| oa.time != DateTime::UNIX_EPOCH)
                                .map(|oa| oa.time)
                        })
                        .map(|date| {
                            AnyValue::Datetime(
                                date.timestamp_nanos_opt().unwrap(),
                                TimeUnit::Nanoseconds,
                                None,
                            )
                        })
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
            ]),
    );
    let event_attributes: HashSet<String> = ocel
        .event_types
        .iter()
        .flat_map(|et| &et.attributes)
        .map(|at| at.name.clone())
        .collect();
    let mut events_df = DataFrame::from_iter(
        event_attributes
            .into_iter()
            .map(|name| {
                Series::from_any_values(
                    (&name).into(),
                    ocel.events
                        .iter()
                        .map(|e| {
                            let attr = e.attributes.iter().find(|a| a.name == name);
                            let val = match attr {
                                Some(v) => &v.value,
                                None => &OCELAttributeValue::Null,
                            };
                            ocel_attribute_val_to_any_value(val)
                        })
                        .collect::<Vec<_>>()
                        .as_ref(),
                    false,
                )
                .unwrap()
            })
            .chain(vec![
                Series::from_any_values(
                    OCEL_EVENT_ID_KEY.into(),
                    &ocel
                        .events
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.id.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_EVENT_TYPE_KEY.into(),
                    &ocel
                        .events
                        .iter()
                        .map(|o| AnyValue::StringOwned(o.event_type.clone().into()))
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
                Series::from_any_values(
                    OCEL_EVENT_TIMESTAMP_KEY.into(),
                    &ocel
                        .events
                        .iter()
                        .map(|o| {
                            AnyValue::Datetime(
                                o.time.timestamp_nanos_opt().unwrap(),
                                TimeUnit::Nanoseconds,
                                None,
                            )
                        })
                        .collect::<Vec<_>>(),
                    false,
                )
                .unwrap(),
            ]),
    );
    events_df
        .sort_in_place(
            vec![OCEL_EVENT_TIMESTAMP_KEY],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .unwrap();

    e2o_df
        .sort_in_place(
            vec![OCEL_EVENT_TIMESTAMP_KEY],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .unwrap();

    object_changes_df
        .sort_in_place(
            vec![OCEL_EVENT_TIMESTAMP_KEY],
            SortMultipleOptions::default().with_maintain_order(true),
        )
        .unwrap();
    OCELDataFrames {
        objects: objects_df,
        events: events_df,
        object_changes: object_changes_df,
        o2o: o2o_df,
        e2o: e2o_df,
    }
}
