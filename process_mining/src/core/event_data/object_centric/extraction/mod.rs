//! Build an OCEL from relational data using a declarative blueprint.
//!
//! A [`Blueprint`](crate::core::event_data::object_centric::extraction::Blueprint) declares a flat
//! graph of nodes producing rows, and mappings turning those rows into OCEL events, objects and
//! relations. It contains no connection details and no schema snapshot: both are supplied by the
//! caller at execution time, which keeps a blueprint portable and free of secrets.

/// The blueprint schema version this build reads and writes.
///
/// A Blueprint's `version` field is checked against this during validation.
pub const MODEL_VERSION: u32 = 1;

pub mod blueprint;
pub mod case_centric;
pub mod catalog;
pub mod compile;
#[cfg(feature = "extraction-dbcon")]
pub mod dbcon_provider;
pub mod desugar;
// The `DuckDbSink`-driving half is gated item by item inside; `snapshot`/`OcelSnapshot` are
// pure and are what the ordering tests compare with, so gating the whole module on
// `ocel-duckdb` made `--features extraction-blueprint` alone fail to compile its own tests.
#[cfg(test)]
mod differential;
#[cfg(feature = "ocel-duckdb")]
pub mod duckdb_sink;
pub mod expr;
mod extract;
mod graph;
mod mapping_exec;
pub mod predicate;
pub mod provider;
mod pushdown;
pub mod report;
pub(crate) mod row;
mod schema;
pub mod sink;
pub mod slim_sink;

#[cfg(feature = "ocel-sqlite")]
pub mod sqlite_provider;
#[cfg(test)]
mod tests;
pub mod validate;
pub mod value;

pub use blueprint::{
    Blueprint, BlueprintParseError, DuplicateObjectPolicy, EventEndpoint, IdRendering,
    InlineObjectRef, Mapping, MappingEntry, MissingEndpointPolicy, Node, NodeOp, ObjectEndpoint,
    Target,
};
pub use case_centric::{
    event_log_to_ocel, event_log_to_slim_ocel, write_event_log_to_sink, EventLogWriteReport,
    FlatEventTable, CASE_OBJECT_TYPE, CASE_QUALIFIER,
};
pub use catalog::{Catalog, ColumnSchema, ExtractionCatalog, TablePreview, TableSchema};
pub use compile::{
    compile, CompileError, CompiledOcel, EmissionShape, Probe, ProbeKind, RejectReason, SqlDialect,
    ViewDef,
};
#[cfg(feature = "extraction-dbcon")]
pub use dbcon_provider::{discover_catalog, DbconProviderError, DbconRowProvider};
pub use desugar::desugar;
#[cfg(feature = "ocel-duckdb")]
pub use duckdb_sink::DuckDbSink;
pub use expr::{
    AttributeMapping, SplitKind, SplitSpec, TimestampFormat, TimestampSource, ValueExpression,
};
pub use extract::extract;
pub use predicate::{CompareOp, Literal, Operand, Predicate};
pub use provider::{ProviderError, RowProvider};
pub use report::{
    DropReason, ExtractionError, ExtractionReport, ExtractionTiming, MappingRef, MappingStats,
};
pub use sink::{EventRef, ExtractionSink, FinalizeReport, ObjectRef, Resolution, SinkError};
pub use slim_sink::SlimOcelSink;
#[cfg(feature = "ocel-sqlite")]
pub use sqlite_provider::SqliteRowProvider;
pub use validate::{validate, ValidationError};
pub use value::{Value, ValueKind};

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn the_blueprint_schema_names_its_top_level_fields() {
        let schema = schemars::schema_for!(Blueprint);
        let json = serde_json::to_value(&schema).expect("serialize schema");
        let properties = json
            .get("properties")
            .and_then(|p| p.as_object())
            .expect("an object schema with properties");
        for field in ["version", "id_rendering", "nodes", "mappings"] {
            assert!(
                properties.contains_key(field),
                "schema is missing '{field}'"
            );
        }
    }

    #[test]
    fn the_catalog_schema_is_generated_too() {
        let schema = schemars::schema_for!(ExtractionCatalog);
        assert!(serde_json::to_value(&schema).is_ok());
    }
}
