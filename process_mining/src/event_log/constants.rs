/// Common identifying field for event identities (i.e., activities)
///
/// _Note_: While the concept XES extension is the de-facto standard for identifying activity names,
/// some XES files might not use `concept:name` or have events without a `concept:name` attribute.
///
/// __Usage Generally Discouraged__: _Instead, try to use event log classifiers, which utilize information present in the event log itself and handle fall-backs individually_
pub const ACTIVITY_NAME: &str = "concept:name";
/// Prefix prepended to attribute keys when flattening event log to events only
///
/// Primarily used only for interoperability with `PM4Py`
pub const TRACE_PREFIX: &str = "case:";
/// Common identifying field for trace identities (i.e., trace IDs)
///
/// __Usage Generally Discouraged__: _Instead, try to use event log classifiers, which utilize information present in the event log itself and handle fall-backs individually_
///
/// See also [`ACTIVITY_NAME`]
pub const TRACE_ID_NAME: &str = "concept:name";
/// Constructed combination of [`TRACE_PREFIX`] and [`TRACE_ID_NAME`]
///
/// Primarily used only for interoperability with `PM4Py`
pub const PREFIXED_TRACE_ID_NAME: &str = "case:concept:name";
