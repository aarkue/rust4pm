/// Create an [`Attribute`].
///
/// An attribute is denoted by a `key => value` mapping.
///
/// # Examples
///
/// ```rust
/// use process_mining::{attribute, chrono::Utc};
///
///
/// let attr_1 = attribute!("concept:name" => "Approve");
/// let attr_2 = attribute!("time:timestamp" => Utc::now());
/// let attr_3 = attribute!("cost" => 2500.00);
/// ```
///
/// [`Attribute`]: crate::event_log::Attribute
#[macro_export]
macro_rules! attribute {
    ($key:expr => $val:expr) => {
        $crate::event_log::Attribute::new(
            $key.into(),
            $crate::event_log::AttributeValue::from($val),
        )
    };
}

/// Create an [`Attributes`] instance.
///
/// Attributes are denoted by a comma-separated list of `key => value` mappings.
///
/// # Examples
///
/// ```rust
/// use process_mining::{attributes, chrono::Utc};
///
/// let attrs = attributes!(
///     "concept:name" => "Approve",
///     "time:timestamp" => Utc::now(),
///     "cost" => 2500.00
/// );
/// ```
///
/// [`Attributes`]: crate::event_log::Attributes
#[macro_export]
macro_rules! attributes {
    ($($key:expr => $value:expr),* $(,)?) => {
        vec![
            $(
                $crate::attribute!($key => $value)
            ),*
        ]
    };
}

/// Create an [`Event`].
///
/// An event consists of an activity, and can optionally be given attributes using
/// a `{key => value, ...}` syntax, separated from the activity using a semicolon.
///
/// If no timestamp is provided, the unix epoch will be added as the timestamp.
///
/// # Examples
///
/// ```rust
/// use process_mining::{
///     chrono::{DateTime, FixedOffset, Utc},
///     event,
/// };
///
/// // Create an event with activity "a".
/// let event_1 = event!("a");
/// // Create an event with the current time as timestamp
/// let event_2 = event!("a"; {
///     "time:timestamp" => Utc::now()
/// });
/// // Create an event with timestamp 0 and additional attributes
/// let event_3 = event!("a"; {
///     "time:timestamp" => DateTime::UNIX_EPOCH,
///     "org:resource" => "John",
///     "cost" => 2500.00,
///     "approved" => true
/// });
///
/// // Use some pre-defined value for an attribute
/// let dt: DateTime<FixedOffset> = "2025-01-01T00:00:00+02:00".parse().unwrap();
/// let event_4 = event!("a"; {
///     "time:timestamp" => dt
/// });
/// ```
///
/// [`Event`]: crate::event_log::Event
#[macro_export]
macro_rules! event {
    // Macro rule to avoid adding a default timestamp. Intended for internal use in
    // the `trace` macro.
    (NO_TIMESTAMP; $name:expr $(; { $($key:expr => $value:expr),* $(,)? })?) => {
        $crate::event_log::Event {
            attributes: vec![
                $crate::attribute!("concept:name" => $name),
                $(
                    $(
                        $crate::attribute!($key => $value)
                    ),*
                )?
            ]
        }
    };
    ($($input:tt)*) => {{
        use $crate::{
            attribute,
            chrono::DateTime,
            event,
            event_log::XESEditableAttribute,
        };

        let mut evt = event!(NO_TIMESTAMP; $($input)*);

        if evt.attributes.get_by_key("time:timestamp").is_none() {
            evt.attributes.add_attribute(
                attribute!(
                    "time:timestamp" => DateTime::UNIX_EPOCH
                )
            )
        }
        evt
    }};
}

/// Create a [`Trace`].
///
/// A trace is a sequence of events, using the syntax of the [`event`] macro.
/// If an event has no timestamp, it is set 1 hour after the previous event. The
/// first event uses the unix epoch as the default timestamp.
///
/// Trace-level attributes can optionally be provided as the first component of the
/// macro invocation, using a `{key => value, ...}` syntax, and separated from the
/// events with a semicolon.
///
/// # Examples
///
/// ```rust
/// use process_mining::{
///     chrono::{DateTime, Utc},
///     trace
/// };
///
/// let trace_1 = trace!("a", "b", "c", "d");
/// // Add trace-level attributes
/// let trace_2 = trace!({"org:resource" => "John"}; "a", "b", "c", "d");
/// // Add trace and event-level attributes
/// let trace_3 = trace!(
///     {"outcome" => "approved"};
///     "a"; {"time:timestamp" => DateTime::UNIX_EPOCH},
///     "b"; {"time:timestamp" => Utc::now()},
///     "c",
///     "d"; {"approved" => true, "cost" => 2500.00}
/// );
/// ```
///
/// [`Trace`]: crate::event_log::Trace
/// [`event`]: crate::event
#[macro_export]
macro_rules! trace {
    (
        { $($key:expr => $value:expr),* $(,)? };
        $(
            $activity:expr $(; { $($keys:expr => $values:expr),* $(,)?})?
        ),*
    ) => {{
        use $crate::{
            attribute, attributes,
            chrono::{DateTime, TimeDelta},
            event_log::{Trace, XESEditableAttribute},
        };

        let mut trace = Trace {
            attributes: attributes!(
                            $($key => $value),*
                        ),
            events: vec![
                $(
                    $crate::event!(NO_TIMESTAMP; $activity; {
                        $(
                            $(
                                $keys => $values
                            ),*
                        )?
                    })
                ),*
            ]
        };

        let delta = TimeDelta::hours(1);

        // Make sure the first event has a timestamp, then fill missing timestamps
        // with previous timestamp + 1h
        if let Some(evt) = trace.events.first_mut() {
            if evt.attributes.get_by_key(
                "time:timestamp"
                ).is_none()
            {
                evt.attributes.add_attribute(
                    attribute!(
                        "time:timestamp" => DateTime::UNIX_EPOCH
                    )
                )
            }
        }

        for i in 1..trace.events.len() {
            if trace.events[i].attributes.get_by_key("time:timestamp").is_none() {
                let prev_timestamp = *trace.events[i - 1]
                    .attributes
                    .get_by_key("time:timestamp")
                    .unwrap()
                    .value
                    .try_as_date()
                    .expect("Timestamp should be a date.");
                trace.events[i].attributes.add_attribute(
                    attribute!(
                        "time:timestamp" => prev_timestamp + delta
                    )
                );
            }
        }



        trace
    }};
    ($($content:tt)*) => {
        $crate::trace!({}; $($content)*)
    }
}

/// Create an [`EventLog`].
///
/// An event log is a sequence of traces, each denoted by square brackets containing
/// events.
///
/// The macro invocation may optionally begin with the definition of log-level
/// attributes using a `{key => value, ...}` syntax. Similarly, trace-level
/// attributes may optionally follow the square brackets. Events follow the syntax
/// of the [`event`] macro.
///
/// Traces are automatically provided trace ids (`concept:name`), if they aren't
/// manually provided. The trace ids correspond to the index in the event log.
///
/// # Examples
/// ```rust
/// use process_mining::{chrono::Utc, event_log};
///
/// // Create an event log with traces <a,b,c,d> and <a,c,b,d>
/// event_log!(
///     ["a", "b", "c", "d"],
///     ["a", "c", "b", "d"],
/// );
///
/// // Add trace and event-level attributes
/// event_log!(
///     ["a"; {"org:resource" => "John"}, "b", "c", "d"] {"cost" => 2500.00},
///     ["a", "c", "b", "d"],
/// );
///
/// // Add log-level attributes
/// event_log!(
///    {"created_at" => Utc::now()};
///    ["a", "b", "c", "d"]
/// );
///
/// ```
///
/// [`EventLog`]: crate::event_log::EventLog
/// [`event`]: crate::event
#[macro_export]
macro_rules! event_log {
    (
        $({ $($key:expr => $value:expr),* $(,)? }$(;)?)?
        $(
            [$($events:tt)*] $({ $($keys:expr => $vals:expr),* $(,)? })?
        ),* $(,)?
     ) => {{
        use $crate::{
            attribute,
            attributes,
            event_log::{EventLog, XESEditableAttribute},
        };
        let mut log = EventLog {
            attributes: attributes!(
                $(
                    $($key => $value),*
                )?
            ),
            traces: vec![
                $(
                    $crate::trace!(
                        $({ $($keys => $vals),*};)?
                        $($events)*
                    )
                ),*
            ],
            extensions: None,
            classifiers: None,
            global_trace_attrs: None,
            global_event_attrs: None,
        };

         // Fill in missing trace ids using their index
         log.traces.iter_mut().enumerate().for_each(|(idx,trace)| {
            if trace.attributes.get_by_key("concept:name").is_none() {
                trace.attributes.add_attribute(
                    attribute!(
                        "concept:name" => i64::try_from(idx).unwrap()
                    )
                )
            }
         });

         log
    }}
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeDelta};
    use uuid::Uuid;

    use crate::event_log::{Attribute, AttributeValue, XESEditableAttribute};

    /// Ensure that all types of attributes can be made using  the [`attribute`]
    /// macro. Uses expressions, literals, and identifiers and each value enum variant.
    #[test]
    fn test_attribute_macro() {
        assert_eq!(
            attribute!("string_attr" => String::from("Wee")),
            Attribute::new(
                "string_attr".to_string(),
                AttributeValue::String(String::from("Wee"))
            )
        );
        assert_eq!(
            attribute!("str_attr" => "asd"),
            Attribute::new(
                "str_attr".to_string(),
                AttributeValue::String(String::from("asd"))
            )
        );
        assert_eq!(
            attribute!("date_attr" => DateTime::UNIX_EPOCH),
            Attribute::new(
                "date_attr".to_string(),
                AttributeValue::Date(DateTime::UNIX_EPOCH.fixed_offset())
            )
        );
        assert_eq!(
            attribute!("int_attr" => 5),
            Attribute::new("int_attr".to_string(), AttributeValue::Int(5))
        );
        assert_eq!(
            attribute!("float_attr" => 3.7),
            Attribute::new("float_attr".to_string(), AttributeValue::Float(3.7))
        );
        assert_eq!(
            attribute!("bool_attr".to_string() => true),
            Attribute::new("bool_attr".to_string(), AttributeValue::Boolean(true))
        );
        attribute!("id" => Uuid::new_v4());
        assert_eq!(
            attribute!("list" => vec![]),
            Attribute::new("list".to_string(), AttributeValue::List(vec![]))
        );
    }

    /// Ensure that all types of attributes can be made using  the [`attributes`]
    /// macro. Uses expressions, literals, and identifiers and each value enum variant.
    #[test]
    fn test_attributes_macro() {
        let id = Uuid::new_v4();
        assert_eq!(
            attributes!(
                "bool_attr".to_string() => true,
                "date_attr" => DateTime::UNIX_EPOCH,
                "float_attr" => 3.7,
                "id" => id,
                "int_attr" => 5,
                "list" => vec![],
                "str_attr" => "asd",
                "string_attr" => String::from("Wee")
            ),
            vec![
                Attribute::new("bool_attr".to_string(), AttributeValue::Boolean(true)),
                Attribute::new(
                    "date_attr".to_string(),
                    AttributeValue::Date(DateTime::UNIX_EPOCH.fixed_offset())
                ),
                Attribute::new("float_attr".to_string(), AttributeValue::Float(3.7)),
                Attribute::new("id".to_string(), AttributeValue::ID(id)),
                Attribute::new("int_attr".to_string(), AttributeValue::Int(5)),
                Attribute::new("list".to_string(), AttributeValue::List(vec![])),
                Attribute::new(
                    "str_attr".to_string(),
                    AttributeValue::String(String::from("asd"))
                ),
                Attribute::new(
                    "string_attr".to_string(),
                    AttributeValue::String(String::from("Wee"))
                ),
            ]
        )
    }

    /// Test the creation of a simple event (no attributes).
    #[test]
    fn test_event_macro_simple() {
        let event_1 = event!("an activity name");
        assert_eq!(
            event_1
                .attributes
                .get_by_key("concept:name")
                .unwrap()
                .value
                .try_as_string()
                .unwrap(),
            "an activity name"
        );

        // Pass an identifier as the activity
        let act = "another activity name";
        let event_2 = event!(act);
        assert_eq!(
            event_2
                .attributes
                .get_by_key("concept:name")
                .unwrap()
                .value
                .try_as_string()
                .unwrap(),
            "another activity name"
        );
    }

    /// Ensure that all kinds of attributes can be use in the [`event`] macro.
    #[test]
    fn test_event_macro_with_attributes() {
        let evt = event!("a"; {
            "string_attr" => String::from("Hello, World!"),
            "str_attr" => "value",
            "date_attr" => chrono::Utc::now(),
            "int_attr" => 5,
            "float_attr" => 3.7,
            "bool_attr" => true,
            "id" => uuid::Uuid::new_v4(),
            "list" => vec![],
        });

        // Check some of the attributes
        assert_eq!(
            evt.attributes
                .get_by_key("int_attr")
                .unwrap()
                .value
                .try_as_int()
                .unwrap(),
            &5
        );
        assert_eq!(
            evt.attributes
                .get_by_key("string_attr")
                .unwrap()
                .value
                .try_as_string()
                .unwrap(),
            "Hello, World!"
        );
        assert_eq!(
            *evt.attributes
                .get_by_key("float_attr")
                .unwrap()
                .value
                .try_as_float()
                .unwrap(),
            3.7
        );

        // If no timestamp (`time:timestamp`) is provided, unix epoch is used
        assert_eq!(
            *evt.attributes
                .get_by_key("time:timestamp")
                .and_then(|a| a.value.try_as_date())
                .unwrap(),
            DateTime::UNIX_EPOCH
        );
    }

    #[test]
    fn test_event_macro_default_timestamp() {
        let evt = event!("a");
        assert_eq!(
            *evt.attributes
                .get_by_key("time:timestamp")
                .and_then(|a| a.value.try_as_date())
                .unwrap(),
            DateTime::UNIX_EPOCH
        )
    }

    /// Ensure that all  macros can take identifiers as values on all levels.
    /// Checks for [`attribute`], [`attributes`], [`event`], [`trace`], and
    /// [`event_log`].
    #[test]
    fn test_macros_take_identifiers() {
        let value = 5;

        assert_eq!(
            attribute!("key" => value),
            Attribute::new("key".to_string(), AttributeValue::Int(5))
        );
        assert_eq!(
            attributes!("key" => value),
            vec![Attribute::new("key".to_string(), AttributeValue::Int(5))]
        );
        assert_eq!(
            *event!("a"; {"key_event" => value})
                .attributes
                .get_by_key("key_event")
                .and_then(|a| a.value.try_as_int())
                .unwrap(),
            value
        );
        let trace = trace!(
            {"key_trace" => value};
            "a"; {"key_event" => value}, "b", "c", "d"
        );
        assert_eq!(
            *trace
                .attributes
                .get_by_key("key_trace")
                .and_then(|a| a.value.try_as_int())
                .unwrap(),
            value
        );
        assert_eq!(
            *trace
                .events
                .first()
                .unwrap()
                .attributes
                .get_by_key("key_event")
                .and_then(|a| a.value.try_as_int())
                .unwrap(),
            value
        );
        let log = event_log!(
            {"key_log" => value}; ["a"; {"key_event" => value}, "b", "c", "d"] {"key_trace" => value}
        );

        assert_eq!(
            *log.attributes
                .get_by_key("key_log")
                .and_then(|a| a.value.try_as_int())
                .unwrap(),
            value
        );

        let log_trace = log.traces.first().unwrap();
        assert_eq!(
            *log_trace
                .attributes
                .get_by_key("key_trace")
                .and_then(|a| a.value.try_as_int())
                .unwrap(),
            value
        );
        assert_eq!(
            *log_trace
                .events
                .first()
                .unwrap()
                .attributes
                .get_by_key("key_event")
                .and_then(|a| a.value.try_as_int())
                .unwrap(),
            value
        );
    }

    /// Test a simple invocation of the [`trace`] macro, as well as the reproducibility
    /// of the invocation.
    #[test]
    fn test_trace_macro_simple() {
        let trace = trace!("a", "b", "c", "d");
        assert_eq!(
            trace
                .events
                .iter()
                .map(|evt| evt
                    .attributes
                    .get_by_key("concept:name")
                    .and_then(|a| a.value.try_as_string())
                    .unwrap())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );

        // Reproducible
        assert_eq!(trace, trace!("a", "b", "c", "d"))
    }

    /// Test that the default timestamp of the first event is the unix epoch and that
    /// all events without explicit timestamps are 1h later than their predecessor.
    #[test]
    fn test_trace_macro_event_timestamps() {
        let trace = trace!("a", "b", "c"; { "time:timestamp" => DateTime::UNIX_EPOCH + TimeDelta::hours(5)}, "d");
        let timestamps: Vec<_> = trace
            .events
            .iter()
            .map(|evt| {
                *evt.attributes
                    .get_by_key("time:timestamp")
                    .and_then(|a| a.value.try_as_date())
                    .unwrap()
            })
            .collect();
        let expected_timestamps = vec![
            DateTime::UNIX_EPOCH,
            DateTime::UNIX_EPOCH + TimeDelta::hours(1),
            DateTime::UNIX_EPOCH + TimeDelta::hours(5),
            DateTime::UNIX_EPOCH + TimeDelta::hours(6),
        ];
        assert_eq!(timestamps, expected_timestamps);
    }

    /// Ensure that empty traces can be created
    #[test]
    fn test_empty_trace_macro() {
        assert!(trace!().events.is_empty());
        assert!(trace!({};).events.is_empty());

        let empty_trace_with_attributes = trace!({"key" => 5};);
        assert!(empty_trace_with_attributes.events.is_empty());
        // Only 1 attribute. Trace id is only added in the event_log macro
        assert!(empty_trace_with_attributes.attributes.len() == 1);
        assert!(empty_trace_with_attributes
            .attributes
            .get_by_key("key")
            .is_some_and(|x| x.value == AttributeValue::Int(5)))
    }

    /// Ensure that all kinds of attributes can be used on on all levels of the
    /// [`trace`] macro. Only checks that it compiles.
    #[test]
    fn test_trace_macro_attributes() {
        // Trace attributes and events in trace with attributes
        let trace = trace!({
            "string_attr" => String::from("Hello, World!"),
            "str_attr" => "value",
            "date_attr" => chrono::Utc::now(),
            "int_attr" => 5,
            "float_attr" => 3.7,
            "bool_attr" => true,
            "id" => uuid::Uuid::new_v4(),
            "list" => vec![],
        }; "a", "b"; {
            "string_attr" => String::from("Hello, World!!"),
            "str_attr" => "value_2",
            "date_attr" => chrono::Utc::now(),
            "int_attr" => 10,
            "float_attr" => 7.3,
            "bool_attr" => false,
            "id" => uuid::Uuid::new_v4(),
            "list" => vec![],
        });

        // Check a few attributes
        assert_eq!(
            *trace
                .attributes
                .get_by_key("float_attr")
                .and_then(|a| a.value.try_as_float())
                .unwrap(),
            3.7
        );
        assert_eq!(
            *trace
                .events
                .get(1)
                .and_then(|evt| evt.attributes.get_by_key("float_attr"))
                .and_then(|a| a.value.try_as_float())
                .unwrap(),
            7.3
        );
    }

    /// Test the creation of a simple event log.
    #[test]
    fn test_event_log_macro_simple() {
        let log = event_log!(
            ["a", "b", "c", "d"],
            ["a", "c", "b", "d"] {"concept:name" => 42},
            ["a", "e", "d"]
        );

        let activity_projection = log
            .traces
            .iter()
            .map(|trace| {
                trace
                    .events
                    .iter()
                    .map(|evt| {
                        evt.attributes
                            .get_by_key("concept:name")
                            .and_then(|a| a.value.try_as_string())
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            activity_projection,
            vec![
                vec!["a", "b", "c", "d"],
                vec!["a", "c", "b", "d"],
                vec!["a", "e", "d"]
            ]
        );

        let trace_ids = log
            .traces
            .iter()
            .map(|trace| {
                *trace
                    .attributes
                    .get_by_key("concept:name")
                    .and_then(|a| a.value.try_as_int())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        // Trace ids are the index, unless they were manually set
        assert_eq!(trace_ids, vec![0, 42, 2]);
    }

    /// Ensure that empty event logs can be created (with and without attributes)
    #[test]
    fn test_empty_event_log_macro() {
        // Empty log
        assert!(event_log!().traces.is_empty());
        // Empty log with attributes
        assert!(event_log!({};).traces.is_empty());

        let empty_event_log_with_attributes = event_log!({"key" => 5};);
        assert!(empty_event_log_with_attributes.traces.is_empty());
        assert!(empty_event_log_with_attributes.attributes.len() == 1);
        assert!(empty_event_log_with_attributes
            .attributes
            .get_by_key("key")
            .is_some_and(|x| x.value == AttributeValue::Int(5)));
    }

    /// Ensure that event logs can be created with empty traces (with and
    /// without attributes)
    #[test]
    fn test_event_log_macro_empty_trace() {
        let log = event_log!([]);
        assert!(log
            .traces
            .first()
            .is_some_and(|trace| trace.events.is_empty()));

        // Can also use attributes for the empty trace
        let log_2 = event_log!([] {"key" => "value"});
        assert!(log_2.traces.first().is_some_and(|trace| {
            trace.events.is_empty()
                && trace
                    .attributes
                    .get_by_key("key")
                    .is_some_and(|attr| attr.value == AttributeValue::String("value".to_string()))
        }));
    }

    /// Ensure that event log creation is deterministic (in particular, trace ids)
    #[test]
    fn event_log_macro_equality() {
        assert_eq!(
            event_log!(["a", "b", "c", "d"], ["a", "c", "b", "d"]),
            event_log!(["a", "b", "c", "d"], ["a", "c", "b", "d"]),
        );
        assert_ne!(
            event_log!(["a", "b", "c", "d"], ["a", "c", "b", "d"]),
            event_log!(["a", "c", "b", "d"], ["a", "b", "c", "d"]),
        );
    }

    #[test]
    /// Ensure that all kinds of attributes can be used on on all levels of the
    /// [`event_log`] macro.
    fn event_log_attributes() {
        // Event log with attributes, trace with attributes, and event with attributes
        let log = event_log!(
        {
            "string_attr" => String::from("Hello, World!!"),
            "str_attr" => "value",
            "date_attr" => chrono::Utc::now(),
            "int_attr" => 5,
            "float_attr" => 3.7,
            "bool_attr" => true,
            "id" => uuid::Uuid::new_v4(),
            "list" => vec![],
        };
        ["a", "b"; {
            "string_attr" => String::from("Hello, World!!"),
            "str_attr" => "value",
            "date_attr" => chrono::Utc::now(),
            "int_attr" => 10,
            "float_attr" => 7.3,
            "bool_attr" => false,
            "id" => uuid::Uuid::new_v4(),
            "list" => vec![],
        }, "c", "d"] {
            "string_attr" => String::from("Hello, World!!!"),
            "str_attr" => "value",
            "date_attr" => chrono::Utc::now(),
            "int_attr" => 15,
            "float_attr" => 37.0,
            "bool_attr" => false,
            "id" => uuid::Uuid::new_v4(),
            "list" => vec![],
        },
        ["no", "attributes", "in", "this", "trace"],
        );

        // Check a few attributes
        assert_eq!(
            *log.attributes
                .get_by_key("float_attr")
                .and_then(|a| a.value.try_as_float())
                .unwrap(),
            3.7
        );
        let trace = log.traces.first().unwrap();
        assert_eq!(
            *trace
                .attributes
                .get_by_key("float_attr")
                .and_then(|a| a.value.try_as_float())
                .unwrap(),
            37.0
        );
        assert_eq!(
            *trace
                .events
                .get(1)
                .and_then(|evt| evt.attributes.get_by_key("float_attr"))
                .and_then(|a| a.value.try_as_float())
                .unwrap(),
            7.3
        );

        // Check that the semicolon after event log attributes can be omitted
        let _log = event_log!({"created_at" => DateTime::UNIX_EPOCH} ["a", "b", "c", "d"]);
    }
}
