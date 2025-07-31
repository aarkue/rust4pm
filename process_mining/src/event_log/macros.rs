/// Create an [`Attribute`].
///
/// An attribute is denoted by a `key => value` mapping.
///
/// # Examples
///
/// ```rust
/// use process_mining::attribute;
///
/// let attr_1 = attribute!("concept:name" => "Approve");
/// let attr_2 = attribute!("time:timestamp" => chrono::Utc::now());
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
    ($key:expr, $val:expr) => {
        $crate::attribute!($key => $val)
    };
}

/// Create an [`Attributes`] instance.
///
/// Attributes are denoted by a comma-separated list of `key => value` mappings.
///
/// # Examples
///
/// ```rust
/// use process_mining::attributes;
///
/// let attrs = attributes!(
///     "concept:name" => "Approve",
///     "time:timestamp" => chrono::Utc::now(),
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
/// # Examples
///
/// ```rust
/// use process_mining::event;
///
/// // Create an event with activity "a".
/// let event_1 = event!("a");
/// // Create an event with the current time as timestamp
/// let event_2 = event!("a"; {
///     "time:timestamp" => chrono::Utc::now()
/// });
/// // Create an event with timestamp 0 and additional attributes
/// let event_3 = event!("a"; {
///     "time:timestamp" => chrono::DateTime::UNIX_EPOCH,
///     "org:resource" => "John",
///     "cost" => 2500.00,
///     "approved" => true
/// });
///
/// // Use some pre-defined value for an attribute
/// use chrono::{DateTime, FixedOffset};
/// let dt: DateTime<FixedOffset> = "2025-01-01T00:00:00+02:00".parse().unwrap();
/// let event_4 = event!("a"; {
///     "time:timestamp" => dt
/// });
/// ```
///
/// [`Event`]: crate::event_log::Event
#[macro_export]
macro_rules! event {
    // Macro rule to disable adding a default timestamp. Intended for internal use.
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
        let mut evt = $crate::event!(NO_TIMESTAMP; $($input)*);

        if $crate::event_log::XESEditableAttribute::get_by_key(
            &evt.attributes,
            "time:timestamp",
        )
        .is_none()
        {
            $crate::event_log::XESEditableAttribute::add_attribute(
                &mut evt.attributes,
                $crate::attribute!(
                    "time:timestamp" => chrono::DateTime::UNIX_EPOCH
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
/// use process_mining::trace;
///
/// let trace_1 = trace!("a", "b", "c", "d");
/// // Add trace-level attributes
/// let trace_2 = trace!({"org:resource" => "John"}; "a", "b", "c", "d");
/// // Add trace and event-level attributes
/// let trace_3 = trace!(
///     {"outcome" => "approved"};
///     "a"; {"time:timestamp" => chrono::DateTime::UNIX_EPOCH},
///     "b"; {"time:timestamp" => chrono::Utc::now()},
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
        let mut trace = $crate::event_log::Trace {
            attributes: $crate::attributes!(
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

        let delta = chrono::TimeDelta::hours(1);

        // Make sure the first event has a timestamp, then fill with previous timestamp + 1h
        if let Some(evt) = trace.events.first_mut() {
            if $crate::event_log::XESEditableAttribute::get_by_key(
                &evt.attributes,
                "time:timestamp"
                ).is_none()
            {
                $crate::event_log::XESEditableAttribute::add_attribute(&mut evt.attributes,
                    $crate::attribute!(
                        "time:timestamp" => chrono::DateTime::UNIX_EPOCH
                    )
                )
            }
        }

        for i in 1..trace.events.len() {

            if $crate::event_log::XESEditableAttribute::get_by_key(&trace.events[i].attributes, "time:timestamp").is_none() {
                let prev_timestamp = *$crate::event_log::XESEditableAttribute::get_by_key(&trace.events[i-1].attributes, "time:timestamp").unwrap().value.try_as_date().expect("Timestamp should be a date.");
                $crate::event_log::XESEditableAttribute::add_attribute(
                    &mut trace.events[i].attributes,
                    $crate::attribute!(
                        "time:timestamp" => prev_timestamp + delta
                    )
                )
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
/// use process_mining::event_log;
///
/// event_log!(
///     ["a", "b", "c", "d"],
///     ["a", "c", "b", "d"],
/// );
///
/// // Add trace and event-level attributes
/// event_log!(
///     ["a"; {"org:resource" => "John"}, "b"] {"cost" => 2500.00},
///     ["a", "c"],
/// );
///
/// // Add log-level attributes
/// event_log!(
///    {"created_at" => chrono::Utc::now()};
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
        $({ $($key:expr => $value:expr),* $(,)? };)?
        $(
            [$($events:tt)*] $({ $($keys:expr => $vals:expr),* $(,)? })?
        ),* $(,)?
     ) => {{
         let mut log = $crate::event_log::EventLog {
             attributes: $crate::attributes!(
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

         log.traces.iter_mut().enumerate().for_each(|(idx,trace)| {
            if $crate::event_log::XESEditableAttribute::get_by_key(&trace.attributes, "concept:name").is_none() {
                $crate::event_log::XESEditableAttribute::add_attribute(
                    &mut trace.attributes,
                    $crate::attribute!(
                        "concept:name" => i64::try_from(idx).unwrap()
                    )
                )
            }
         });

         log
    }}
}
