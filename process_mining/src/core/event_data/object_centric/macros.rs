//! Convenient Macros for Creating Object-centric Event Data
/// Creates an [`OCEL`] containing the given events, objects, event-to-object (e2o) relations.
///
/// `ocel!` allows `OCEL`s to be defined with a list of events and their e2o relations.
/// Each event is a tuple that contains as first entry the event type and as second entry the
/// related objects as a list of object identifier. Object identifiers have to be
/// denoted '`ob_type`':'`ob_id`'.
///
/// See the example below, containing two events: ev:1 and ev:2 with event types "place" and "pack",
/// respectively. 'ev:1' has as e2o relations ('`ev_1`', 'c:1'), ('`ev_1`', 'o:1'), ('`ev_1`', 'i:1'),
/// ('`ev_1`', 'i:1'), where 'c:1' has object type 'c' (e.g., customer), 'o:1' has object
/// type 'o' (e.g., order), and 'i:1', 'i:2' have object type 'i' (e.g., item).
///
/// ```
/// use process_mining::ocel;
///
/// let object_centric_event_log = ocel![
///     events:
///     ("place", ["c:1", "o:1", "i:1", "i:2"]),
///     ("pack", ["o:1", "i:2", "e:1"]),
///     o2o:
///     ("o:1", "i:1")
/// ];
/// ```
///
/// [`OCEL`]: crate::core::OCEL
#[macro_export]
macro_rules! ocel {
    (events: $(($ev_type:expr, [$($object:expr), *])), *, o2o: $(($from_ob:expr, $to_ob:expr)), *) => {{
        use ::std::collections::{HashSet, HashMap};
        use chrono::{TimeDelta, TimeZone, Utc};
        use $crate::core::event_data::object_centric::{
            OCEL, OCELEvent, OCELObject, OCELRelationship, OCELType,
        };
        use std::ops::AddAssign;

        // Adding all event types, object types, and objects exactly once
        // There can be multiple events that can be identical
        let mut event_types_set = HashSet::new();
        let mut object_types_set = HashSet::new();
        let mut events = Vec::new();
        let mut object_set = HashSet::new();

        // Timestamp are given in a distance of seconds starting at 01-01-2020 at 00:00:00
        let mut timestamp = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();

        // Events are labelled contiunously
        let mut ev_counter = 0;
        $(
            event_types_set.insert(
                $ev_type
            );

            // Relations are given by square brackets, i.e., [...], after each event
            // From the relations the existence of the objects is derived
            let mut relations = Vec::new();
            $(
                let object_type = $object.to_string().split(":").next().unwrap().to_string();
                relations.push(OCELRelationship::new($object.to_string(), object_type.to_string()));
                object_types_set.insert(
                    object_type.clone()
                );
                object_set.insert(
                    (
                        $object.to_string(),
                        object_type,
                    )
                );
            )*

            // The event is created with event identifier ev:x and the given event type
            ev_counter += 1;
            events.push(
                OCELEvent::new(
                    format!("ev:{}",ev_counter),
                    $ev_type.to_string(),
                    timestamp.clone(),
                    vec![],
                    relations,
                )
            );

            // Current timestamp is updated for the next event to be added
            timestamp.add_assign(TimeDelta::seconds(1));
        )*

        // From the unique event types, object_types, and pairs of objects with their ids and their
        // ev_type, the corresponding [`OCELType`] and [`OCELObject`] are created
        let event_types = event_types_set.into_iter().map(|ev_type| {
            OCELType{
                name: ev_type.to_string(),
                attributes: Vec::new(),
            }
        }).collect::<Vec<_>>();

        #[allow(unused_mut)]
        let mut object_id_to_object = object_set.into_iter().map(|(ob_id, ob_type)| {
            (
                ob_id.clone(),
                OCELObject {
                    id: ob_id,
                    object_type: ob_type,
                    attributes: Vec::new(),
                    relationships: Vec::new(),
                }
            )
        }).collect::<HashMap<_, _>>();

        // Adds o2o relations
        $(
            let object_type = $to_ob.to_string().split(":").next().unwrap().to_string();
            let o2o_relation = OCELRelationship::new($to_ob.to_string(), object_type.to_string());

            if object_id_to_object.contains_key(&$from_ob.to_string()) {
                object_id_to_object.get_mut(&$from_ob.to_string()).unwrap().relationships.push(o2o_relation);
            } else {
                object_types_set.insert(object_type.clone());
                object_id_to_object.insert($from_ob.to_string(),
                    OCELObject{
                        id: $from_ob.to_string(),
                        object_type: object_type,
                        attributes: Vec::new(),
                        relationships: vec![o2o_relation]
                    }
                );
            }
        )*

        let object_types = object_types_set.into_iter().map(|ob_type| {
            OCELType{
                name: ob_type,
                attributes: Vec::new(),
            }
        }).collect::<Vec<_>>();

        OCEL {
            event_types,
            object_types,
            events,
            objects: object_id_to_object.iter().map(|(_, object)| object.to_owned()).collect(),
        }
    }};
}
