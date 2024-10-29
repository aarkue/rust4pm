use crate::event_log::event_log_struct::EventLogClassifier;
use crate::event_log::{Attributes, Event};
use petgraph::adj::DefaultIx;
use petgraph::graph::NodeIndex;
use petgraph::{Directed, Graph};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::SetLastValueWins;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
/// A struct to create a [`Hash`] value from an [`Event`].
pub struct EventHash(u64);

impl EventHash {
    fn new(event: &Event) -> EventHash {
        let mut hasher = DefaultHasher::new();
        event.hash(&mut hasher);
        Self(hasher.finish())
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
/// A partial order trace of [`Event`]s.
/// Contains all events of a trace and their particular partial relations.
pub struct PartialOrderTrace {
    /// Trace-level attributes
    pub attributes: Attributes,
    /// A mapping from an [`Event`]'s hash value [`EventHash`] to the [`Event`] itself.
    pub event_map: HashMap<EventHash, Event>,
    #[serde_as(as = "SetLastValueWins<_>")]
    /// The partial relations between the [`Event`]s contained in the [`PartialOrderTrace`].
    pub partial_relations: HashSet<(EventHash, EventHash)>,
}

impl PartialOrderTrace {
    /// Create new [`PartialOrderTrace`] with no events and no partial relations.
    pub fn new() -> Self {
        Self {
            attributes: Attributes::new(),
            event_map: HashMap::new(),
            partial_relations: HashSet::new(),
        }
    }

    /// Serialize to JSON string.
    pub fn to_json(self) -> String { serde_json::to_string(&self).unwrap() }

    /// Returns all the start events of the [`PartialOrderTrace`], i.e., the events having no
    /// preceding event.
    pub fn get_start_events(&self) -> HashSet<&Event> {
        let mut result: HashSet<&Event> = HashSet::from_iter(self.event_map.values());

        self.partial_relations.iter().for_each(|(_, event_hash)| {
            result.remove(self.event_map.get(event_hash).unwrap());
        });

        result
    }

    /// Returns all the end events of the [`PartialOrderTrace`], i.e., the events having no
    /// succeeding event.
    pub fn get_end_events(&self) -> HashSet<&Event> {
        let mut result: HashSet<&Event> = HashSet::from_iter(self.event_map.values());

        self.partial_relations.iter().for_each(|(event_hash, _)| {
            result.remove(self.event_map.get(event_hash).unwrap());
        });

        result
    }

    /// Adds an [`Event`] to the [`PartialOrderTrace`].
    pub fn add_event(&mut self, event: &Event) {
        self.event_map.insert(EventHash::new(&event), event.clone());
    }

    /// Removes an [`Event`] from the [`PartialOrderTrace`] including all partial relations
    /// containing the [`Event`] itself.
    pub fn remove_event(&mut self, event: &Event) {
        let event_hash = EventHash::new(&event);
        self.event_map.remove(&event_hash);

        self.partial_relations
            .retain(|(from, to)| {
                from != &event_hash && to != &event_hash
            });
    }

    /// Adds a partial relation by adding two [`EventHash`] values.
    pub fn add_partial_relation(&mut self, from: &Event, to: &Event) {
        self.partial_relations.insert((EventHash::new(from), EventHash::new(to)));
    }

    /// Removes a partial relation identified by two [`EventHash`] values.
    pub fn remove_partial_relation(&mut self, from: &Event, to: &Event) {
        self.partial_relations.remove(&(EventHash::new(from), EventHash::new(to)));
    }

    /// Returns all events preceding an [`Event`].
    pub fn get_ingoing_events(&self, event: &Event) -> Vec<&Event> {
        self.partial_relations
            .iter()
            .filter_map(|(from, to)| {
                if self.event_map.get(to).unwrap() == event {
                    Some(self.event_map.get(from).unwrap())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns all events succeeding an [`Event`].
    pub fn get_outgoing_events(&self, event: &Event) -> Vec<&Event> {
        self.partial_relations
            .iter()
            .filter_map(|(from, to)| {
                if self.event_map.get(from).unwrap() == event {
                    Some(self.event_map.get(to).unwrap())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Creates a [`Graph`] from the [`PartialOrderTrace`].
    pub fn to_graph(&self, classifier: &EventLogClassifier) -> Graph<String, &str> {
        let mut graph: Graph<String, &str, Directed, DefaultIx> = Graph::<String, &str>::new();
        let mut event_to_node: HashMap<Event, NodeIndex> = HashMap::new();

        self.event_map.iter().for_each(|(_, event)| {
            let new_node = graph.add_node(classifier.get_class_identity(event));
            event_to_node.insert(event.clone(), new_node);
        });
        self.partial_relations.iter().for_each(|(from, to)| {
            graph.add_edge(
                *event_to_node.get(self.event_map.get(from).unwrap()).unwrap(),
                *event_to_node.get(self.event_map.get(to).unwrap()).unwrap(),
                "",
            );
        });

        graph
    }


    /// By creating a [`Graph`] for each [`PartialOrderTrace`] and for two given [`EventLogClassifier`]
    /// used for classification in each [`PartialOrderTrace`], the partial order traces are compared
    /// for equality by checking whether their graphs are isomorphic.
    pub fn is_isomorphic(&self, other: &PartialOrderTrace, classifier: &EventLogClassifier, other_classifier: &EventLogClassifier) -> bool {
        let graph = self.to_graph(classifier);
        let other_graph = other.to_graph(other_classifier);

        petgraph::algo::is_isomorphic(&graph, &other_graph)
    }

    #[cfg(feature = "graphviz-export")]
    /// Export directly-follows graph as a PNG image
    ///
    /// The PNG file is written to the specified filepath
    ///
    /// _Note_: This is an export method for __visualizing__ the directly-follows graph.
    ///
    /// Only available with the `graphviz-export` feature.
    pub fn export_png<P: AsRef<std::path::Path>>(&self, classifier: &EventLogClassifier, path: P) -> Result<(), std::io::Error> {
        super::image_export::export_p_trace_image_png(self, classifier, path)
    }

    #[cfg(feature = "graphviz-export")]
    /// Export directly-follows graph as an SVG image.
    ///
    /// The SVG file is written to the specified filepath.
    ///
    /// _Note_: This is an export method for __visualizing__ the directly-follows graph.
    ///
    /// Only available with the `graphviz-export` feature.
    pub fn export_svg<P: AsRef<std::path::Path>>(&self, classifier: &EventLogClassifier, path: P) -> Result<(), std::io::Error> {
        super::image_export::export_p_trace_image_svg(self, &classifier, path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Partial Order event log consisting of a list of [`PartialOrderTrace`]s and log [`Attributes`]
pub struct PartialOrderEventLog {
    /// Top-level attributes
    pub attributes: Attributes,
    /// List of [`PartialOrderTrace`]
    partial_order_traces: Vec<PartialOrderTrace>,
}

impl Default for PartialOrderEventLog {
    fn default() -> Self { Self::new() }
}

impl PartialOrderEventLog {
    /// Create new [`PartialOrderEventLog`] with [`Attributes`] and no [`PartialOrderTrace`]s.
    pub fn new() -> Self {
        Self {
            attributes: Attributes::new(),
            partial_order_traces: Vec::new(),
        }
    }

    /// Adds a trace to the list of [`PartialOrderTrace`]s.
    pub fn add_trace(&mut self, trace: &PartialOrderTrace) {
        self.partial_order_traces.push(trace.clone());
    }
}


#[cfg(test)]
mod tests {
    pub const SAMPLE_JSON_P_TRACE: &str = r#"
{
    "attributes":[]
    "event_map":
    {
        "5747163295916315711":
        {
            "attributes":[
                {
                    "key":"concept:name",
                    "value":{"type":"String","content":"Make Coffee"},
                    "own_attributes":null
                }
            ]
        },
        "6954268098552642400":
        {
            "attributes":[
                {
                    "key":"concept:name",
                    "value":{"type":"String","content":"Listen to Podcast"},
                    "own_attributes":null
                }
            ]
        },
        "16623231828871474506":
        {
            "attributes":[
                {
                    "key":"concept:name",
                    "value":{"type":"String","content":"Breakfast"},
                    "own_attributes":null
                }
            ]
        },
        "10135014032808600890":
        {
            "attributes":[
                {
                    "key":"concept:name",
                    "value":{"type":"String","content":"Wake up"},
                    "own_attributes":null
                }
            ]
        },
        "18102607635049523792":
        {
            "attributes":[
                {
                    "key":"concept:name",
                    "value":{"type":"String","content":"Brush teeth"},
                    "own_attributes":null
                }
            ]
        },
        "10735273421821633029":
        {
            "attributes":[
                {
                    "key":"concept:name",
                    "value":{"type":"String","content":"Work"},
                    "own_attributes":null
                }
            ]
        },
        "10110542754164153265":
        {
            "attributes":[
                {
                    "key":"concept:name",
                    "value":{"type":"String","content":"Wait for call from Internet provider"},
                    "own_attributes":null
                }
            ]
        }
    },
    "partial_relations":
    [
        [10135014032808600890,18102607635049523792],
        [5747163295916315711,16623231828871474506],
        [16623231828871474506,10735273421821633029],
        [10135014032808600890,6954268098552642400],
        [10135014032808600890,5747163295916315711],
        [6954268098552642400,10735273421821633029],
        [18102607635049523792,16623231828871474506]
    ]
}"#;

    use super::*;
    #[test]
    fn partial_order_trace_test() {
        let mut partial_order: PartialOrderTrace = PartialOrderTrace::new();

        let event_1 = Event::new("Wake up".into());
        partial_order.add_event(&event_1);
        let event_2 = Event::new("Brush teeth".into());
        partial_order.add_event(&event_2);
        let event_3 = Event::new("Make Coffee".into());
        partial_order.add_event(&event_3);
        let event_4 = Event::new("Breakfast".into());
        partial_order.add_event(&event_4);
        let event_5 = Event::new("Listen to Podcast".into());
        partial_order.add_event(&event_5);
        let event_6 = Event::new("Work".into());
        partial_order.add_event(&event_6);
        let event_7 = Event::new("Wait for call from Internet provider".into());
        partial_order.add_event(&event_7);

        partial_order.add_partial_relation(&event_1, &event_2);
        partial_order.add_partial_relation(&event_1, &event_3);
        partial_order.add_partial_relation(&event_1, &event_5);
        partial_order.add_partial_relation(&event_2, &event_4);
        partial_order.add_partial_relation(&event_3, &event_4);
        partial_order.add_partial_relation(&event_4, &event_6);
        partial_order.add_partial_relation(&event_5, &event_6);

        assert!(partial_order.event_map.len() == 7);

        let mut partial_order_iso: PartialOrderTrace = PartialOrderTrace::new();

        let event_6 = Event::new("Work".into());
        partial_order_iso.add_event(&event_6);
        let event_3 = Event::new("Make Coffee".into());
        partial_order_iso.add_event(&event_3);
        let event_7 = Event::new("Wait for call from Internet provider".into());
        partial_order_iso.add_event(&event_7);
        let event_1 = Event::new("Wake up".into());
        partial_order_iso.add_event(&event_1);
        let event_5 = Event::new("Listen to Podcast".into());
        partial_order_iso.add_event(&event_5);
        let event_4 = Event::new("Breakfast".into());
        partial_order_iso.add_event(&event_4);
        let event_2 = Event::new("Brush teeth".into());
        partial_order_iso.add_event(&event_2);

        partial_order_iso.add_partial_relation(&event_4, &event_6);
        partial_order_iso.add_partial_relation(&event_1, &event_5);
        partial_order_iso.add_partial_relation(&event_2, &event_4);
        partial_order_iso.add_partial_relation(&event_1, &event_2);
        partial_order_iso.add_partial_relation(&event_1, &event_3);
        partial_order_iso.add_partial_relation(&event_3, &event_4);
        partial_order_iso.add_partial_relation(&event_5, &event_6);

        assert!(partial_order.is_isomorphic(&partial_order_iso, &Default::default(), &Default::default()))
    }

    #[test]
    fn deserialize_p_trace_test() {
        let p_trace: PartialOrderTrace = serde_json::from_str(SAMPLE_JSON_P_TRACE).unwrap();
        assert!(p_trace.event_map.len() == 7)
    }
}