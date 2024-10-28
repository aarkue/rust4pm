use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use serde_with::serde_as;
use crate::event_log::event_log_struct::EventLogClassifier;
use crate::EventLog;

/// Activity in a directly-follows graph.
type Activity = String;

/// A directly-follows graph of [`Activity`]s.
/// Graph containing a set of activities, a set of directly-follows relations, a set of start
/// activities, and a set of end activities.
/// Both, the number of occurrences of activities and of directly follows relations are annotated
/// with their frequency.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct DirectlyFollowsGraph {
    /// Activities
    pub activities: HashMap<Activity, u32>,
    /// Directly-follows relations
    #[serde_as(as = "Vec<(_, _)>")]
    pub directly_follows_relations: HashMap<(String, String), u32>,
    /// Start activities
    pub start_activities: HashSet<Activity>,
    /// End activities
    pub end_activities: HashSet<Activity>,
}

impl Default for DirectlyFollowsGraph {
    fn default() -> Self { Self::new() }
}

impl DirectlyFollowsGraph {
    /// Create new [`DirectlyFollowsGraph`] with no activities and directly-follows relations.
    pub fn new() -> Self {
        Self {
            activities: HashMap::new(),
            directly_follows_relations: HashMap::new(),
            start_activities: HashSet::new(),
            end_activities: HashSet::new(),
        }
    }

    pub fn create_from_eventlog(event_log: EventLog, classifier: EventLogClassifier) -> Self {
        let mut result = Self::new();

        let mut last_event_identity: Option<String> = None;
        event_log.traces.iter().for_each(|t| {
            t.events.iter().for_each(|e| {
                let curr_event_identity = classifier.get_class_identity(e);
                result.add_activity(curr_event_identity.clone(), 1);

                if last_event_identity.is_some() {
                    result.add_directly_follows_relation(
                        last_event_identity.clone().unwrap(),
                        curr_event_identity.clone(),
                        1,
                    )
                } else {
                    result.add_start_activity(curr_event_identity.clone());
                }

                last_event_identity = Some(curr_event_identity.clone());
            });
            if last_event_identity.is_some() {
                result.add_end_activity(last_event_identity.clone().unwrap());
            }
        });

        result
    }

    /// Serialize to JSON string.
    pub fn to_json(self) -> String { serde_json::to_string(&self).unwrap() }

    /// Add an activity with a frequency.
    ///
    /// If the activity already exists, the frequency count is added to the existing activity.
    pub fn add_activity(&mut self, activity: Activity, frequency: u32) {
        *self.activities.entry(activity).or_default() += frequency;
    }

    /// Adds an activity to the set of start activities.
    pub fn add_start_activity(&mut self, activity: Activity) {
        self.start_activities.insert(activity);
    }

    /// Adds an activity to the set of end activities.
    pub fn add_end_activity(&mut self, activity: Activity) {
        self.end_activities.insert(activity);
    }

    /// Checks if an activity is already contained in the directly-follows graph.
    pub fn contains_activity(&self, activity: &Activity) -> bool {
        self.activities.contains_key(activity)
    }

    /// Checks if an activity is a start activity in the directly-follows graph.
    pub fn is_start_activity(&self, activity: &Activity) -> bool {
        self.start_activities.contains(activity)
    }

    /// Checks if an activity is an end activity in the directly-follows graph.
    pub fn is_end_activity(&self, activity: &Activity) -> bool {
        self.end_activities.contains(activity)
    }

    /// Removes an activity from the directly-follows graph.
    pub fn remove_activity(&mut self, activity: &Activity) {
        let is_present = self.activities.remove(activity).is_some();

        // Removes the activity from the start and end activities if existing
        if is_present {
            self.start_activities.remove(activity);
            self.end_activities.remove(activity);
        }
    }

    /// Add a directly-follows relation with a frequency.
    ///
    /// If the directly-follows relation already exists, the frequency count is added to the
    /// existing directly-follows relation.
    pub fn add_directly_follows_relation(&mut self, from: Activity, to: Activity, frequency: u32) {
        *self.directly_follows_relations.entry((from, to)).or_default() += frequency;
    }

    /// Checks if a directly-follows relation is already contained in the directly-follows graph.
    pub fn contains_directly_follows_relation(&self, dfr: &(String, String)) -> bool {
        self.directly_follows_relations.contains_key(dfr)
    }

    /// Returns the ingoing activities of an activity in the directly-follows graph.
    pub fn ingoing_activities(&self, activity: &Activity) -> HashSet<&Activity> {
        self.directly_follows_relations
            .keys()
            .filter_map(|(x, y)| {
                if activity == y {
                    Some(x)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the outgoing activities of an activity in the directly-follows graph.
    pub fn outgoing_activities(&self, activity: &Activity) -> HashSet<&Activity> {
        self.directly_follows_relations
            .keys()
            .filter_map(|(x, y)| {
                if activity == x {
                    Some(y)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the ingoing directly-follows relations of an activity in the directly-follows graph.
    pub fn get_ingoing_directly_follows_relations(&self, activity: &Activity) -> HashSet<&(String, String)> {
        self.directly_follows_relations
            .keys()
            .filter_map(|arc| {
                if activity == &arc.1 {
                    Some(arc)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the outgoing directly-follows relations of an activity in the directly-follows graph.
    pub fn get_outgoing_directly_follows_relations(&self, activity: &Activity) -> HashSet<&(String, String)> {
        self.directly_follows_relations
            .keys()
            .filter_map(|arc| {
                if activity == &arc.0 {
                    Some(arc)
                } else {
                    None
                }
            })
            .collect()
    }

    #[cfg(feature = "graphviz-export")]
    /// Export directly-follows graph as a PNG image
    ///
    /// The PNG file is written to the specified filepath
    ///
    /// _Note_: This is an export method for __visualizing__ the directly-follows graph.
    ///
    /// Only available with the `graphviz-export` feature.
    pub fn export_png<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), std::io::Error> {
        super::image_export::export_dfg_image_png(self, path)
    }

    #[cfg(feature = "graphviz-export")]
    /// Export directly-follows graph as an SVG image.
    ///
    /// The SVG file is written to the specified filepath.
    ///
    /// _Note_: This is an export method for __visualizing__ the directly-follows graph.
    ///
    /// Only available with the `graphviz-export` feature.
    pub fn export_svg<P: AsRef<std::path::Path>>(&self, path: P) -> Result<(), std::io::Error> {
        super::image_export::export_dfg_image_svg(self, path)
    }
}

#[cfg(test)]
mod tests {
    pub const SAMPLE_JSON_DFG: &str = r#"
{
    "activities": {
        "Sleep": 13,
        "Cook": 3,
        "Work": 11,
        "Have fun": 9
    },
    "directly_follows_relations": [
        [
            ["Work","Sleep"],
            4
        ],
        [
            ["Have fun","Sleep"],
            9
        ],
        [
            ["Work","Have fun"],
            6
        ],
        [
            ["Cook","Have fun"],
            3
        ]
    ],
    "start_activities": [
        "Work",
        "Cook"
    ],
    "end_activities": [
        "Work",
        "Sleep"
    ]
}"#;

    #[cfg(feature = "graphviz-export")]
    use crate::dfg::image_export::export_dfg_image_png;
    #[cfg(feature = "graphviz-export")]
    use crate::dfg::image_export::export_dfg_image_svg;
    use crate::event_log::import_xes::{import_xes, import_xes_file, XESImportOptions};
    use super::*;

    #[test]
    fn directly_follows_graph() {
        let mut graph = DirectlyFollowsGraph::new();
        graph.add_activity("Work".into(), 11);
        graph.add_start_activity("Work".into());
        graph.add_end_activity("Work".into());

        graph.add_activity("Cook".into(), 3);
        graph.add_start_activity("Cook".into());

        graph.add_activity("Have fun".into(), 9);
        graph.add_directly_follows_relation("Work".into(), "Have fun".into(), 6);
        graph.add_directly_follows_relation("Cook".into(), "Have fun".into(), 3);

        graph.add_activity("Sleep".into(), 13);
        graph.add_directly_follows_relation("Work".into(), "Sleep".into(), 4);
        graph.add_directly_follows_relation("Have fun".into(), "Sleep".into(), 9);
        graph.add_end_activity("Sleep".into());

        let mut test_hashmap = HashMap::new();
        test_hashmap.insert("Work".into(), 11);
        test_hashmap.insert("Cook".into(), 3);
        test_hashmap.insert("Have fun".into(), 9);
        test_hashmap.insert("Sleep".into(), 13);
        assert_eq!(graph.activities, test_hashmap);
    }

    #[test]
    fn deserialize_dfg_test() {
        let dfg: DirectlyFollowsGraph = serde_json::from_str(SAMPLE_JSON_DFG).unwrap();
        assert!(dfg.activities.len() == 4);
        assert!(dfg.directly_follows_relations.len() == 4);
        assert!(dfg.start_activities.len() == 2);
        assert!(dfg.end_activities.len() == 2);
    }

    #[test]
    fn reading_dfg_from_event_log_bpi_2018() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("event_log")
            .join("tests")
            .join("test_data")
            .join("repairExample.xes");

        let log: EventLog = import_xes_file(
            path,
            XESImportOptions {
                ignore_log_attributes_except: Some(HashSet::default()),
                ignore_trace_attributes_except: Some(
                    vec!["concept:name".to_string()].into_iter().collect(),
                ),
                ignore_event_attributes_except: Some(
                    vec!["concept:name".to_string(), "time:timestamp".to_string()]
                        .into_iter()
                        .collect(),
                ),
                ..XESImportOptions::default()
            },
        ).unwrap();

        let classifier = log.classifiers.clone().unwrap().get(0).unwrap().clone();

        let graph = DirectlyFollowsGraph::create_from_eventlog(log, classifier);

        let path_output = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("event_log")
            .join("tests")
            .join("test_data")
            .join("repairExample.png");
        #[cfg(feature = "graphviz-export")]
        export_dfg_image_png(&graph, &path_output).unwrap();
        std::fs::remove_file(&path_output).unwrap();
    }
}