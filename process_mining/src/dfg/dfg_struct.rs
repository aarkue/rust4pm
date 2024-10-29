use crate::event_log::event_log_struct::EventLogClassifier;
use crate::EventLog;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

/// Activity in a directly-follows graph.
type Activity = String;

/// A directly-follows graph of [`Activity`]s.
/// Graph containing a set of activities, a set of directly-follows relations, a set of start
/// activities, and a set of end activities.
/// Both, the number of occurrences of activities and of directly follows relations are annotated
/// with their frequency.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct DirectlyFollowsGraph<'a> {
    /// Activities
    pub activities: HashMap<Activity, u32>,
    /// Directly-follows relations
    #[serde_as(as = "Vec<(_, _)>")]
    pub directly_follows_relations: HashMap<(Cow<'a, str>, Cow<'a, str>), u32>,
    /// Start activities
    pub start_activities: HashSet<Activity>,
    /// End activities
    pub end_activities: HashSet<Activity>,
}

impl Default for DirectlyFollowsGraph<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> DirectlyFollowsGraph<'a> {
    /// Create new [`DirectlyFollowsGraph`] with no activities and directly-follows relations.
    pub fn new() -> Self {
        Self {
            activities: HashMap::new(),
            directly_follows_relations: HashMap::new(),
            start_activities: HashSet::new(),
            end_activities: HashSet::new(),
        }
    }

    /// Construct a [`DirectlyFollowsGraph`] from an [`EventLog`] using the specified [`EventLogClassifier`] to derive the 'activity' names
    ///
    /// If there is no special classifier to be used, the default (`&EventLogClassifier::default()`) can also simply be passed in
    pub fn create_from_log(event_log: &EventLog, classifier: &EventLogClassifier) -> Self {
        let mut result = Self::new();
        event_log.traces.iter().for_each(|t| {
            let mut last_event_identity: Option<String> = None;
            t.events.iter().for_each(|e| {
                let curr_event_identity = classifier.get_class_identity(e);
                result.add_activity(curr_event_identity.clone(), 1);

                if let Some(last_ev_id) = last_event_identity.take() {
                    result.add_df_relation(last_ev_id.into(), curr_event_identity.clone().into(), 1)
                } else {
                    result.add_start_activity(curr_event_identity.clone());
                }

                last_event_identity = Some(curr_event_identity.clone());
            });
            if let Some(last_ev_id) = last_event_identity.take() {
                result.add_end_activity(last_ev_id);
            }
        });

        result
    }

    /// Serialize to JSON string.
    pub fn to_json(self) -> String {
        serde_json::to_string(&self).unwrap()
    }

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
    pub fn contains_activity<S: AsRef<str>>(&self, activity: S) -> bool {
        self.activities.contains_key(activity.as_ref())
    }

    /// Checks if an activity is a start activity in the directly-follows graph.
    pub fn is_start_activity<S: AsRef<str>>(&self, activity: S) -> bool {
        self.start_activities.contains(activity.as_ref())
    }

    /// Checks if an activity is an end activity in the directly-follows graph.
    pub fn is_end_activity<S: AsRef<str>>(&self, activity: S) -> bool {
        self.end_activities.contains(activity.as_ref())
    }

    /// Removes an activity from the directly-follows graph.
    pub fn remove_activity<S: AsRef<str>>(&mut self, activity: S) {
        let is_present = self.activities.remove(activity.as_ref()).is_some();

        // Removes the activity from the start and end activities if existing
        if is_present {
            self.start_activities.remove(activity.as_ref());
            self.end_activities.remove(activity.as_ref());

            self.directly_follows_relations
                .retain(|(from, to), _| {
                    from != activity.as_ref() && to != activity.as_ref()
                });
        }
    }

    /// Add a directly-follows relation with a frequency.
    ///
    /// If the directly-follows relation already exists, the frequency count is added to the
    /// existing directly-follows relation.
    pub fn add_df_relation(&mut self, from: Cow<'a, str>, to: Cow<'a, str>, frequency: u32) {
        *self
            .directly_follows_relations
            .entry((from.clone(), to.clone()))
            .or_default() += frequency;
    }

    /// Checks if a directly-follows relation is already contained in the directly-follows graph.
    pub fn contains_df_relation<S: Into<Cow<'a, str>>>(&self, (a, b): (S, S)) -> bool {
        self.directly_follows_relations
            .contains_key(&(a.into(), b.into()))
    }

    /// Returns the ingoing activities of an activity in the directly-follows graph.
    pub fn ingoing_activities<S: Into<Cow<'a, str>>>(&self, activity: S) -> HashSet<&Cow<'a, str>> {
        let a = activity.into();
        self.directly_follows_relations
            .keys()
            .filter_map(|(x, y)| if &a == y { Some(x) } else { None })
            .collect()
    }

    /// Returns the outgoing activities of an activity in the directly-follows graph.
    pub fn outgoing_activities<S: Into<Cow<'a, str>>>(
        &self,
        activity: S,
    ) -> HashSet<&Cow<'a, str>> {
        let a = activity.into();
        self.directly_follows_relations
            .keys()
            .filter_map(|(x, y)| if &a == x { Some(y) } else { None })
            .collect()
    }

    /// Returns the ingoing directly-follows relations of an activity in the directly-follows graph.
    pub fn get_ingoing_df_relations<S: Into<Cow<'a, str>>>(
        &self,
        activity: S,
    ) -> HashSet<&(Cow<'a, str>, Cow<'a, str>)> {
        let a = activity.into();
        self.directly_follows_relations
            .keys()
            .filter_map(|arc| if a == arc.1 { Some(arc) } else { None })
            .collect()
    }

    /// Returns the outgoing directly-follows relations of an activity in the directly-follows graph.
    pub fn get_outgoing_df_relations<S: Into<Cow<'a, str>>>(
        &self,
        activity: S,
    ) -> HashSet<&(Cow<'a, str>, Cow<'a, str>)> {
        let a = activity.into();
        self.directly_follows_relations
            .keys()
            .filter_map(|arc| if &a == &arc.0 { Some(arc) } else { None })
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

    use super::*;
    #[cfg(feature = "graphviz-export")]
    use crate::dfg::image_export::export_dfg_image_png;

    use crate::event_log::import_xes::{import_xes_file, XESImportOptions};
    use crate::utils::test_utils::get_test_data_path;

    #[test]
    fn directly_follows_graph() {
        let mut graph = DirectlyFollowsGraph::new();
        graph.add_activity("Work".into(), 11);
        graph.add_start_activity("Work".into());
        graph.add_end_activity("Work".into());

        graph.add_activity("Cook".into(), 3);
        graph.add_start_activity("Cook".into());

        graph.add_activity("Have fun".into(), 9);
        graph.add_df_relation("Work".into(), "Have fun".into(), 6);
        graph.add_df_relation("Cook".into(), "Have fun".into(), 3);

        graph.add_activity("Sleep".into(), 13);
        graph.add_df_relation("Work".into(), "Sleep".into(), 4);
        graph.add_df_relation("Have fun".into(), "Sleep".into(), 9);
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
        let dfg: DirectlyFollowsGraph<'_> = serde_json::from_str(SAMPLE_JSON_DFG).unwrap();
        assert!(dfg.activities.len() == 4);
        assert!(dfg.directly_follows_relations.len() == 4);
        assert!(dfg.start_activities.len() == 2);
        assert!(dfg.end_activities.len() == 2);
    }

    #[test]
    fn reading_dfg_from_event_log_repair_example() {
        let path = get_test_data_path().join("xes").join("RepairExample.xes");

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
        )
            .unwrap();

        let classifier = log.classifiers.as_ref().and_then(|c| c.first()).unwrap();

        let graph = DirectlyFollowsGraph::create_from_log(&log, classifier);

        #[cfg(feature = "graphviz-export")]
        {
            let path_output = get_test_data_path()
                .join("export")
                .join("RepairExample-DFG.png");
            export_dfg_image_png(&graph, &path_output).unwrap();
        }
    }

    #[test]
    fn test_dfg_from_sepsis_log() {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");

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
        )
            .unwrap();

        let graph = DirectlyFollowsGraph::create_from_log(&log, &EventLogClassifier::default());

        #[cfg(feature = "graphviz-export")]
        {
            let path_output = get_test_data_path().join("export").join("Sepsis-DFG.png");
            export_dfg_image_png(&graph, &path_output).unwrap();
        }
    }

    #[test]
    fn test_dfg_from_an1_example() {
        let path = get_test_data_path().join("xes").join("AN1-example.xes");

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
        )
            .unwrap();

        let graph = DirectlyFollowsGraph::create_from_log(&log, &EventLogClassifier::default());

        assert_eq!(graph.activities.len(), 6);
        assert_eq!(graph.directly_follows_relations.len(), 16);
        assert_eq!(graph.get_ingoing_df_relations("a").len(), 0);
        assert_eq!(graph.get_outgoing_df_relations("a").len(), 3);
        assert_eq!(
            graph
                .outgoing_activities("b")
                .iter()
                .map(|a| a.as_ref())
                .collect::<HashSet<_>>(),
            vec!["b", "e", "c"].into_iter().collect()
        );
        assert!(graph.contains_df_relation(("a", "b")));
        assert!(graph.contains_df_relation(("b", "b")));
        assert!(graph.contains_df_relation(("c", "f")));
        assert!(!graph.contains_df_relation(("f", "c")));

        #[cfg(feature = "graphviz-export")]
        {
            let path_output = get_test_data_path().join("export").join("AN1-DFG.png");
            export_dfg_image_png(&graph, &path_output).unwrap();
        }
    }
}
