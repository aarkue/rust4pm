//! Splitting Event Logs in Multiple Sublogs
use rand::distr::{Distribution, Uniform};
use rand::prelude::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use std::collections::HashSet;

use crate::core::event_data::case_centric::{EventLogClassifier, Trace};
use crate::core::EventLog;

///
/// Picks a randomized number of subsets to distribute a set of activities to and computes then a
/// distribution of the activities among the subsets such that no subset is empty.
///
/// # Arguments
///
/// * `activity_set`: A set of all activities that should be distributed among n sets.
/// * `max_num_of_splits`: The maximum number of sets to split the set of activities into.
///
/// Returns a `Vec`<`HashSet`<`&str`>> which is a distribution of activities among the randomized
/// size n of subsets in \[2,`max_num_of_splits`\].
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use process_mining::core::event_data::case_centric::utils::event_log_splitter::random_activity_split_max_bins;
///
/// // Create the set of activities
/// let mut activities = HashSet::new();
/// activities.insert("Admission IC");
/// activities.insert("ER Sepsis Triage");
/// activities.insert("IV Antibiotics");
/// activities.insert("Release A");
/// activities.insert("Release B");
/// activities.insert("Admission NC");
///
/// // Splits the activities randomly into 2, 3, or 4 sets.
/// let split_sets: Vec<HashSet<&str>> = random_activity_split_max_bins(&activities, 4);
/// ```
pub fn random_activity_split_max_bins<'a>(
    activity_set: &'a HashSet<&str>,
    max_num_of_splits: usize,
) -> Vec<HashSet<&'a str>> {
    // Sanity checks:
    // If max_num_of_splits is 1, return the distribution as the original activities to be mapped
    // all on one set, else if max_num_of_splits > activity_set.len() set the number of subsets
    // to the number of activities such that every activity is in its own subset
    if max_num_of_splits < 2 {
        let result: Vec<HashSet<&str>> = vec![activity_set.clone()];

        return result;
    } else if max_num_of_splits > activity_set.len() {
        let mut result = Vec::new();
        activity_set.iter().for_each(|&activity| {
            result.push(HashSet::from([activity]));
        });

        return result;
    }

    // Otherwise, randomly find some number of subsets to distribute the activities
    let mut rng: ThreadRng = rand::rng();
    let num_split_event_logs: usize = rng.random_range(2..max_num_of_splits);

    // Distribute the activities into the chosen number of subsets
    random_activity_split(activity_set, num_split_event_logs)
}

///
/// Distributes a set of activities over a given number of subsets such that no subset is empty.
///
/// # Arguments
///
/// * `activity_set`: A set of all activities that should be distributed among n sets.
/// * `num_of_splits`: Number of subsets to distribute the activities to.
///
/// Returns: Vec<`HashSet`<&str>> which is a distribution of strings over several subsets.
///
/// # Examples
///
/// ```
/// use process_mining::core::event_data::case_centric::utils::event_log_splitter::random_activity_split;
/// use std::collections::HashSet;
///
/// // Create the set of activities
/// let activities = HashSet::from([
///     "Admission IC",
///     "ER Sepsis Triage",
///     "IV Antibiotics",
///     "Release A",
///     "Release B",
///     "Admission NC",
/// ]);
///
/// // Splits the activities into exactly three sets with no set being empty and random distribution.
/// let split_sets: Vec<HashSet<&str>> = random_activity_split(&activities, 3);
/// ```
pub fn random_activity_split<'a>(
    activity_set: &'a HashSet<&str>,
    num_of_splits: usize,
) -> Vec<HashSet<&'a str>> {
    // Initialize a random number generator
    let mut rng: ThreadRng = rand::rng();

    // Initialize the sets to distribute the activities to
    let mut activity_split_sets: Vec<HashSet<&str>> = Vec::with_capacity(num_of_splits);
    for _ in 0..num_of_splits {
        activity_split_sets.push(HashSet::new());
    }

    // Initialize a uniform distribution to randomly pick an index of the set to add an activity to
    let uniform_dist: Uniform<usize> = Uniform::new(0, num_of_splits).unwrap();

    // Randomize the initial ordering of the activities such that the following round-robin
    // procedure is non-deterministic
    let mut vec: Vec<&str> = activity_set.iter().copied().collect::<Vec<&str>>();
    vec.shuffle(&mut rng);

    // Round-robin to cover all first elements
    // Afterward, randomly distribute the remaining elements
    for (pos, activity) in vec.iter().enumerate() {
        if pos < activity_split_sets.len() {
            activity_split_sets[pos].insert(activity);
        } else {
            activity_split_sets[uniform_dist.sample(&mut rng)].insert(activity);
        }
    }

    activity_split_sets
}

/// An `ActivityBasedEventLogSplitter` is used to split an event log into several event logs using
/// a given distribution of activities.
///
/// # Examples
/// For the following example, it is required to have the sepsis case event log setup on your device.
/// ```
/// use process_mining::core::event_data::case_centric::utils::event_log_splitter::ActivityBasedEventLogSplitter;
/// use process_mining::core::event_data::case_centric::{
///     utils::event_log_splitter::random_activity_split,
///     xes::{import_xes_file, XESImportOptions},
/// };
/// use process_mining::test_utils::get_test_data_path;
/// use std::collections::HashSet;
///
/// let path = get_test_data_path()
///     .join("xes")
///     .join("Sepsis Cases - Event Log.xes.gz");
/// let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
///
/// let activities = HashSet::from([
///     "Admission IC",
///     "ER Sepsis Triage",
///     "IV Antibiotics",
///     "Release A",
///     "Release B",
///     "Admission NC",
///     "CRP",
///     "IV Liquid",
///     "Release C",
///     "Release D",
///     "ER Registration",
///     "ER Triage",
///     "LacticAcid",
///     "Leucocytes",
///     "Release E",
///     "Return ER",
/// ]);
///
/// let split_sets: Vec<HashSet<&str>> = random_activity_split(&activities, 4);
/// assert_eq!(split_sets.len(), 4);
/// let splitter = ActivityBasedEventLogSplitter::new(&log, &split_sets);
/// assert!(splitter.check_split_set_validity());
/// let result_event_logs = &splitter.split();
/// for event_log in result_event_logs {
///     assert_eq!(event_log.traces.len(), 1050);
/// }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ActivityBasedEventLogSplitter<'a> {
    /// The activity sets to split by
    pub activity_split_sets: &'a Vec<HashSet<&'a str>>,
    /// The number of event logs that we split to
    pub num_split_event_logs: usize,
    /// The input event log to be split
    pub event_log: &'a EventLog,
}

impl<'a> ActivityBasedEventLogSplitter<'a> {
    /// Creates a new `ActivityBasedEventLogSplitter` that can be used to split an event log into
    /// several sub event logs by the given activities. The number of sets to be obtained is derived
    /// from the number of sets in the `activity_split_sets`.
    ///
    /// # Arguments
    ///
    /// * `event_log`: An event log to be split into several sub event logs.
    /// * `activity_split_sets`: A distribution of activities among different sets which should be
    ///   disjunct.
    ///
    /// Returns: A new `ActivityBasedEventLogSplitter`
    ///
    pub fn new(event_log: &'a EventLog, activity_split_sets: &'a Vec<HashSet<&str>>) -> Self {
        Self {
            activity_split_sets,
            num_split_event_logs: activity_split_sets.len(),
            event_log,
        }
    }

    ///
    /// Function to check whether the given activity sets are disjunct or not.
    ///
    /// Returns: A `bool` whether the activity sets are disjunct
    ///
    pub fn check_split_set_validity(&self) -> bool {
        // Stores all activities in a new hashset and keeps track of the expected size
        let mut all_activity_set: HashSet<&str> = HashSet::new();
        let mut expected_size: usize = 0;

        // If there are duplicates, the size of the intermediate hashset will be smaller than
        // the expected size due to the duplicate not being listed twice in a hashset
        for set in self.activity_split_sets {
            all_activity_set.extend(set);
            expected_size += set.len();

            if all_activity_set.len() != expected_size {
                return false;
            }
        }

        // If no duplicate found
        true
    }

    ///
    /// Identifies the index of the list containing of activity subsets that an activity belongs to.
    ///
    /// # Arguments
    ///
    /// * `activity`: An activity label.
    ///
    /// Returns: The `usize` index of the split set that the activity belongs to in the list of
    /// activity sets to be split on.
    ///
    pub fn find_activity_set(&self, activity: &str) -> usize {
        for (pos, set) in self.activity_split_sets.iter().enumerate() {
            if set.contains(activity) {
                return pos;
            }
        }

        self.num_split_event_logs
    }

    ///
    /// Splits the event log in several event logs based on the activity split sets. Empty traces
    /// are kept track of, and they are added to each event log.
    ///
    /// Returns: A list of sub event logs that contains only activity labels as corresponding to
    /// the initial specification
    ///
    pub fn split(&self) -> Vec<EventLog> {
        // Set up the result event log list
        let mut result: Vec<EventLog> = Vec::with_capacity(self.num_split_event_logs);

        // Fill it with empty event logs that, however, share the same attributes as the original
        // event log
        for _ in 0..self.num_split_event_logs {
            result.push(self.event_log.clone_without_traces());
        }

        // Based on the name classifier, we distinguish all events and assign them to the
        // corresponding sub event log
        // Note: If the input is an 'invalid' split, the method is greedy and assigns to the first
        // matching event log
        let name_classifier: EventLogClassifier = EventLogClassifier::default();
        for trace in &self.event_log.traces {
            // Create the empty trace for all event logs that holds the same trace attributes
            for event_log in result.iter_mut().take(self.num_split_event_logs) {
                let new_trace: Trace = trace.clone_without_events();
                event_log.traces.push(new_trace);
            }

            // Distribute the event based on their activity
            for event in &trace.events {
                let activity_label: String = name_classifier.get_class_identity(event);
                let split_pos: usize = self.find_activity_set(&activity_label);

                result[split_pos]
                    .traces
                    .last_mut()
                    .unwrap()
                    .events
                    .push(event.clone());
            }
        }

        result
    }
}

/// An `RandomEventLogSplitter` is used to split an event log into several event logs by randomly
/// assigning events to the sub event logs.
///
/// # Examples
/// For the following example, it is required to have the sepsis case event log setup on your device.
/// ```
/// use std::collections::HashSet;
///
/// use process_mining::{
///     core::event_data::case_centric::{
///         utils::event_log_splitter::{
///             random_activity_split, ActivityBasedEventLogSplitter, RandomEventLogSplitter,
///         },
///         xes::{import_xes_file, XESImportOptions},
///     },
///     test_utils::get_test_data_path,
/// };
/// let path = get_test_data_path()
///     .join("xes")
///     .join("Sepsis Cases - Event Log.xes.gz");
/// let log = import_xes_file(&path, XESImportOptions::default()).unwrap();
/// let activities = HashSet::from([
///     "Admission IC",
///     "ER Sepsis Triage",
///     "IV Antibiotics",
///     "Release A",
///     "Release B",
///     "Admission NC",
///     "CRP",
///     "IV Liquid",
///     "Release C",
///     "Release D",
///     "ER Registration",
///     "ER Triage",
///     "LacticAcid",
///     "Leucocytes",
///     "Release E",
///     "Return ER",
/// ]);
/// let mut splitter = RandomEventLogSplitter::new(&log, 4);
/// let result_event_logs = &splitter.split();
/// for event_log in result_event_logs {
///     assert_eq!(event_log.traces.len(), 1050);
/// }
///
/// let split_sets: Vec<HashSet<&str>> = random_activity_split(&activities, 4);
/// assert_eq!(split_sets.len(), 4);
/// let splitter = ActivityBasedEventLogSplitter::new(&log, &split_sets);
/// assert!(splitter.check_split_set_validity());
/// let result_event_logs = &splitter.split();
/// for event_log in result_event_logs {
///     assert_eq!(event_log.traces.len(), 1050);
/// }
/// ```
#[derive(Debug, Clone)]
pub struct RandomEventLogSplitter<'a> {
    num_split_event_logs: usize,
    event_log: &'a EventLog,
    rng: ThreadRng,
}

impl<'a> RandomEventLogSplitter<'a> {
    ///
    /// Creates a new `RandomEventLogSplitter` that can be used to split an event log into
    /// several sub event logs randomly. Therefore, the number of expected sub event logs need to
    /// be specified. A thread internal random number generator is automatically generated for
    /// the struct.
    ///
    /// # Arguments
    ///
    /// * `event_log`: An event log to be split into several sub event logs.
    /// * `num_split_event_logs`: The number of event logs to split into.
    ///
    /// Returns: A new `RandomEventLogSplitter`
    ///
    pub fn new(event_log: &'a EventLog, num_split_event_logs: usize) -> Self {
        Self {
            event_log,
            num_split_event_logs,
            rng: rand::rng(),
        }
    }

    ///
    /// Splits the event log in several event logs randomly. Empty traces are kept track of, and
    /// they are added to each event log.
    ///
    /// Returns: A list of sub event logs that have the specified size and that have their events
    /// assigned randomly for each trace
    ///
    pub fn split(&mut self) -> Vec<EventLog> {
        // Creates a uniform distribution used to randomly choose the event log to assign an event to.
        let uniform_distribution: Uniform<usize> =
            Uniform::new(0, self.num_split_event_logs).unwrap();

        // Initializes the result list of sub event logs
        let mut result: Vec<EventLog> = Vec::with_capacity(self.num_split_event_logs);

        // Creates trace-empty clones of the event log
        for _ in 0..self.num_split_event_logs {
            result.push(self.event_log.clone_without_traces());
        }

        // Randomly assigns for each trace the events to the sub event logs
        for trace in &self.event_log.traces {
            // Create the empty trace for all event logs that holds the same trace attributes
            for res in &mut result {
                let new_trace = trace.clone_without_events();
                res.traces.push(new_trace);
            }

            // Randomly distribute the events
            for event in &trace.events {
                let split_pos = uniform_distribution.sample(&mut self.rng);

                result[split_pos]
                    .traces
                    .last_mut()
                    .unwrap()
                    .events
                    .push(event.clone());
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::{
        core::event_data::case_centric::{
            utils::event_log_splitter::{random_activity_split, ActivityBasedEventLogSplitter},
            xes::{import_xes_file, XESImportOptions},
        },
        test_utils::get_test_data_path,
    };

    #[test]
    fn test_activity_based_event_log_splitter() {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();

        let mut split_sets: Vec<HashSet<&str>> = Vec::new();

        let mut set_1 = HashSet::new();
        set_1.insert("Admission IC");
        set_1.insert("ER Sepsis Triage");
        set_1.insert("IV Antibiotics");
        set_1.insert("Release A");
        set_1.insert("Release B");

        split_sets.push(set_1);

        let mut set_2 = HashSet::new();
        set_2.insert("Admission NC");
        set_2.insert("CRP");
        set_2.insert("IV Liquid");
        set_2.insert("Release C");
        set_2.insert("Release D");

        split_sets.push(set_2);

        let mut set_3 = HashSet::new();
        set_3.insert("ER Registration");
        set_3.insert("ER Triage");
        set_3.insert("LacticAcid");
        set_3.insert("Leucocytes");
        set_3.insert("Release E");
        set_3.insert("Return ER");

        split_sets.push(set_3);

        let splitter = ActivityBasedEventLogSplitter::new(&log, &split_sets);
        assert!(splitter.check_split_set_validity());

        let result_event_logs = &splitter.split();
        for event_log in result_event_logs {
            assert_eq!(event_log.traces.len(), 1050);
        }

        let mut counts: Vec<usize> = Vec::with_capacity(3);
        for event_log in result_event_logs.iter().take(3) {
            counts.push(
                event_log
                    .traces
                    .iter()
                    .map(|trace| trace.events.len())
                    .sum::<usize>(),
            )
        }

        assert_eq!(counts[0], 2716);
        assert_eq!(counts[1], 5246);
        assert_eq!(counts[2], 7252);
    }

    #[test]
    fn test_random_activity_split() {
        let path = get_test_data_path()
            .join("xes")
            .join("Sepsis Cases - Event Log.xes.gz");
        let log = import_xes_file(&path, XESImportOptions::default()).unwrap();

        let mut activities = HashSet::new();

        activities.insert("Admission IC");
        activities.insert("ER Sepsis Triage");
        activities.insert("IV Antibiotics");
        activities.insert("Release A");
        activities.insert("Release B");
        activities.insert("Admission NC");
        activities.insert("CRP");
        activities.insert("IV Liquid");
        activities.insert("Release C");
        activities.insert("Release D");
        activities.insert("ER Registration");
        activities.insert("ER Triage");
        activities.insert("LacticAcid");
        activities.insert("Leucocytes");
        activities.insert("Release E");
        activities.insert("Return ER");

        let split_sets: Vec<HashSet<&str>> = random_activity_split(&activities, 4);
        assert_eq!(split_sets.len(), 4);

        let mut summed_set_size = 0;
        split_sets.iter().for_each(|s| {
            assert!(!s.is_empty());
            summed_set_size += s.len();
        });
        assert_eq!(summed_set_size, activities.len());

        let splitter = ActivityBasedEventLogSplitter::new(&log, &split_sets);
        assert!(splitter.check_split_set_validity());

        let result_event_logs = &splitter.split();
        for event_log in result_event_logs {
            assert_eq!(event_log.traces.len(), 1050);
        }
    }
}
