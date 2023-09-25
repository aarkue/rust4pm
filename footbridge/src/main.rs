use std::{
    collections::HashSet,
    fs::File,
    io::BufReader,
    vec,
};

use pm_rust::{
    add_start_end_acts,
    alphappp::candidate_building::build_candidates,
    event_log::{
        activity_projection::EventLogActivityProjection,
        import_xes::import_log_xes,
    },
    Attribute, AttributeAddable, AttributeValue, Attributes, DateTime, Utc, Uuid,
};
use serde::{Deserialize, Serialize};
fn main() {
    let mut attributes = Attributes::new();
    attributes.add_to_attributes("test".into(), AttributeValue::String("Hello".into()));

    attributes.add_to_attributes(
        "date-test".into(),
        AttributeValue::Date(DateTime::<Utc>::default()),
    );

    attributes.add_to_attributes("int-test".into(), AttributeValue::Int(42));

    attributes.add_to_attributes(
        "date-test".into(),
        AttributeValue::Date(DateTime::<Utc>::default()),
    );
    attributes.add_to_attributes("float-test".into(), AttributeValue::Float(1.337));
    attributes.add_to_attributes("boolean-test".into(), AttributeValue::Boolean(true));
    attributes.add_to_attributes("id-test".into(), AttributeValue::ID(Uuid::new_v4()));
    attributes.add_to_attributes(
        "list-test".into(),
        AttributeValue::List(vec![
            Attribute {
                key: "first".into(),
                value: AttributeValue::Int(1),
            },
            Attribute {
                key: "first".into(),
                value: AttributeValue::Float(1.1),
            },
            Attribute {
                key: "second".into(),
                value: AttributeValue::Int(2),
            },
        ]),
    );

    let mut container_test_inner = Attributes::new();
    container_test_inner.add_to_attributes("first".into(), AttributeValue::Int(1));
    container_test_inner.add_to_attributes("second".into(), AttributeValue::Int(2));
    container_test_inner.add_to_attributes("third".into(), AttributeValue::Int(3));
    attributes.add_to_attributes(
        "container-test".into(),
        AttributeValue::Container(container_test_inner),
    );
    // let event: Event = Event { attributes };
    // println!("Hello, world!");
    // let json = serde_json::to_string_pretty(&event.attributes).unwrap();
    // println!("{}", json);

    let mut log =
        import_log_xes(&"/home/aarkue/dow/event_logs/BPI_Challenge_2020_request_for_payments.xes");
    add_start_end_acts(&mut log);
    let mut log_proj: EventLogActivityProjection = (&log).into();
    let df_threshold = 10;
    // log_proj = add_artificial_acts_for_skips(log_proj, df_threshold);
    // log_proj = add_artificial_acts_for_loops(log_proj, df_threshold);
    // let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let cnds = build_candidates(&log_proj);
    let mut cnds_strs: Vec<(Vec<String>, Vec<String>)> = cnds
        .iter()
        .map(|(a, b)| {
            (
                a.iter()
                    .map(|act| log_proj.activities[*act].clone())
                    .collect(),
                b.iter()
                    .map(|act| log_proj.activities[*act].clone())
                    .collect(),
            )
        })
        .collect();
    // for (a, b) in &cnds_strs {
    //     println!("{:?} => {:?}\n", a, b);
    // }
    compare_candidates(&mut cnds_strs,"candidates-prom.json".to_string());
    // let file = File::create("candidates.json").unwrap();
    // let writer = BufWriter::new(file);
    // serde_json::to_writer_pretty(writer, &cnds_strs).unwrap();
    println!("Number of candidates {:?}", cnds.len());
    // let reachable = get_reachable_bf(*log_proj.act_to_index.get(START_EVENT).unwrap(), &dfg, 1);
    // println!("Reachable: ");
    // reachable.iter().for_each(|r| {
    //     let path : Vec<String> = r.iter().map(|a| log_proj.activities[*a].clone()).collect();
    //     if path.last().unwrap().as_str() != END_EVENT {
    //         println!("# {:?}", r);
    //         println!("   > {:?}\n", path);
    //     }
    // });
}

pub fn compare_candidates(cnds: &mut Vec<(Vec<String>, Vec<String>)>, prom_cnds_json_path: String) {
    #[derive(Debug, Serialize, Deserialize)]
    struct JavaPair<P1, P2> {
        first: P1,
        second: P2,
    }
    let file = File::open(prom_cnds_json_path).unwrap();
    let reader = BufReader::new(file);
    let other_cnds_java: Vec<JavaPair<Vec<String>, Vec<String>>> =
        serde_json::from_reader(reader).unwrap();
        let mut other_cnds: Vec<(Vec<String>, Vec<String>)> = other_cnds_java
        .into_iter()
        .map(|jcnd| (jcnd.first, jcnd.second))
        .collect();
    cnds.iter_mut().for_each(|(a, b)| {
        a.sort();
        b.sort()
    });
    cnds.sort();
    other_cnds.iter_mut().for_each(|(a, b)| {
        a.sort();
        b.sort()
    });
    other_cnds.sort();

    println!("Rust: {:?}", cnds[0]);
    println!("Java: {:?}", other_cnds[0]);

    let cnds_set: HashSet<(Vec<String>, Vec<String>)> = cnds
        .into_iter()
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();

    let other_cnds_set: HashSet<(Vec<String>, Vec<String>)> = other_cnds
        .into_iter()
        .map(|(a, b)| (a.clone(), b.clone()))
        .collect();

    let diff : Vec<&(Vec<String>, Vec<String>)> = cnds_set.symmetric_difference(&other_cnds_set).collect();
    for (a, b) in &diff {
        if cnds_set.contains(&(a.clone(), b.clone())) {
            println!("Candidate not in java: {:?} => {:?}\n",a,b);
        }else{
            println!("Candidate not in cnds: {:?} => {:?}\n",a,b);
        }
    }
    println!("#Differences: {:?}", diff.len());
}
