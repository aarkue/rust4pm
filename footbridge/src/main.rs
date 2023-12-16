use std::{collections::HashSet, fs::File, io::BufReader, time::Instant};

use process_mining::{
    alphappp::full::{alphappp_discover_petri_net, AlphaPPPConfig},
    event_log::{activity_projection::EventLogActivityProjection, import_xes::import_xes_file},
};
use serde::{Deserialize, Serialize};
fn main() {
    // let file = File::open("/home/aarkue/dow/order-management.json").unwrap();
    // let reader = BufReader::new(file);
    // let ocel: OCEL = serde_json::from_reader(reader).unwrap();
    // println!("{:#?}",ocel.objects.len());

    // let export_file = File::create("/home/aarkue/dow/order-management_copy.json").unwrap();
    // let writer = BufWriter::new(export_file);
    // serde_json::to_writer(writer, &ocel).unwrap();
    // let mut attributes = Attributes::new();
    // attributes.add_to_attributes("test".into(), AttributeValue::String("Hello".into()));

    // attributes.add_to_attributes(
    //     "date-test".into(),
    //     AttributeValue::Date(DateTime::<Utc>::default()),
    // );

    // attributes.add_to_attributes("int-test".into(), AttributeValue::Int(42));

    // attributes.add_to_attributes(
    //     "date-test".into(),
    //     AttributeValue::Date(DateTime::<Utc>::default()),
    // );
    // attributes.add_to_attributes("float-test".into(), AttributeValue::Float(1.337));
    // attributes.add_to_attributes("boolean-test".into(), AttributeValue::Boolean(true));
    // attributes.add_to_attributes("id-test".into(), AttributeValue::ID(Uuid::new_v4()));
    // attributes.add_to_attributes(
    //     "list-test".into(),
    //     AttributeValue::List(vec![
    //         Attribute {
    //             key: "first".into(),
    //             value: AttributeValue::Int(1),
    //             own_attributes: None
    //         },
    //         Attribute {
    //             key: "first".into(),
    //             value: AttributeValue::Float(1.1),
    //             own_attributes: None
    //         },
    //         Attribute {
    //             key: "second".into(),
    //             value: AttributeValue::Int(2),
    //             own_attributes: None
    //         },
    //     ]),
    // );

    // let mut container_test_inner = Attributes::new();
    // container_test_inner.add_to_attributes("first".into(), AttributeValue::Int(1));
    // container_test_inner.add_to_attributes("second".into(), AttributeValue::Int(2));
    // container_test_inner.add_to_attributes("third".into(), AttributeValue::Int(3));
    // attributes.add_to_attributes(
    //     "container-test".into(),
    //     AttributeValue::Container(container_test_inner),
    // );
    // // let event: Event = Event { attributes };
    // // println!("Hello, world!");
    // // let json = serde_json::to_string_pretty(&event.attributes).unwrap();
    // // println!("{}", json);

    let log_name = "Sepsis Cases - Event Log.xes.gz";
    // let str = read_to_string(format!("/home/aarkue/doc/sciebo/alpha-revisit/{}", log_name).as_str()).unwrap();
    // let log = import_xes_str(&str, None);
    let log = import_xes_file(
        format!("/home/aarkue/doc/sciebo/alpha-revisit/{}", log_name).as_str(),
        None,
    );
    let log_proj: EventLogActivityProjection = (&log).into();

    // let res = alphappp_discover_with_auto_parameters(&log_proj);
    // println!("After; Best config: {:#?}",res.0);
    // add_start_end_acts_proj(&mut log_proj);
    // let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    // let dfg_sum: u64 = dfg.edges.values().sum();
    // let mean_dfg = dfg_sum as f32 / dfg.edges.len() as f32;
    let repair_thresh = 4.0;
    // // println!("Repair thresh: {}", repair_thresh * mean_dfg);
    // // let log_proj: EventLogActivityProjection = (&log).into();
    let now = Instant::now();
    let config = AlphaPPPConfig {
        balance_thresh: 0.5,
        fitness_thresh: 0.5,
        replay_thresh: 0.5,
        log_repair_skip_df_thresh_rel: repair_thresh,
        log_repair_loop_df_thresh_rel: repair_thresh,
        absolute_df_clean_thresh: 1,
        relative_df_clean_thresh: 0.01,
    };

    let (pn, algo_dur) = alphappp_discover_petri_net(&log_proj, config);
    println!("Duration: {} | {:?}", algo_dur.total, now.elapsed());
    // let res_name = format!(
    //     "res-{}-α+++|{:.1}|b{:.1}|t{:.1}|",
    //     log_name, repair_thresh, config.balance_thresh, config.fitness_thresh
    // );
    // let file = File::create(format!("{}.json", res_name)).unwrap();
    // export_petri_net_to_pnml(&pn, format!("{}.pnml", res_name).as_str());
    // let writer = BufWriter::new(file);
    // serde_json::to_writer_pretty(writer, &algo_dur).unwrap();

    // export_petri_net_to_pnml(&pn, "net.pnml");
    // println!("Discovered Petri Net: {:?}", pn.to_json());
    // let df_threshold = 10;
    // // log_proj = add_artificial_acts_for_skips(log_proj, df_threshold);
    // // log_proj = add_artificial_acts_for_loops(log_proj, df_threshold);
    // let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    // let dfg = filter_dfg(&dfg, 2,0.01);
    // let cnds: HashSet<(Vec<usize>, Vec<usize>)> = build_candidates(&dfg);
    // let filtered_cnds = prune_candidates(&cnds, 0.1, 0.8, &log_proj);
    // let mut cnds_strs: Vec<(Vec<String>, Vec<String>)> = cnds
    //     .iter()
    //     .map(|(a, b)| {
    //         (
    //             a.iter()
    //                 .map(|act| log_proj.activities[*act].clone())
    //                 .collect(),
    //             b.iter()
    //                 .map(|act| log_proj.activities[*act].clone())
    //                 .collect(),
    //         )
    //     })
    //     .collect();
    // for (a, b) in &cnds_strs {
    //     println!("{:?} => {:?}\n", a, b);
    // }
    // compare_candidates(&mut cnds_strs,"candidates-prom.json".to_string());
    // let file = File::create("candidates.json").unwrap();
    // let writer = BufWriter::new(file);
    // serde_json::to_writer_pretty(writer, &cnds_strs).unwrap();
    // println!("Number of candidates {:?}", cnds.len());
    // println!("After filtering {:?}", filtered_cnds.len());
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

    let diff: Vec<&(Vec<String>, Vec<String>)> =
        cnds_set.symmetric_difference(&other_cnds_set).collect();
    for (a, b) in &diff {
        if cnds_set.contains(&(a.clone(), b.clone())) {
            println!("Candidate not in java: {:?} => {:?}\n", a, b);
        } else {
            println!("Candidate not in cnds: {:?} => {:?}\n", a, b);
        }
    }
    println!("#Differences: {:?}", diff.len());
}
