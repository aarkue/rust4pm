use std::vec;

use pm_rust::{
    event_log::{import_xes::import_log_xes, activity_projection::{EventLogActivityProjection, ActivityProjectionDFG}}, Attribute, AttributeAddable, AttributeValue, Attributes,
    DateTime, Utc, Uuid, alphappp::log_repair::{get_reachable_bf, add_artificial_acts_for_skips, add_artificial_acts_for_loops}, START_EVENT, add_start_end_acts, END_EVENT,
};
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
    let mut log_proj : EventLogActivityProjection = (&log).into();
    let df_threshold = 10;
    log_proj = add_artificial_acts_for_skips(log_proj, df_threshold);
    log_proj = add_artificial_acts_for_loops(log_proj, df_threshold);
    let dfg = ActivityProjectionDFG::from_event_log_projection(&log_proj);
    let reachable = get_reachable_bf(*log_proj.act_to_index.get(START_EVENT).unwrap(), &dfg, 1);
    println!("Reachable: ");
    reachable.iter().for_each(|r| {
        let path : Vec<String> = r.iter().map(|a| log_proj.activities[*a].clone()).collect();
        if path.last().unwrap().as_str() != END_EVENT {
            println!("# {:?}", r);
            println!("   > {:?}\n", path);
        }
    });

}
