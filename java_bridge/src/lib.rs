#![allow(non_snake_case)]

use jni::{
    objects::{JByteArray, JClass, JString},
    JNIEnv,
};
use std::time::Instant;

use jni_fn::jni_fn;
use pm_rust::{
    add_start_end_acts, export_log, export_log_to_byte_vec, import_log, import_log_from_byte_vec,
    EventLog, EventLogActivityProjection,
};

#[jni_fn("HelloProcessMining")]
pub fn addArtificialActs<'local>(
    env: JNIEnv<'local>,
    _: JClass,
    data: JByteArray<'local>,
) -> JByteArray<'local> {
    let mut now = Instant::now();
    let d: Vec<u8> = env.convert_byte_array(&data).unwrap();
    println!(
        "Got byte array of size {:.2} in {:.2?}",
        d.len(),
        now.elapsed()
    );
    now = Instant::now();
    let mut log: EventLog = import_log_from_byte_vec(&d);

    println!("Time until struct ready: {:.2?}", now.elapsed());
    now = Instant::now();
    log.attributes.insert(
        "name".to_string(),
        "Transformed Rust Log from byte[]".into(),
    );
    println!("Time until into EventLog: {:.2?}", now.elapsed());
    now = Instant::now();
    add_start_end_acts(&mut log);
    println!("Time until start/end added: {:.2?}", now.elapsed());
    now = Instant::now();
    let log_projection: EventLogActivityProjection<usize> = log.into();
    let log_again: EventLog = log_projection.into();
    println!("Time until into/from: {:.2?}", now.elapsed());
    now = Instant::now();
    let export_vec = export_log_to_byte_vec(&log_again);
    println!("ExportVec to byte array: {:.2?}", now.elapsed());
    return env.byte_array_from_slice(&export_vec).unwrap();
}

// #[jni_fn("HelloProcessMining")]
// pub fn addArtificialActsAvro<'local>(
//     env: JNIEnv<'local>,
//     _: JClass,
//     data: JByteArray<'local>,
// ) -> JByteArray<'local> {
//     let mut now = Instant::now();
//     let d: Vec<u8>  = env.convert_byte_array(data).unwrap();
//     println!("Got byte array of size {:.2} in {:.2?}",d.len(),now.elapsed());
//     now = Instant::now();
//     let mut log: EventLog = import_log_from_byte_vec_avro(&d).unwrap();
// }

#[jni_fn("HelloProcessMining")]
pub fn addArtificialActsUsingFiles<'local>(
    mut env: JNIEnv<'local>,
    _: JClass,
    importPath: JString<'local>,
    exportPath: JString<'local>,
) -> JString<'local> {
    let mut now = Instant::now();
    let import_path: String = env
        .get_string(&importPath)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let export_path: String = env
        .get_string(&exportPath)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    println!("Time until paths ready: {:.2?}", now.elapsed());
    now = Instant::now();
    let mut log: EventLog = import_log(import_path);
    println!("Time until into EventLog: {:.2?}", now.elapsed());
    now = Instant::now();
    log.attributes
        .insert("name".to_string(), "Transformed Rust Log from file".into());
    add_start_end_acts(&mut log);
    println!("Time until start/end added: {:.2?}", now.elapsed());
    now = Instant::now();
    let log_projection: EventLogActivityProjection<usize> = log.into();
    let log_again: EventLog = log_projection.into();
    println!("Time until into/from: {:.2?}", now.elapsed());
    export_log(export_path.clone(), &log_again);
    return env.new_string(export_path).unwrap();
}
