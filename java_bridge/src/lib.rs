#![allow(non_snake_case)]

use jni::{
    objects::{JClass, JString},
    JNIEnv,
};
use std::time::Instant;

use jni_fn::jni_fn;
use pm_rust::{add_start_end_acts, export_log, import_log, EventLog};

#[jni_fn("HelloProcessMining")]
pub fn addArtificialActs<'local>(
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
    println!("Time until path ready: {:.2?}", now.elapsed());
    now = Instant::now();
    let mut log: EventLog = import_log(import_path);
    println!("Time until struct ready: {:.2?}", now.elapsed());
    log.logName = "Rust Log".into();
    now = Instant::now();
    println!("Time until into EventLog: {:.2?}", now.elapsed());
    now = Instant::now();
    add_start_end_acts(&mut log);
    println!("Time until start/end added: {:.2?}", now.elapsed());
    // now = Instant::now();
    export_log(export_path.clone(), &log);
    return env.new_string(export_path).unwrap();
}