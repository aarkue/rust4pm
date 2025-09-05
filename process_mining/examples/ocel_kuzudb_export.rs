use std::{
    env::args,
    fs::{create_dir_all, remove_file},
    path::PathBuf,
    time::Instant,
};

use process_mining::{
    import_ocel_xml_file,
    ocel::{graph_db::ocel_kuzudb::export_ocel_to_kuzudb_typed, linked_ocel::IndexLinkedOCEL},
};

fn main() {
    let base_path_opt = args().nth(1);
    if let Some(base_path) = base_path_opt.map(PathBuf::from) {
        let export_path = base_path.join("kuzu");
        create_dir_all(&export_path).expect("Could not create export folder (`kuzu`) base path");
        for p in [
            "order-management.xml",
            "ocel2-p2p.xml",
            "ContainerLogistics.xml",
            "bpic2017-o2o-workflow-qualifier-index.xml",
        ] {
            println!("== {p} ==");
            let now = Instant::now();
            let ocel = import_ocel_xml_file(base_path.join(p));
            println!("Import OCEL XML took {:?}", now.elapsed());
            let now = Instant::now();
            let locel = IndexLinkedOCEL::from(ocel);
            println!("Linking OCEL took {:?}", now.elapsed());
            let now = Instant::now();
            let file_path = export_path.join(format!("{p}.kuzu"));
            // Remove file (if it already exists)
            let _ = remove_file(&file_path);
            export_ocel_to_kuzudb_typed(&file_path, &locel).unwrap();
            println!("Kuzu export took {:?}", now.elapsed());
        }
    }
}
