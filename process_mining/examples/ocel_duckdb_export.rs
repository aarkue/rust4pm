use std::{collections::HashSet, env::args, path::PathBuf};

use process_mining::core::event_data::object_centric::{
    ocel_sql::export_ocel_duckdb_to_path, ocel_xml::xml_ocel_import::import_ocel_xml_file,
};

pub fn main() {
    let path_opt = args().nth(1);
    if let Some(mut path) = path_opt.map(PathBuf::from) {
        let mut ocel = import_ocel_xml_file(&path);
        // Including invalid E2O relations (i.e., to objects that do not exist) can cause corrupted or incomplete SQL exports
        // Thus, we filter the E2O relations to only keep valid ones
        let all_obj_ids: HashSet<_> = ocel.objects.iter().map(|o| &o.id).collect();
        for e in &mut ocel.events {
            e.relationships
                .retain(|r| all_obj_ids.contains(&r.object_id));
        }
        // Export
        path.set_file_name(format!(
            "{}.duckdb",
            path.file_name()
                .and_then(|p| p.to_str())
                .unwrap_or_default()
        ));
        export_ocel_duckdb_to_path(&ocel, &path).unwrap();
    }
}
