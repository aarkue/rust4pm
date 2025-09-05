use std::{collections::HashSet, env::args, path::PathBuf};

use process_mining::{import_ocel_xml_file, ocel::sql::export_ocel_duckdb_to_path};

pub fn main() {
    let path_opt = args().nth(1);
    if let Some(path) = path_opt.map(PathBuf::from) {
        let mut ocel = import_ocel_xml_file(&path);
        // Including invalid E2O relations (i.e., to objects that do not exist) can cause corrupted or incomplete SQL exports
        // Thus, we filter the E2O relations to only keep valid ones
        let all_obj_ids: HashSet<_> = ocel.objects.iter().map(|o| &o.id).collect();
        for e in &mut ocel.events {
            e.relationships
                .retain(|r| all_obj_ids.contains(&r.object_id));
        }
        let export_path = path.with_file_name(format!(
            "{}.duck",
            path.file_name().and_then(|n| n.to_str()).unwrap_or("ocel")
        ));
        export_ocel_duckdb_to_path(&ocel, &export_path).unwrap();
    }
}
