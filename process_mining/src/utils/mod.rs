/// Helper utils regarding XML import/export
pub mod xml_utils;

#[cfg(test)]
pub mod test_utils {
    use std::path::PathBuf;

    pub fn get_test_data_path() -> PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data")
    }
}
