/// Helper utils regarding XML import/export
pub mod xml_utils;

#[cfg(test)]
/// Used for internal testing
pub mod test_utils {
    use std::path::PathBuf;


    /// Get the based path for test data.
    /// 
    ///  Used for internal testing
    pub fn get_test_data_path() -> PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data")
    }
}
