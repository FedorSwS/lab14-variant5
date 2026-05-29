use pyo3::prelude::*;
use regex::Regex;

#[pyclass]
pub struct LogValidator {
    ip_regex: Regex,
    path_regex: Regex,
}

#[pymethods]
impl LogValidator {
    #[new]
    fn new() -> Self {
        LogValidator {
            ip_regex: Regex::new(r"^(\d{1,3}\.){3}\d{1,3}$").unwrap(),
            path_regex: Regex::new(r"^/[a-zA-Z0-9/._-]*$").unwrap(),
        }
    }

    fn validate_ip(&self, ip: String) -> bool {
        self.ip_regex.is_match(&ip)
    }

    fn validate_path(&self, path: String) -> bool {
        self.path_regex.is_match(&path)
    }

    fn validate_status(&self, status: i32) -> bool {
        (100..=599).contains(&status)
    }

    fn validate_response_time(&self, rt: f64) -> bool {
        (0.0..=60.0).contains(&rt)
    }

    fn validate_entry(&self, ip: String, path: String, status: i32, rt: f64) -> Vec<String> {
        let mut errors = Vec::new();
        if !self.validate_ip(ip) {
            errors.push("Invalid IP".to_string());
        }
        if !self.validate_path(path) {
            errors.push("Invalid path".to_string());
        }
        if !self.validate_status(status) {
            errors.push(format!("Invalid status: {}", status));
        }
        if !self.validate_response_time(rt) {
            errors.push(format!("Invalid response time: {}", rt));
        }
        errors
    }

    fn anonymize_ip(&self, ip: String) -> String {
        let parts: Vec<&str> = ip.split('.').collect();
        if parts.len() == 4 {
            format!("{}.{}.0.0", parts[0], parts[1])
        } else {
            "0.0.0.0".to_string()
        }
    }

    fn batch_validate(&self, entries: Vec<(String, String, i32, f64)>) -> Vec<Vec<String>> {
        entries.into_iter()
            .map(|(ip, path, status, rt)| self.validate_entry(ip, path, status, rt))
            .collect()
    }
}

#[pymodule]
fn log_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<LogValidator>()?;
    Ok(())
}
