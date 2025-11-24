use crate::tio::proto::DeviceRoute;
use glob::Pattern;

pub struct ColumnFilter {
    pattern: Pattern,
}

impl ColumnFilter {
    pub fn new(pattern_str: &str) -> Result<Self, String> {
        let corrected = if pattern_str.ends_with('*') {
            pattern_str.to_string()
        } else {
            format!("{}/*", pattern_str) // e.g. "/0/vector" -> "/0/vector/*"
        };

        let pattern = Pattern::new(&corrected)
            .map_err(|e| format!("Invalid glob pattern: {}", e))?;
        
        Ok(Self { pattern })
    }

    pub fn matches(
        &self, 
        route: &DeviceRoute, 
        stream_name: &str, 
        col_name: &str
    ) -> bool {
        let route_str = route.to_string();
        let clean_route = route_str.trim_start_matches('/');
        let stream_path = if clean_route.is_empty() {
            format!("/{}", stream_name)
        } else {
            format!("/{}/{}", clean_route, stream_name)
        };

        let full_path = format!("{}/{}", stream_path, col_name);
        self.pattern.matches(&stream_path) || self.pattern.matches(&full_path)
    }

    pub fn get_path_string(&self, route: &DeviceRoute, stream_name: &str, col_name: &str) -> String {
        let route_str = route.to_string();
        let clean_route = route_str.trim_start_matches('/');
        
        if clean_route.is_empty() {
            format!("/{}/{}", stream_name, col_name)
        } else {
            format!("/{}/{}/{}", clean_route, stream_name, col_name)
        }
    }
}