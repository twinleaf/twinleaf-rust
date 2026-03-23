use crate::tio::proto::DeviceRoute;
use glob::Pattern;

/// Column filter using glob patterns against paths of the form `/{route}/{stream}/{column}`.
///
/// # Path Structure
/// - Routes are always numeric device indices: `/0`, `/0/1`, `/0/1/2`
/// - Streams and columns have alphabetic names: `vector`, `accel`, `x`, `y`
/// - Full path example: `/0/1/vector/x` (route=`/0/1`, stream=`vector`, column=`x`)
///
/// # Pattern Behavior
/// - `*` matches any characters EXCEPT `/` (single path segment)
/// - `**` matches any characters INCLUDING `/` (zero or more segments)
///
/// # Convenience Rules
/// - Bare alphabetic names are treated as stream names and expanded:
///   `vector` → `**/vector/**` (matches stream `vector` at any route depth, all columns)
///
/// # Stream vs Column Disambiguation
/// - Pattern ending with `/**` or `/*` indicates stream match (all columns)
/// - Pattern ending with alphabetic name indicates column match
/// - `**/x` → column `x` anywhere (implicit `**/*/x`)
/// - `**/x/**` → stream `x` anywhere, all columns
///
/// # Route Detection
/// Routes are detected by numeric-only segments. The first segment containing
/// letters marks the beginning of stream/column portion.
///
/// # Examples
/// | Pattern | Interpretation | Matches |
/// |---------|----------------|---------|
/// | `vector` | Stream anywhere | `/vector/*`, `/0/vector/*`, `/0/1/vector/*` |
/// | `**/x` | Column anywhere | Any column named `x` |
/// | `**/vector/**` | Stream anywhere | Stream `vector` at any depth |
/// | `/0/vector/**` | Exact route+stream | All columns in `/0/vector` |
/// | `/0/vector/x` | Exact column | Only `/0/vector/x` |
/// | `/0/*/x` | Wildcard stream | Column `x` in any stream at `/0` |
/// | `/0/**` | Recursive route | Everything under route `/0` |
pub struct ColumnFilter {
    pattern: Pattern,
}

impl ColumnFilter {
    pub fn new(pattern_str: &str) -> Result<Self, String> {
        let normalized = Self::normalize_pattern(pattern_str);
        let pattern =
            Pattern::new(&normalized).map_err(|e| format!("Invalid glob pattern: {}", e))?;

        Ok(Self { pattern })
    }

    /// Normalize user pattern to a full path glob pattern.
    ///
    /// Rules:
    /// 1. Bare name (no `/`, no `*`) → `**/name/**` (stream anywhere)
    /// 2. Pattern ending with alphabetic name (no trailing `/**`) → treat final segment as column
    /// 3. Everything else → pass through as-is
    fn normalize_pattern(pattern_str: &str) -> String {
        let trimmed = pattern_str.trim();

        // Empty pattern matches nothing (or should error?)
        if trimmed.is_empty() {
            return trimmed.to_string();
        }

        // If pattern already contains wildcards, analyze structure
        if trimmed.contains('*') {
            // Check if it looks like a column-anywhere pattern: **/name (no trailing /**)
            // e.g., "**/x" should match column x anywhere
            if trimmed.starts_with("**/") && !trimmed.ends_with("/**") && !trimmed.ends_with("/*") {
                let after_prefix = &trimmed[3..]; // strip "**/""
                                                  // If what remains is a simple name (no more slashes), it's a column pattern
                                                  // **/x -> **/*/x (any route, any stream, column x)
                if !after_prefix.contains('/') && Self::is_alphabetic_name(after_prefix) {
                    return format!("**/*/{}", after_prefix);
                }
            }
            // Otherwise use as-is - user knows what they're doing
            return trimmed.to_string();
        }

        // No wildcards - check if bare name or path
        if !trimmed.contains('/') {
            // Bare name like "vector" without leading slash
            // Interpret as: match this stream name at any route depth, all columns
            return format!("**/{}/**", trimmed);
        }

        // Has slashes but no wildcards - use as-is for exact matching
        trimmed.to_string()
    }

    /// Check if a string looks like an alphabetic name (contains letters, not purely numeric)
    fn is_alphabetic_name(s: &str) -> bool {
        // A name is alphabetic if it contains at least one letter
        // This distinguishes stream/column names from route indices
        s.chars().any(|c| c.is_alphabetic())
    }

    pub fn matches(&self, route: &DeviceRoute, stream_name: &str, col_name: &str) -> bool {
        let full_path = self.get_path_string(route, stream_name, col_name);
        self.pattern.matches(&full_path)
    }

    pub fn get_path_string(
        &self,
        route: &DeviceRoute,
        stream_name: &str,
        col_name: &str,
    ) -> String {
        let route_str = route.to_string();
        let clean_route = route_str.trim_start_matches('/');

        if clean_route.is_empty() {
            format!("/{}/{}", stream_name, col_name)
        } else {
            format!("/{}/{}/{}", clean_route, stream_name, col_name)
        }
    }
}
