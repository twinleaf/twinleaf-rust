//! Glob matching over column paths.
//!
//! [`ColumnFilter`] holds two patterns per filter. One matches the named node
//! itself and the other matches its subtree, so naming a node selects
//! everything beneath it.

use crate::proto::DeviceRoute;
use glob::Pattern;

/// Glob filter over column paths of the form `/{route}/{stream}/{column}`.
///
/// For example `/0/1/vector/x` is route `/0/1`, stream `vector`, column `x`.
/// Routes are numeric device indices. Streams and columns are names.
///
/// A pattern names a node in that tree and selects **that node and everything
/// under it**, like a path in `.gitignore`. Naming a stream keeps all its
/// columns. Naming a route keeps every stream under it. Naming a column keeps
/// just that column.
///
/// Wildcards match with strict separators, like a shell.
/// - `*` matches exactly one segment and does not cross `/`.
/// - `**` matches any number of segments and crosses `/`.
///
/// A bare name with no `/` is shorthand for `**/name`, that node at any depth.
/// A pattern containing `/` is anchored and used as written. An empty pattern
/// matches nothing.
///
/// # Examples
///
/// | Pattern        | Selects                                        |
/// |----------------|------------------------------------------------|
/// | `vector`       | stream `vector` (all columns), any route       |
/// | `**/vector`    | the same as `vector`                           |
/// | `sync.vco.x`   | the column `sync.vco.x`, any route             |
/// | `/0`           | everything under route `/0`                     |
/// | `/0/vector`    | stream `vector` under `/0` (all columns)        |
/// | `/0/vector/x`  | exactly that column                            |
/// | `/0/*/x`       | column `x` of any stream directly under `/0`    |
///
/// ```
/// use twinleaf::data::ColumnFilter;
/// use twinleaf::DeviceRoute;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let filter = ColumnFilter::new("vector")?;
/// let route: DeviceRoute = "/0".parse()?;
/// assert!(filter.matches(&route, "vector", "x"));
/// assert!(!filter.matches(&route, "accel", "x"));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ColumnFilter {
    /// Matches the named node itself (a route, stream, or column path).
    node: Pattern,
    /// Matches anything beneath that node.
    subtree: Pattern,
}

impl ColumnFilter {
    /// Compiles `pattern_str` into a filter.
    ///
    /// # Errors
    ///
    /// Returns a message if the pattern is not valid glob syntax.
    pub fn new(pattern_str: &str) -> Result<Self, String> {
        let normalized = Self::normalize_pattern(pattern_str);
        let build = |p: &str| Pattern::new(p).map_err(|e| format!("Invalid glob pattern: {}", e));

        // A pattern selects the node it names (`node`) plus everything under it
        // (`<node>/**`). An empty pattern matches nothing, so both stay empty —
        // the empty glob only matches the empty string, which paths never are.
        let (node, subtree) = if normalized.is_empty() {
            (build("")?, build("")?)
        } else {
            (build(&normalized)?, build(&format!("{}/**", normalized))?)
        };

        Ok(Self { node, subtree })
    }

    /// Expands a trimmed user pattern into a glob that names one node of the
    /// tree.
    ///
    /// - A bare name such as `vector` becomes `**/vector`, a node named
    ///   `vector` at any depth.
    /// - A pattern containing `/` is anchored and used as written.
    /// - An empty pattern stays empty and matches nothing.
    ///
    /// [`new`](Self::new) also matches that node's subtree.
    fn normalize_pattern(pattern_str: &str) -> String {
        let trimmed = pattern_str.trim();
        if trimmed.is_empty() {
            String::new()
        } else if trimmed.contains('/') {
            trimmed.to_string()
        } else {
            format!("**/{}", trimmed)
        }
    }

    /// Returns `true` if the filter selects the column, either as the named
    /// node or as part of its subtree.
    pub fn matches(&self, route: &DeviceRoute, stream_name: &str, col_name: &str) -> bool {
        let full_path = self.path_string(route, stream_name, col_name);
        let opts = glob::MatchOptions {
            require_literal_separator: true,
            ..Default::default()
        };
        // The column matches if the pattern names this exact path, or names an
        // ancestor (route or stream) whose subtree this column lives in.
        self.node.matches_with(&full_path, opts) || self.subtree.matches_with(&full_path, opts)
    }

    /// Returns the `/{route}/{stream}/{column}` path that patterns match
    /// against. The root route adds no segment, giving `/{stream}/{column}`.
    pub fn path_string(&self, route: &DeviceRoute, stream_name: &str, col_name: &str) -> String {
        let route_str = route.to_string();
        let clean_route = route_str.trim_start_matches('/');

        if clean_route.is_empty() {
            format!("/{}/{}", stream_name, col_name)
        } else {
            format!("/{}/{}/{}", clean_route, stream_name, col_name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::DeviceRoute;

    fn route(s: &str) -> DeviceRoute {
        s.parse().unwrap()
    }

    #[test]
    fn test_bare_stream_name() {
        let filter = ColumnFilter::new("vector").unwrap();
        assert!(filter.matches(&route("/"), "vector", "x"));
        assert!(filter.matches(&route("/0"), "vector", "y"));
        assert!(filter.matches(&route("/0/1"), "vector", "z"));
        assert!(!filter.matches(&route("/0"), "accel", "x"));
    }

    #[test]
    fn test_column_anywhere() {
        let filter = ColumnFilter::new("**/x").unwrap();
        assert!(filter.matches(&route("/"), "vector", "x"));
        assert!(filter.matches(&route("/0"), "accel", "x"));
        assert!(filter.matches(&route("/0/1/2"), "gmr", "x"));
        assert!(!filter.matches(&route("/0"), "vector", "y"));
    }

    #[test]
    fn test_stream_anywhere_explicit() {
        let filter = ColumnFilter::new("**/vector/**").unwrap();
        assert!(filter.matches(&route("/"), "vector", "x"));
        assert!(filter.matches(&route("/0/1"), "vector", "y"));
        assert!(!filter.matches(&route("/0"), "accel", "x"));
    }

    #[test]
    fn test_exact_stream_path() {
        let filter = ColumnFilter::new("/0/vector/**").unwrap();
        assert!(filter.matches(&route("/0"), "vector", "x"));
        assert!(filter.matches(&route("/0"), "vector", "y"));
        assert!(!filter.matches(&route("/1"), "vector", "x"));
        assert!(!filter.matches(&route("/0"), "accel", "x"));
    }

    #[test]
    fn test_exact_column() {
        let filter = ColumnFilter::new("/0/vector/x").unwrap();
        assert!(filter.matches(&route("/0"), "vector", "x"));
        assert!(!filter.matches(&route("/0"), "vector", "y"));
        assert!(!filter.matches(&route("/1"), "vector", "x"));
    }

    #[test]
    fn test_wildcard_stream() {
        let filter = ColumnFilter::new("/0/*/x").unwrap();
        assert!(filter.matches(&route("/0"), "vector", "x"));
        assert!(filter.matches(&route("/0"), "accel", "x"));
        assert!(!filter.matches(&route("/0"), "vector", "y"));
        assert!(!filter.matches(&route("/1"), "vector", "x"));
    }

    #[test]
    fn test_recursive_route() {
        let filter = ColumnFilter::new("/0/**").unwrap();
        assert!(filter.matches(&route("/0"), "vector", "x"));
        assert!(filter.matches(&route("/0"), "accel", "y"));
        assert!(filter.matches(&route("/0/1"), "gmr", "z"));
        assert!(!filter.matches(&route("/1"), "vector", "x"));
    }

    #[test]
    fn test_root_stream() {
        let filter = ColumnFilter::new("/vector/**").unwrap();
        assert!(filter.matches(&route("/"), "vector", "x"));
        assert!(filter.matches(&route("/"), "vector", "y"));
        assert!(!filter.matches(&route("/0"), "vector", "x"));
    }

    #[test]
    fn test_nested_route_stream() {
        let filter = ColumnFilter::new("/0/1/vector/**").unwrap();
        assert!(filter.matches(&route("/0/1"), "vector", "x"));
        assert!(!filter.matches(&route("/0"), "vector", "x"));
        assert!(!filter.matches(&route("/0/1/2"), "vector", "x"));
    }

    #[test]
    fn test_wildcard_column() {
        let filter = ColumnFilter::new("/0/vector/*").unwrap();
        assert!(filter.matches(&route("/0"), "vector", "x"));
        assert!(filter.matches(&route("/0"), "vector", "y"));
        assert!(filter.matches(&route("/0"), "vector", "z"));
        assert!(!filter.matches(&route("/0"), "accel", "x"));
    }
}
