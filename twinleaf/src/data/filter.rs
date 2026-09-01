//! Glob matching over column paths.
//!
//! [`ColumnFilter`] holds two patterns per filter — one matching the named node
//! itself, one matching its subtree — so naming a node selects everything
//! beneath it without the caller enumerating depths.

use crate::tio::proto::DeviceRoute;
use glob::Pattern;

/// Glob filter over column paths of the form `/{route}/{stream}/{column}`.
///
/// For example `/0/1/vector/x` is route `/0/1`, stream `vector`, column `x`.
/// Routes are numeric device indices; streams and columns are names.
///
/// A pattern names a node in that tree and selects **that node and everything
/// under it**, like a path in `.gitignore`. Naming a stream keeps all its
/// columns; naming a route keeps every stream under it; naming a column keeps
/// just that column.
///
/// Wildcards match with strict separators, like a shell:
/// - `*`  matches exactly one segment (does not cross `/`)
/// - `**` matches any number of segments (crosses `/`)
///
/// A bare name (no `/`) is shorthand for `**/name` — that node at any depth. A
/// pattern containing `/` is anchored and used as written. An empty pattern
/// matches nothing.
///
/// # Examples
/// | Pattern        | Selects                                        |
/// |----------------|------------------------------------------------|
/// | `vector`       | stream `vector` (all columns), any route       |
/// | `**/vector`    | same — bare name and `**/name` are equivalent  |
/// | `sync.vco.x`   | the column `sync.vco.x`, any route             |
/// | `/0`           | everything under route `/0`                     |
/// | `/0/vector`    | stream `vector` under `/0` (all columns)        |
/// | `/0/vector/x`  | exactly that column                            |
/// | `/0/*/x`       | column `x` of any stream directly under `/0`    |
pub struct ColumnFilter {
    /// Matches the named node itself (a route, stream, or column path).
    node: Pattern,
    /// Matches anything beneath that node — i.e. the node's subtree.
    subtree: Pattern,
}

impl ColumnFilter {
    /// Compile a pattern, failing only when it is not valid glob syntax.
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

    /// Expand a user pattern into a glob naming one node of the tree.
    ///
    /// - bare name (`vector`) → `**/vector` (a node named `vector` at any depth)
    /// - anything containing `/` → used verbatim (anchored)
    /// - empty input → empty (matches nothing)
    ///
    /// [`new`](Self::new) then also matches that node's subtree, so a bare name
    /// selects the whole stream, a route selects all its streams, and so on.
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

    /// Whether this column is selected, either by being the named node or by
    /// living under it.
    pub fn matches(&self, route: &DeviceRoute, stream_name: &str, col_name: &str) -> bool {
        let full_path = self.get_path_string(route, stream_name, col_name);
        let opts = glob::MatchOptions {
            require_literal_separator: true,
            ..Default::default()
        };
        // The column matches if the pattern names this exact path, or names an
        // ancestor (route or stream) whose subtree this column lives in.
        self.node.matches_with(&full_path, opts) || self.subtree.matches_with(&full_path, opts)
    }

    /// The `/{route}/{stream}/{column}` path that patterns are matched against.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tio::proto::DeviceRoute;

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
