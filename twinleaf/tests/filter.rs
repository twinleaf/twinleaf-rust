use twinleaf::data::ColumnFilter;
use twinleaf::tio::proto::DeviceRoute;

fn route(s: &str) -> DeviceRoute {
    DeviceRoute::from_str(s).unwrap()
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
