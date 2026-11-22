//! The arguments of `tio mcp`.

use clap::Parser;

/// Serves an MCP host that starts its servers as local programs. What the
/// host writes on stdin goes to the MCP endpoint of the web page of the
/// proxy that tools on this machine go through, the one `TWINLEAF_PROXY`
/// names or else the one on the default port, and what the endpoint
/// replies comes back on stdout. The proxy must run with `tio proxy --mcp`.
#[derive(Parser, Debug)]
pub struct McpCli {}
