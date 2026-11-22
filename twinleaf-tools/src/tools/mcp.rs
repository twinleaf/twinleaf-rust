//! The `tio mcp` subcommand, which bridges an MCP host's stdio to the MCP
//! endpoint of the web page of `tio proxy --mcp`.
//!
//! It holds no logic of the protocol. Each JSON-RPC message the host writes
//! on stdin goes to the endpoint, and each the endpoint replies comes back
//! on stdout, one a line. Before it bridges it asks the page whether the
//! endpoint is on, and says on stderr when it is off or no page answers.

use crate::McpCli;
use eyre::{eyre, WrapErr};
use rmcp::transport::async_rw::AsyncRwTransport;
use rmcp::transport::{StreamableHttpClientTransport, Transport};
use rmcp::RoleServer;

pub fn run_mcp(cli: McpCli) -> eyre::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let McpCli {} = cli;
    let address = twinleaf::proxy::chosen_http_address().ok_or_else(|| {
        let proxy = twinleaf::proxy::chosen();
        eyre!("the proxy {proxy} is reached by no TCP address, so no page answers there")
    })?;
    runtime.block_on(bridge(&address))
}

/// Says whether the endpoint of the page of the proxy at `address`, a host
/// and port, is on, or why it is not reached.
async fn check(address: &str) -> eyre::Result<()> {
    let switch = format!("http://{address}/api/mcp");
    let reply = reqwest::get(&switch)
        .await
        .wrap_err_with(|| format!("no web page of a proxy answers at {switch}"))?;
    let state: serde_json::Value = reply
        .error_for_status()
        .wrap_err_with(|| format!("the page at {switch} refused"))?
        .json()
        .await
        .wrap_err_with(|| format!("{switch} gave no switch of an MCP endpoint"))?;
    if state["on"] != true {
        return Err(eyre!(
            "the MCP endpoint of the proxy at {address} is off, as the proxy runs without --mcp"
        ));
    }
    Ok(())
}

/// Carries messages between stdin and stdout and the endpoint of the page
/// of the proxy at `address` until either end closes.
async fn bridge(address: &str) -> eyre::Result<()> {
    check(address).await?;
    let endpoint = format!("http://{address}/mcp");
    let (stdin, stdout) = rmcp::transport::stdio();
    let mut host = AsyncRwTransport::<RoleServer, _, _>::new_server(stdin, stdout);
    let mut hub = StreamableHttpClientTransport::from_uri(endpoint.as_str());
    loop {
        tokio::select! {
            asked = host.receive() => {
                let Some(asked) = asked else {
                    let _ = hub.close().await;
                    return Ok(());
                };
                hub.send(asked)
                    .await
                    .map_err(|error| eyre!("the MCP endpoint at {endpoint} failed: {error}"))?;
            }
            told = hub.receive() => {
                let Some(told) = told else {
                    return Err(eyre!("the MCP endpoint at {endpoint} closed"));
                };
                host.send(told).await.wrap_err("stdout closed")?;
            }
        }
    }
}
