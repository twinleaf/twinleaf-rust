#![doc = include_str!("../README.md")]

// The `tio` binary and the benches share this library. It is not an API, so
// docs.rs shows the README in its place.
#[doc(hidden)]
pub mod cli;
#[doc(hidden)]
pub mod logging;
#[doc(hidden)]
pub mod tools;
#[doc(hidden)]
pub mod tui;

#[doc(hidden)]
pub use cli::*;

#[doc(hidden)]
pub fn install_error_handler() -> eyre::Result<()> {
    color_eyre::config::HookBuilder::default()
        .display_env_section(false)
        .display_location_section(false)
        .install()
}

#[doc(hidden)]
pub trait ProxyHelp<T> {
    fn with_proxy_help(self) -> eyre::Result<T>;
}

impl<T> ProxyHelp<T> for eyre::Result<T> {
    fn with_proxy_help(self) -> eyre::Result<T> {
        use color_eyre::Help;
        self.suggestion("run `tio list` to discover devices and host a selection")
            .suggestion("or specify a source with -r <url>")
    }
}
