//! The terminal UI parts the tools share. [`run`] drives a [`Screen`], which
//! holds an [`RpcConsole`]. [`scroll`] has the scroll math of list views, and
//! [`selector`] is the device picker of `tio list`.

mod rpc_console;
mod rpc_palette;
mod screen;
pub mod scroll;
pub mod selector;

pub use rpc_console::RpcConsole;
pub use screen::{run, Screen};
