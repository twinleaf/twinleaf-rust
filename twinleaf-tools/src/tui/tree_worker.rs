//! Background thread that drives a [`DeviceTree`] and forwards its
//! [`TreeItem`]s onto a channel, for TUI hosts whose event loops cannot block
//! on `tree.next_item()`.

use crossbeam::channel::{self, Receiver};
use twinleaf::device::{DeviceTree, TreeItem};
use twinleaf::tio::proxy::RpcError;

/// Spawn a worker thread that owns the given [`DeviceTree`] and streams its
/// items through the returned [`Receiver`]. Each successful item arrives as
/// `Ok(item)`; if the tree errors (e.g. the proxy disconnects) the final
/// message is `Err(reason)` so the host can report why the stream ended,
/// rather than treating a dropped channel as a clean exit. The thread exits
/// when the tree errors or the receiver is dropped.
pub fn spawn_tree_worker(tree: DeviceTree) -> Receiver<Result<TreeItem, RpcError>> {
    let (tx, rx) = channel::unbounded::<Result<TreeItem, RpcError>>();
    std::thread::spawn(move || {
        let mut tree = tree;
        loop {
            match tree.next_item() {
                Ok(item) => {
                    if tx.send(Ok(item)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            }
        }
    });
    rx
}
