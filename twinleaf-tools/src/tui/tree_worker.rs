//! Background thread that drives a [`DeviceTree`] and forwards its
//! [`TreeItem`]s onto a channel, for TUI hosts whose event loops cannot block
//! on `tree.next_item()`.

use crossbeam::channel::{self, Receiver};
use twinleaf::device::{DeviceTree, TreeItem};

/// Spawn a worker thread that owns the given [`DeviceTree`] and streams its
/// items through the returned [`Receiver`]. The thread exits when the tree
/// errors or the receiver is dropped.
pub fn spawn_tree_worker(tree: DeviceTree) -> Receiver<TreeItem> {
    let (tx, rx) = channel::unbounded::<TreeItem>();
    std::thread::spawn(move || {
        let mut tree = tree;
        while let Ok(item) = tree.next_item() {
            if tx.send(item).is_err() {
                return;
            }
        }
    });
    rx
}
