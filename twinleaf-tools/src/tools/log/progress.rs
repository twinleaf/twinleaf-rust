use indicatif::{ProgressBar, ProgressStyle};

const UPDATE_BYTES: u64 = 1024 * 1024;

/// A byte-counted progress bar whose relatively expensive state updates are
/// kept out of per-packet hot paths.
pub(super) struct ByteProgress {
    bar: ProgressBar,
    next_update: u64,
}

impl ByteProgress {
    pub(super) fn new(total_bytes: u64) -> Self {
        let bar = crate::multi_progress().add(ProgressBar::new(total_bytes));
        bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        Self {
            bar,
            next_update: UPDATE_BYTES,
        }
    }

    pub(super) fn set_message(&self, message: String) {
        self.bar.set_message(message);
    }

    pub(super) fn update(&mut self, position: u64) {
        if position >= self.next_update {
            self.bar.set_position(position);
            self.next_update = position.saturating_add(UPDATE_BYTES);
        }
    }

    pub(super) fn finish_and_clear(self, position: u64) {
        self.bar.set_position(position);
        self.bar.finish_and_clear();
    }

    #[cfg(feature = "hdf5")]
    pub(super) fn finish_with_message(self, position: u64, message: &'static str) {
        self.bar.set_position(position);
        self.bar.finish_with_message(message);
    }
}
