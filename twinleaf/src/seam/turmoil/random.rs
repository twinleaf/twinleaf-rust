//! The randomness of a run, which follows its seed.
//!
//! The standard library takes the keys of a thread's hash maps from the C
//! library's `getrandom`, or `CCRandomGenerateBytes` on macOS, once per
//! thread, and tokio seeds its runtimes and the start of each `StreamMap`
//! poll from those keys. So the test binary defines those itself, and
//! `getentropy` on macOS, as madsim and mad-turmoil do. On a thread that
//! [`seeded`] started, they give the bytes of a generator seeded with the
//! run's seed. Everywhere else they ask the operating system. A run on a
//! thread of its own then hashes, picks streams and draws jitter the same
//! way each time it runs with one seed.

use std::cell::Cell;

thread_local! {
    /// The state of this thread's generator, if [`seeded`] started the
    /// thread.
    static GENERATOR: Cell<Option<u64>> = const { Cell::new(None) };
}

/// Runs `run` on a thread of its own, whose randomness follows `seed`.
/// Returns what `run` returns, and panics as it panics.
pub(crate) fn seeded<T: Send>(seed: u64, run: impl FnOnce() -> T + Send) -> T {
    std::thread::scope(|scope| {
        let thread = std::thread::Builder::new()
            .name(format!("seed {seed}"))
            .spawn_scoped(scope, || {
                GENERATOR.with(|generator| generator.set(Some(seed)));
                run()
            })
            .expect("a thread for the run");
        thread
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// Fills `buf` with `len` random bytes, as the C library's `getrandom`
/// does, from this thread's generator if [`seeded`] started the thread.
///
/// # Safety
///
/// `buf` must be valid for writes of `len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn getrandom(
    buf: *mut u8,
    len: usize,
    #[cfg_attr(not(target_os = "linux"), expect(unused_variables))] flags: u32,
) -> isize {
    let Some(mut state) = GENERATOR.with(Cell::get) else {
        // SAFETY: the caller's promise about `buf` is the kernel's.
        #[cfg(target_os = "linux")]
        return unsafe { libc::syscall(libc::SYS_getrandom, buf, len, flags) as isize };
        // SAFETY: the caller's promise about `buf` is the C library's, whose
        // `arc4random_buf` this binary does not define.
        #[cfg(target_os = "macos")]
        return unsafe {
            libc::arc4random_buf(buf.cast(), len);
            len as isize
        };
    };
    for at in 0..len {
        // SplitMix64, of which each byte takes the top eight bits.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        // SAFETY: `at` is below `len`.
        unsafe { buf.add(at).write(((mixed ^ (mixed >> 31)) >> 56) as u8) };
    }
    GENERATOR.with(|generator| generator.set(Some(state)));
    len as isize
}

/// Fills `bytes` with `count` random bytes as [`getrandom`] does, returning
/// `kCCSuccess`, as the C library's `CCRandomGenerateBytes` does.
///
/// # Safety
///
/// `bytes` must be valid for writes of `count` bytes.
#[cfg(target_os = "macos")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CCRandomGenerateBytes(bytes: *mut u8, count: usize) -> i32 {
    // SAFETY: the caller's promise about `bytes` is `getrandom`'s.
    unsafe { getrandom(bytes, count, 0) };
    libc::kCCSuccess
}

/// Fills `buf` with `len` random bytes as [`getrandom`] does, returning 0,
/// or -1 with `EIO` for more than 256, as the C library's `getentropy` does.
///
/// # Safety
///
/// `buf` must be valid for writes of `len` bytes.
#[cfg(target_os = "macos")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn getentropy(buf: *mut u8, len: usize) -> i32 {
    if len > 256 {
        // SAFETY: `__error` points at this thread's `errno`.
        unsafe { *libc::__error() = libc::EIO };
        return -1;
    }
    // SAFETY: the caller's promise about `buf` is `getrandom`'s.
    unsafe { getrandom(buf, len, 0) };
    0
}
