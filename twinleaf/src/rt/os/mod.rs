#[cfg(target_os = "macos")]
pub mod macos_helpers {
    use objc2::rc::Retained;
    use objc2::runtime::ProtocolObject;
    use objc2_foundation::{NSActivityOptions, NSObjectProtocol, NSProcessInfo, NSString};

    pub struct ActivityGuard(Retained<ProtocolObject<dyn NSObjectProtocol>>);

    impl ActivityGuard {
        pub fn latency_critical(reason: &str) -> Option<Self> {
            let opts = NSActivityOptions::UserInteractive
                | NSActivityOptions::LatencyCritical
                | NSActivityOptions::IdleSystemSleepDisabled;
            let pi = NSProcessInfo::processInfo();
            let reason = NSString::from_str(reason);
            #[allow(unused_unsafe)]
            let token = unsafe { pi.beginActivityWithOptions_reason(opts, &reason) };
            Some(ActivityGuard(token))
        }
    }

    impl Drop for ActivityGuard {
        fn drop(&mut self) {
            unsafe { NSProcessInfo::processInfo().endActivity(&self.0) };
        }
    }
}

#[cfg(target_os = "windows")]
pub mod windows_helpers {
    use core::ffi::c_void;
    use std::io;
    use windows_sys::w;

    use windows_sys::Win32::Foundation::{
        GetHandleInformation, SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT,
        INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::Console::{
        GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
    };
    use windows_sys::Win32::System::Power::{
        SetThreadExecutionState, ES_CONTINUOUS, ES_DISPLAY_REQUIRED, ES_SYSTEM_REQUIRED,
    };
    use windows_sys::Win32::System::Threading::{
        AvRevertMmThreadCharacteristics, AvSetMmThreadCharacteristicsW, AvSetMmThreadPriority,
        GetCurrentThread, GetThreadPriority, SetThreadInformation, ThreadPowerThrottling,
        AVRT_PRIORITY, AVRT_PRIORITY_CRITICAL, THREAD_INFORMATION_CLASS,
        THREAD_POWER_THROTTLING_CURRENT_VERSION, THREAD_POWER_THROTTLING_EXECUTION_SPEED,
        THREAD_POWER_THROTTLING_STATE,
    };
    use windows_sys::Win32::System::WindowsProgramming::THREAD_PRIORITY_ERROR_RETURN;

    type AvrtHandle = *mut c_void;

    pub struct SleepGuard {
        _prev: u32,
    }
    impl SleepGuard {
        pub fn prevent_sleep(keep_display_on: bool) -> io::Result<Self> {
            let mut flags = ES_CONTINUOUS | ES_SYSTEM_REQUIRED;
            if keep_display_on {
                flags |= ES_DISPLAY_REQUIRED;
            }
            let prev = unsafe { SetThreadExecutionState(flags) };
            if prev == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(SleepGuard { _prev: prev })
            }
        }
    }
    impl Drop for SleepGuard {
        fn drop(&mut self) {
            unsafe {
                let _ = SetThreadExecutionState(ES_CONTINUOUS);
            }
        }
    }

    pub struct ActivityGuard {
        mmcss_handle: AvrtHandle,
        _prev_priority: i32,
    }

    impl ActivityGuard {
        pub fn latency_critical() -> io::Result<Self> {
            unsafe {
                let thread = GetCurrentThread();

                let prev_raw = GetThreadPriority(thread);
                if prev_raw == THREAD_PRIORITY_ERROR_RETURN as i32 {
                    return Err(io::Error::last_os_error());
                }

                // Disable CPU power throttling for this thread
                let mut pstate = THREAD_POWER_THROTTLING_STATE {
                    Version: THREAD_POWER_THROTTLING_CURRENT_VERSION,
                    ControlMask: THREAD_POWER_THROTTLING_EXECUTION_SPEED,
                    StateMask: 0, // 0 = disable throttling
                };
                let _ = SetThreadInformation(
                    thread,
                    ThreadPowerThrottling as THREAD_INFORMATION_CLASS,
                    &mut pstate as *mut _ as *const c_void,
                    core::mem::size_of::<THREAD_POWER_THROTTLING_STATE>() as u32,
                );

                // Join MMCSS "Pro Audio" and raise priority within it
                let mut task_index: u32 = 0;
                let handle: AvrtHandle =
                    AvSetMmThreadCharacteristicsW(w!("Pro Audio"), &mut task_index);
                let _ = if !handle.is_null() {
                    AvSetMmThreadPriority(handle, AVRT_PRIORITY_CRITICAL as AVRT_PRIORITY)
                } else {
                    0
                };

                Ok(ActivityGuard {
                    mmcss_handle: handle,
                    _prev_priority: prev_raw,
                })
            }
        }
    }

    impl Drop for ActivityGuard {
        fn drop(&mut self) {
            unsafe {
                if !self.mmcss_handle.is_null() {
                    AvRevertMmThreadCharacteristics(self.mmcss_handle);
                }
            }
        }
    }

    /// Keeps this process's standard handles from a child started while it
    /// lives. Windows hands a child every handle marked inheritable, and a
    /// child that outlives this process, as a background proxy does, would
    /// hold its output open. A caller reading that output to its end would
    /// then wait for the child to exit. Dropping the guard marks each handle
    /// inheritable again if it was.
    pub struct StdioGuard {
        inheritable: Vec<HANDLE>,
    }

    impl StdioGuard {
        pub fn keep() -> Self {
            let mut inheritable = Vec::new();
            for which in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
                // SAFETY: the calls only read and set the flags of a handle
                // this process holds, and fail on one it does not.
                unsafe {
                    let handle = GetStdHandle(which);
                    let mut flags = 0;
                    if handle.is_null()
                        || handle == INVALID_HANDLE_VALUE
                        || GetHandleInformation(handle, &mut flags) == 0
                        || flags & HANDLE_FLAG_INHERIT == 0
                    {
                        continue;
                    }
                    if SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) != 0 {
                        inheritable.push(handle);
                    }
                }
            }
            StdioGuard { inheritable }
        }
    }

    impl Drop for StdioGuard {
        fn drop(&mut self) {
            for &handle in &self.inheritable {
                // SAFETY: as in `keep`.
                unsafe {
                    SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Returns whether each standard handle is inheritable, or `None`
        /// for one this process does not have.
        fn inheritable() -> Vec<Option<bool>> {
            [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE]
                .into_iter()
                .map(|which| unsafe {
                    let handle = GetStdHandle(which);
                    let mut flags = 0;
                    let known = !handle.is_null()
                        && handle != INVALID_HANDLE_VALUE
                        && GetHandleInformation(handle, &mut flags) != 0;
                    known.then_some(flags & HANDLE_FLAG_INHERIT != 0)
                })
                .collect()
        }

        /// No standard handle is inheritable while the guard lives, and each
        /// is as it was once the guard drops.
        #[test]
        fn the_guard_keeps_each_standard_handle_and_gives_it_back() {
            let before = inheritable();

            let guard = StdioGuard::keep();
            let kept = inheritable();
            drop(guard);

            assert!(kept.iter().all(|flag| *flag != Some(true)), "{kept:?}");
            assert_eq!(inheritable(), before);
        }
    }
}
