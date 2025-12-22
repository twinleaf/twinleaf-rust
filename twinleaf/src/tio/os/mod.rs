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
            let token = pi.beginActivityWithOptions_reason(opts, &reason);
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
        #[allow(dead_code)]
        _sleep: Option<SleepGuard>,
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

                // Also keep the system awake while this thread runs
                let sleep = SleepGuard::prevent_sleep(false).ok();

                Ok(ActivityGuard {
                    mmcss_handle: handle,
                    _prev_priority: prev_raw,
                    _sleep: sleep,
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
}
