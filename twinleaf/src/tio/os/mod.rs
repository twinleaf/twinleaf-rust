#[cfg(target_os = "macos")]
pub mod macos_helpers {
    use objc2::rc::Retained;
    use objc2::runtime::ProtocolObject;
    use objc2_foundation::{NSActivityOptions, NSObjectProtocol, NSProcessInfo, NSString};

    pub struct ActivityGuard(Retained<ProtocolObject<dyn NSObjectProtocol>>);

    impl ActivityGuard {
        pub fn latency_critical(reason: &str) -> Option<Self> {
            let opts = NSActivityOptions::UserInteractive | NSActivityOptions::LatencyCritical;
            let pi = NSProcessInfo::processInfo();
            let reason = NSString::from_str(reason);
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
    use std::io;
    use core::ffi::c_void;
    use windows_sys::w;

    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, GetThreadPriority, SetThreadInformation,
        THREAD_INFORMATION_CLASS, ThreadPowerThrottling,
        THREAD_POWER_THROTTLING_STATE, THREAD_POWER_THROTTLING_CURRENT_VERSION,
        THREAD_POWER_THROTTLING_EXECUTION_SPEED,
        AvSetMmThreadCharacteristicsW, AvSetMmThreadPriority, AvRevertMmThreadCharacteristics,
        AVRT_PRIORITY, AVRT_PRIORITY_CRITICAL,
    };
    use windows_sys::Win32::System::WindowsProgramming::THREAD_PRIORITY_ERROR_RETURN;

    type AvrtHandle = *mut c_void;

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

                // Join MMCSS "Pro Audio"
                let mut task_index: u32 = 0;
                let handle: AvrtHandle = AvSetMmThreadCharacteristicsW(w!("Pro Audio"), &mut task_index);
                if handle.is_null() {
                    // MMCSS unavailable; continue without it
                    return Ok(ActivityGuard { mmcss_handle: core::ptr::null_mut(), _prev_priority: prev_raw });
                }

                // Raise within MMCSS class
                let _ = AvSetMmThreadPriority(handle, AVRT_PRIORITY_CRITICAL as AVRT_PRIORITY);

                Ok(ActivityGuard { mmcss_handle: handle, _prev_priority: prev_raw })
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
