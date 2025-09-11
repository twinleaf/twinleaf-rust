#[cfg(target_os = "macos")]
pub mod macos_helpers {
    use objc2::rc::Retained;
    use objc2::runtime::ProtocolObject;
    use objc2_foundation::{NSActivityOptions, NSObjectProtocol, NSProcessInfo, NSString};

    pub struct ActivityGuard(Retained<ProtocolObject<dyn NSObjectProtocol>>);

    impl ActivityGuard {
        pub fn latency_critical(reason: &str) -> Option<Self> {
            let opts = NSActivityOptions::UserInitiated | NSActivityOptions::LatencyCritical;
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
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, GetThreadPriority, SetThreadInformation, SetThreadPriority,
        THREAD_INFORMATION_CLASS, ThreadPowerThrottling,
        THREAD_POWER_THROTTLING_STATE, THREAD_POWER_THROTTLING_CURRENT_VERSION,
        THREAD_POWER_THROTTLING_EXECUTION_SPEED,
        THREAD_PRIORITY_NORMAL, THREAD_PRIORITY_TIME_CRITICAL,
    };
    use windows_sys::Win32::System::WindowsProgramming::THREAD_PRIORITY_ERROR_RETURN;

    pub struct ActivityGuard {
        prev: i32,
    }

    impl ActivityGuard {
        pub fn latency_critical() -> io::Result<Self> {
            unsafe {
                let h = GetCurrentThread();

                let prev_raw = GetThreadPriority(h);
                if prev_raw == (THREAD_PRIORITY_ERROR_RETURN as i32) {
                    return Err(io::Error::last_os_error());
                }
                let prev = prev_raw;

                let mut pstate = THREAD_POWER_THROTTLING_STATE {
                    Version: THREAD_POWER_THROTTLING_CURRENT_VERSION,
                    ControlMask: THREAD_POWER_THROTTLING_EXECUTION_SPEED,
                    StateMask: 0
                };
                let _ = SetThreadInformation(
                    h,
                    ThreadPowerThrottling as THREAD_INFORMATION_CLASS,
                    &mut pstate as *mut _ as *mut _,
                    core::mem::size_of::<THREAD_POWER_THROTTLING_STATE>() as u32,
                );

                if SetThreadPriority(h, THREAD_PRIORITY_TIME_CRITICAL) == 0 {
                    return Err(io::Error::last_os_error());
                }

                Ok(ActivityGuard { prev })
            }
        }
    }

    impl Drop for ActivityGuard {
        fn drop(&mut self) {
            unsafe {
                let h = GetCurrentThread();
                let _ = SetThreadPriority(h, self.prev);
                let _ = SetThreadPriority(h, THREAD_PRIORITY_NORMAL);
            }
        }
    }
}