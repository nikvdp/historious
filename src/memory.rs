#[derive(Debug, Clone, Copy, Default)]
pub struct MemorySample {
    pub total_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub process_rss_bytes: Option<u64>,
}

impl MemorySample {
    pub fn available_gib(self) -> Option<f64> {
        self.available_bytes
            .map(|bytes| bytes as f64 / 1024.0 / 1024.0 / 1024.0)
    }
}

pub fn sample_memory() -> Option<MemorySample> {
    platform_sample_memory()
}

#[cfg(target_os = "linux")]
fn platform_sample_memory() -> Option<MemorySample> {
    let (total_bytes, mut available_bytes) = linux_meminfo();
    if let Some(cgroup_available) = linux_cgroup_available_bytes() {
        available_bytes = Some(
            available_bytes
                .map(|available| available.min(cgroup_available))
                .unwrap_or(cgroup_available),
        );
    }
    Some(MemorySample {
        total_bytes,
        available_bytes,
        process_rss_bytes: linux_process_rss_bytes(),
    })
}

#[cfg(target_os = "linux")]
fn linux_meminfo() -> (Option<u64>, Option<u64>) {
    let Ok(text) = std::fs::read_to_string("/proc/meminfo") else {
        return (None, None);
    };
    let mut total = None;
    let mut available = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("MemTotal:") {
            total = parse_meminfo_kib(value);
        } else if let Some(value) = line.strip_prefix("MemAvailable:") {
            available = parse_meminfo_kib(value);
        }
    }
    (total, available)
}

#[cfg(target_os = "linux")]
fn parse_meminfo_kib(value: &str) -> Option<u64> {
    value
        .split_whitespace()
        .next()
        .and_then(|text| text.parse::<u64>().ok())
        .map(|kib| kib.saturating_mul(1024))
}

#[cfg(target_os = "linux")]
fn linux_process_rss_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("VmRSS:") {
            return parse_meminfo_kib(value);
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn linux_cgroup_available_bytes() -> Option<u64> {
    let cgroup_path = linux_cgroup_v2_path()?;
    let max_text = std::fs::read_to_string(cgroup_path.join("memory.max")).ok()?;
    let max_text = max_text.trim();
    if max_text == "max" {
        return None;
    }
    let max = max_text.parse::<u64>().ok()?;
    let current = std::fs::read_to_string(cgroup_path.join("memory.current"))
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    Some(max.saturating_sub(current))
}

#[cfg(target_os = "linux")]
fn linux_cgroup_v2_path() -> Option<std::path::PathBuf> {
    let text = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    for line in text.lines() {
        let mut parts = line.splitn(3, ':');
        let hierarchy = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        if hierarchy == "0" && controllers.is_empty() {
            return Some(std::path::Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/')));
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn platform_sample_memory() -> Option<MemorySample> {
    let total_bytes = macos_total_bytes();
    let vm = macos_vm_stats();
    let page_size = macos_page_size();
    let available_bytes = vm.zip(page_size).map(|(vm, page_size)| {
        let pages = vm
            .free_count
            .saturating_add(vm.inactive_count)
            .saturating_add(vm.speculative_count);
        (pages as u64).saturating_mul(page_size)
    });
    Some(MemorySample {
        total_bytes,
        available_bytes,
        process_rss_bytes: macos_process_rss_bytes(),
    })
}

#[cfg(target_os = "macos")]
fn macos_total_bytes() -> Option<u64> {
    let mut value = 0u64;
    let mut size = std::mem::size_of::<u64>();
    let name = std::ffi::CString::new("hw.memsize").ok()?;
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut value as *mut _ as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (rc == 0).then_some(value)
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn macos_vm_stats() -> Option<libc::vm_statistics64_data_t> {
    let mut stats = std::mem::MaybeUninit::<libc::vm_statistics64_data_t>::zeroed();
    let mut count = libc::HOST_VM_INFO64_COUNT;
    let rc = unsafe {
        libc::host_statistics64(
            libc::mach_host_self(),
            libc::HOST_VM_INFO64,
            stats.as_mut_ptr() as libc::host_info64_t,
            &mut count,
        )
    };
    if rc == 0 {
        Some(unsafe { stats.assume_init() })
    } else {
        None
    }
}

#[cfg(target_os = "macos")]
fn macos_page_size() -> Option<u64> {
    let mut value = 0i32;
    let mut size = std::mem::size_of::<i32>();
    let name = std::ffi::CString::new("hw.pagesize").ok()?;
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            &mut value as *mut _ as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (rc == 0 && value > 0).then_some(value as u64)
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
fn macos_process_rss_bytes() -> Option<u64> {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info_data_t>::zeroed();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    let rc = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr() as libc::task_info_t,
            &mut count,
        )
    };
    if rc == 0 {
        Some(unsafe { info.assume_init() }.resident_size as u64)
    } else {
        None
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_sample_memory() -> Option<MemorySample> {
    None
}
