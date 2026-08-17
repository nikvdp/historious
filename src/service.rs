use anyhow::{bail, Context, Result};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const UPDATE_LABEL: &str = "com.historious.update";
const REPORT_LABEL: &str = "com.historious.report";
const UPDATE_TIMER: &str = "historious-update.timer";
const REPORT_TIMER: &str = "historious-report.timer";

#[derive(Debug, Clone, Copy)]
pub enum Action {
    Install,
    Uninstall,
    Status,
}

#[derive(Debug, Clone)]
pub struct JobStatus {
    pub name: &'static str,
    pub schedule: &'static str,
    pub installed: bool,
    pub active: bool,
}

#[derive(Debug, Clone)]
pub struct ServiceStatus {
    pub backend: &'static str,
    pub update: JobStatus,
    pub report: JobStatus,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
enum Platform {
    Launchd,
    Systemd,
}

impl Platform {
    #[cfg(target_os = "macos")]
    fn current() -> Result<Self> {
        Ok(Self::Launchd)
    }

    #[cfg(target_os = "linux")]
    fn current() -> Result<Self> {
        Ok(Self::Systemd)
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn current() -> Result<Self> {
        bail!("persistent services are supported only on macOS and Linux")
    }

    fn name(self) -> &'static str {
        match self {
            Self::Launchd => "launchd",
            Self::Systemd => "systemd --user",
        }
    }
}

struct CommandResult {
    success: bool,
    stderr: String,
}

trait Runner {
    fn run(&mut self, program: &str, args: &[OsString]) -> Result<CommandResult>;
}

struct RealRunner;

impl Runner for RealRunner {
    fn run(&mut self, program: &str, args: &[OsString]) -> Result<CommandResult> {
        let output = Command::new(program)
            .args(args)
            .output()
            .with_context(|| format!("running {program}"))?;
        Ok(CommandResult {
            success: output.status.success(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
        })
    }
}

pub fn manage(action: Action, data_dir: &Path) -> Result<ServiceStatus> {
    let platform = Platform::current()?;
    let executable = env::current_exe().context("locating the histo executable")?;
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is unavailable; cannot install a user service")?;
    let mut runner = RealRunner;
    manage_with(
        platform,
        action,
        &home,
        env::var_os("XDG_CONFIG_HOME").as_deref().map(Path::new),
        &executable,
        data_dir,
        &mut runner,
    )
}

fn manage_with(
    platform: Platform,
    action: Action,
    home: &Path,
    xdg_config_home: Option<&Path>,
    executable: &Path,
    data_dir: &Path,
    runner: &mut impl Runner,
) -> Result<ServiceStatus> {
    match platform {
        Platform::Launchd => manage_launchd(action, home, executable, data_dir, runner)?,
        Platform::Systemd => {
            manage_systemd(action, home, xdg_config_home, executable, data_dir, runner)?
        }
    }
    status_with(platform, home, xdg_config_home, runner)
}

fn manage_launchd(
    action: Action,
    home: &Path,
    executable: &Path,
    data_dir: &Path,
    runner: &mut impl Runner,
) -> Result<()> {
    let directory = home.join("Library/LaunchAgents");
    let update_path = directory.join(format!("{UPDATE_LABEL}.plist"));
    let report_path = directory.join(format!("{REPORT_LABEL}.plist"));
    let domain = format!("gui/{}", unsafe { libc::geteuid() });

    match action {
        Action::Install => {
            fs::create_dir_all(&directory)
                .with_context(|| format!("creating {}", directory.display()))?;
            fs::write(
                &update_path,
                launchd_plist(UPDATE_LABEL, executable, data_dir, LaunchdSchedule::Hourly),
            )
            .with_context(|| format!("writing {}", update_path.display()))?;
            fs::write(
                &report_path,
                launchd_plist(REPORT_LABEL, executable, data_dir, LaunchdSchedule::Daily),
            )
            .with_context(|| format!("writing {}", report_path.display()))?;
            let _ = runner.run(
                "launchctl",
                &[
                    OsString::from("bootout"),
                    OsString::from(format!("{domain}/{UPDATE_LABEL}")),
                ],
            );
            let _ = runner.run(
                "launchctl",
                &[
                    OsString::from("bootout"),
                    OsString::from(format!("{domain}/{REPORT_LABEL}")),
                ],
            );
            run_checked(
                runner,
                "launchctl",
                &[
                    OsString::from("bootstrap"),
                    OsString::from(&domain),
                    update_path.into_os_string(),
                ],
            )?;
            run_checked(
                runner,
                "launchctl",
                &[
                    OsString::from("bootstrap"),
                    OsString::from(&domain),
                    report_path.into_os_string(),
                ],
            )?;
        }
        Action::Uninstall => {
            let _ = runner.run(
                "launchctl",
                &[
                    OsString::from("bootout"),
                    OsString::from(format!("{domain}/{UPDATE_LABEL}")),
                ],
            );
            let _ = runner.run(
                "launchctl",
                &[
                    OsString::from("bootout"),
                    OsString::from(format!("{domain}/{REPORT_LABEL}")),
                ],
            );
            remove_if_exists(&update_path)?;
            remove_if_exists(&report_path)?;
        }
        Action::Status => {}
    }
    Ok(())
}

fn manage_systemd(
    action: Action,
    home: &Path,
    xdg_config_home: Option<&Path>,
    executable: &Path,
    data_dir: &Path,
    runner: &mut impl Runner,
) -> Result<()> {
    let directory = systemd_directory(home, xdg_config_home);
    let files = systemd_files(executable, data_dir);
    match action {
        Action::Install => {
            fs::create_dir_all(&directory)
                .with_context(|| format!("creating {}", directory.display()))?;
            for (name, content) in &files {
                let path = directory.join(name);
                fs::write(&path, content).with_context(|| format!("writing {}", path.display()))?;
            }
            run_checked(
                runner,
                "systemctl",
                &[OsString::from("--user"), OsString::from("daemon-reload")],
            )?;
            run_checked(
                runner,
                "systemctl",
                &[
                    OsString::from("--user"),
                    OsString::from("enable"),
                    OsString::from("--now"),
                    OsString::from(UPDATE_TIMER),
                    OsString::from(REPORT_TIMER),
                ],
            )?;
        }
        Action::Uninstall => {
            let _ = runner.run(
                "systemctl",
                &[
                    OsString::from("--user"),
                    OsString::from("disable"),
                    OsString::from("--now"),
                    OsString::from(UPDATE_TIMER),
                    OsString::from(REPORT_TIMER),
                ],
            );
            for (name, _) in &files {
                remove_if_exists(&directory.join(name))?;
            }
            run_checked(
                runner,
                "systemctl",
                &[OsString::from("--user"), OsString::from("daemon-reload")],
            )?;
        }
        Action::Status => {}
    }
    Ok(())
}

fn status_with(
    platform: Platform,
    home: &Path,
    xdg_config_home: Option<&Path>,
    runner: &mut impl Runner,
) -> Result<ServiceStatus> {
    let (update_installed, report_installed, update_active, report_active) = match platform {
        Platform::Launchd => {
            let directory = home.join("Library/LaunchAgents");
            let domain = format!("gui/{}", unsafe { libc::geteuid() });
            (
                directory.join(format!("{UPDATE_LABEL}.plist")).is_file(),
                directory.join(format!("{REPORT_LABEL}.plist")).is_file(),
                command_succeeds(
                    runner,
                    "launchctl",
                    &[
                        OsString::from("print"),
                        OsString::from(format!("{domain}/{UPDATE_LABEL}")),
                    ],
                )?,
                command_succeeds(
                    runner,
                    "launchctl",
                    &[
                        OsString::from("print"),
                        OsString::from(format!("{domain}/{REPORT_LABEL}")),
                    ],
                )?,
            )
        }
        Platform::Systemd => {
            let directory = systemd_directory(home, xdg_config_home);
            (
                directory.join("historious-update.service").is_file()
                    && directory.join(UPDATE_TIMER).is_file(),
                directory.join("historious-report.service").is_file()
                    && directory.join(REPORT_TIMER).is_file(),
                systemd_timer_active(runner, UPDATE_TIMER)?,
                systemd_timer_active(runner, REPORT_TIMER)?,
            )
        }
    };
    Ok(ServiceStatus {
        backend: platform.name(),
        update: JobStatus {
            name: "update",
            schedule: "hourly",
            installed: update_installed,
            active: update_active,
        },
        report: JobStatus {
            name: "report",
            schedule: "daily at 03:00 local time",
            installed: report_installed,
            active: report_active,
        },
    })
}

fn systemd_timer_active(runner: &mut impl Runner, timer: &str) -> Result<bool> {
    command_succeeds(
        runner,
        "systemctl",
        &[
            OsString::from("--user"),
            OsString::from("is-active"),
            OsString::from("--quiet"),
            OsString::from(timer),
        ],
    )
}

fn command_succeeds(runner: &mut impl Runner, program: &str, args: &[OsString]) -> Result<bool> {
    Ok(runner.run(program, args)?.success)
}

fn run_checked(runner: &mut impl Runner, program: &str, args: &[OsString]) -> Result<()> {
    let result = runner.run(program, args)?;
    if result.success {
        Ok(())
    } else if result.stderr.is_empty() {
        bail!("{program} failed")
    } else {
        bail!("{program} failed: {}", result.stderr)
    }
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

#[derive(Clone, Copy)]
enum LaunchdSchedule {
    Hourly,
    Daily,
}

fn launchd_plist(
    label: &str,
    executable: &Path,
    data_dir: &Path,
    schedule: LaunchdSchedule,
) -> String {
    let command = match schedule {
        LaunchdSchedule::Hourly => "update",
        LaunchdSchedule::Daily => "report",
    };
    let extra_argument = match schedule {
        LaunchdSchedule::Hourly => String::new(),
        LaunchdSchedule::Daily => "    <string>--update</string>\n".to_string(),
    };
    let schedule_xml = match schedule {
        LaunchdSchedule::Hourly => {
            "  <key>StartInterval</key>\n  <integer>3600</integer>\n  <key>RunAtLoad</key>\n  <true/>\n"
                .to_string()
        }
        LaunchdSchedule::Daily => "  <key>StartCalendarInterval</key>\n  <dict>\n    <key>Hour</key>\n    <integer>3</integer>\n    <key>Minute</key>\n    <integer>0</integer>\n  </dict>\n".to_string(),
    };
    let log_path = data_dir.join(format!("service-{command}.log"));
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\">\n<dict>\n  <key>Label</key>\n  <string>{}</string>\n  <key>ProgramArguments</key>\n  <array>\n    <string>{}</string>\n    <string>--data-dir</string>\n    <string>{}</string>\n    <string>{command}</string>\n{extra_argument}  </array>\n{schedule_xml}  <key>StandardOutPath</key>\n  <string>{}</string>\n  <key>StandardErrorPath</key>\n  <string>{}</string>\n</dict>\n</plist>\n",
        xml_escape(label),
        xml_escape(&executable.to_string_lossy()),
        xml_escape(&data_dir.to_string_lossy()),
        xml_escape(&log_path.to_string_lossy()),
        xml_escape(&log_path.to_string_lossy()),
    )
}

fn systemd_directory(home: &Path, xdg_config_home: Option<&Path>) -> PathBuf {
    xdg_config_home
        .map(Path::to_path_buf)
        .unwrap_or_else(|| home.join(".config"))
        .join("systemd/user")
}

fn systemd_files(executable: &Path, data_dir: &Path) -> Vec<(&'static str, String)> {
    let prefix = format!(
        "{} --data-dir {}",
        systemd_quote(executable),
        systemd_quote(data_dir)
    );
    vec![
        (
            "historious-update.service",
            format!(
                "[Unit]\nDescription=Update Historious index\n\n[Service]\nType=oneshot\nExecStart={prefix} update\n"
            ),
        ),
        (
            UPDATE_TIMER,
            "[Unit]\nDescription=Update Historious index hourly\n\n[Timer]\nOnBootSec=5m\nOnUnitActiveSec=1h\nPersistent=true\nUnit=historious-update.service\n\n[Install]\nWantedBy=timers.target\n".to_string(),
        ),
        (
            "historious-report.service",
            format!(
                "[Unit]\nDescription=Refresh Historious report\n\n[Service]\nType=oneshot\nExecStart={prefix} report --update\n"
            ),
        ),
        (
            REPORT_TIMER,
            "[Unit]\nDescription=Refresh Historious report daily\n\n[Timer]\nOnCalendar=*-*-* 03:00:00\nPersistent=true\nUnit=historious-report.service\n\n[Install]\nWantedBy=timers.target\n".to_string(),
        ),
    ]
}

fn systemd_quote(path: &Path) -> String {
    format!(
        "\"{}\"",
        path.to_string_lossy()
            .replace('%', "%%")
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeRunner {
        calls: Vec<(String, Vec<String>)>,
        success: bool,
    }

    impl Runner for FakeRunner {
        fn run(&mut self, program: &str, args: &[OsString]) -> Result<CommandResult> {
            self.calls.push((
                program.to_string(),
                args.iter()
                    .map(|arg| arg.to_string_lossy().to_string())
                    .collect(),
            ));
            Ok(CommandResult {
                success: self.success,
                stderr: String::new(),
            })
        }
    }

    #[test]
    fn launchd_definitions_schedule_hourly_update_and_daily_report() {
        let executable = Path::new("/Applications/Histo & Tools/histo");
        let data_dir = Path::new("/Users/example/History & Search");
        let update = launchd_plist(UPDATE_LABEL, executable, data_dir, LaunchdSchedule::Hourly);
        assert!(update.contains("<integer>3600</integer>"));
        assert!(update.contains("<string>update</string>"));
        assert!(update.contains("Histo &amp; Tools"));
        assert!(update.contains("History &amp; Search"));

        let report = launchd_plist(REPORT_LABEL, executable, data_dir, LaunchdSchedule::Daily);
        assert!(report.contains("<integer>3</integer>"));
        assert!(report.contains("<string>report</string>"));
        assert!(report.contains("<string>--update</string>"));
    }

    #[test]
    fn systemd_definitions_schedule_hourly_update_and_daily_report() {
        let files = systemd_files(
            Path::new("/opt/Histo Tools/histo"),
            Path::new("/home/example/Histo Data"),
        );
        let update_service = &files[0].1;
        let update_timer = &files[1].1;
        let report_service = &files[2].1;
        let report_timer = &files[3].1;
        assert!(update_service.contains(
            "ExecStart=\"/opt/Histo Tools/histo\" --data-dir \"/home/example/Histo Data\" update"
        ));
        assert!(update_timer.contains("OnUnitActiveSec=1h"));
        assert!(update_timer.contains("Persistent=true"));
        assert!(report_service.contains("report --update"));
        assert!(report_timer.contains("OnCalendar=*-*-* 03:00:00"));
    }

    #[test]
    fn systemd_lifecycle_is_idempotent_in_an_isolated_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let home = dir.path().join("home");
        let config = dir.path().join("config");
        let executable = Path::new("/usr/local/bin/histo");
        let data_dir = dir.path().join("data");
        let mut runner = FakeRunner {
            success: true,
            ..FakeRunner::default()
        };

        let installed = manage_with(
            Platform::Systemd,
            Action::Install,
            &home,
            Some(&config),
            executable,
            &data_dir,
            &mut runner,
        )
        .expect("install service definitions");
        assert!(installed.update.installed && installed.update.active);
        assert!(installed.report.installed && installed.report.active);
        assert!(config
            .join("systemd/user/historious-update.timer")
            .is_file());
        assert!(runner
            .calls
            .iter()
            .any(|(_, args)| args.contains(&"enable".to_string())));

        manage_with(
            Platform::Systemd,
            Action::Install,
            &home,
            Some(&config),
            executable,
            &data_dir,
            &mut runner,
        )
        .expect("repeat service install");
        let removed = manage_with(
            Platform::Systemd,
            Action::Uninstall,
            &home,
            Some(&config),
            executable,
            &data_dir,
            &mut runner,
        )
        .expect("uninstall service definitions");
        assert!(!removed.update.installed);
        assert!(!removed.report.installed);
        assert!(!config.join("systemd/user/historious-update.timer").exists());
    }
}
