use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

const WORKLOAD_ID: &str = "rusty-fossil-synthetic-v1";
const SCENARIOS: &[&str] = &[
    "success",
    "controlled_failure",
    "timeout",
    "cancellation",
    "bounded_output",
    "unexpected_termination",
];

fn main() -> ExitCode {
    let scenario = std::env::var("FOSSIL_SCENARIO").unwrap_or_default();
    let started = Instant::now();
    if !is_supported_scenario(&scenario) {
        eprintln!("synthetic worker received an unsupported scenario");
        return ExitCode::from(64);
    }
    emit("started", &scenario, None);
    match scenario.as_str() {
        "success" => {
            emit("progress", &scenario, Some("synthetic work is progressing"));
            emit("completed", &scenario, Some(&elapsed(started)));
            ExitCode::SUCCESS
        }
        "controlled_failure" => {
            emit("failed", &scenario, Some("controlled exit status 17"));
            ExitCode::from(17)
        }
        "timeout" | "cancellation" => loop {
            thread::sleep(Duration::from_secs(60));
        },
        "bounded_output" => {
            let line = "x".repeat(1024);
            for _ in 0..2048 {
                println!("{line}");
            }
            emit("completed", &scenario, Some("bounded output emitted"));
            ExitCode::SUCCESS
        }
        "unexpected_termination" => std::process::abort(),
        _ => unreachable!("scenario was checked against the fixed set"),
    }
}

fn is_supported_scenario(value: &str) -> bool {
    SCENARIOS.contains(&value)
}

fn emit(event: &str, scenario: &str, detail: Option<&str>) {
    let detail = detail.unwrap_or("");
    println!(
        "{{\"workloadId\":\"{WORKLOAD_ID}\",\"event\":\"{event}\",\"scenario\":\"{scenario}\",\"detail\":\"{detail}\"}}"
    );
}

fn elapsed(started: Instant) -> String {
    format!("elapsed_ms={}", started.elapsed().as_millis())
}

#[cfg(test)]
mod tests {
    use super::{SCENARIOS, is_supported_scenario};

    #[test]
    fn worker_scenarios_are_a_closed_set() {
        assert!(
            SCENARIOS
                .iter()
                .all(|scenario| is_supported_scenario(scenario))
        );
        assert!(!is_supported_scenario("run-shell-command"));
    }
}
