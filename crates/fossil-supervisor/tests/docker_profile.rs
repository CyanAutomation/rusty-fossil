use fossil_core::execution::SyntheticScenario;
use fossil_core::ids::{ClaimCallbackId, EventId, RunId, WorkerId};
use fossil_storage::{ContainerLaunchRecord, ContainerLaunchState, WorkloadKind};
use fossil_supervisor::{DockerConfigError, DockerRuntimeConfig};
use std::collections::HashMap;

const IMAGE: &str = "ghcr.io/cyanautomation/rusty-fossil-synthetic-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn launch(scenario: SyntheticScenario) -> ContainerLaunchRecord {
    ContainerLaunchRecord {
        run_id: RunId::parse("f8a0c4c5-d628-7b93-a7cf-4b4629a28853").unwrap(),
        worker_id: WorkerId::parse("fossil-host-01").unwrap(),
        claim_callback_id: ClaimCallbackId::parse(
            "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133",
        )
        .unwrap(),
        start_callback_id: EventId::parse("15870068-6723-4b70-9e9f-53b8271e032d").unwrap(),
        workload_kind: WorkloadKind::Synthetic,
        workload_version: 1,
        scenario,
        image_identity: Some(IMAGE.to_owned()),
        profile_version: "synthetic-v1".to_owned(),
        effective_policy_json: "{}".to_owned(),
        repository_url: None,
        authorized_git_ref: None,
        resolved_commit: None,
        workspace_identity: None,
        runtime_limit_ms: Some(900_000),
        effective_deadline_at_ms: None,
        container_name: "rusty-fossil-synth-f8a0c4c5-d628-7b93-a7cf-4b4629a28853-15870068-6723-4b70-9e9f-53b8271e032d".to_owned(),
        container_id: None,
        state: ContainerLaunchState::LaunchIntentPersisted,
        exit_code: None,
        outcome_class: None,
        diagnostic_summary: None,
        output_truncated: false,
        terminal_callback_id: None,
        stop_reason: None,
        stop_requested_at_ms: None,
        started_at_ms: None,
        completed_at_ms: None,
        updated_at_ms: 1_798_000_000_000,
    }
}

#[test]
fn synthetic_container_profile_is_fixed_and_restrictive() {
    let config = DockerRuntimeConfig::new(IMAGE, "/var/run/docker.sock").unwrap();
    let request = config
        .container_config(&launch(SyntheticScenario::Success))
        .unwrap();
    assert_eq!(request.image.as_deref(), Some(IMAGE));
    assert_eq!(request.user.as_deref(), Some("65532:65532"));
    assert!(request.cmd.is_none());
    assert!(request.entrypoint.is_none());
    assert_eq!(
        request.env,
        Some(vec!["FOSSIL_SCENARIO=success".to_owned()])
    );

    let labels = request.labels.unwrap();
    assert_eq!(labels["io.rusty-fossil.managed"], "true");
    assert_eq!(
        labels["io.rusty-fossil.run-id"],
        "f8a0c4c5-d628-7b93-a7cf-4b4629a28853"
    );
    assert_eq!(labels["io.rusty-fossil.worker-id"], "fossil-host-01");
    assert_eq!(
        labels["io.rusty-fossil.claim-callback-id"],
        "d744c5a2-1d8f-4a2e-bc4d-6813d6fc0133"
    );
    assert!(!labels.keys().any(|key| key.contains("prompt")));

    let host = request.host_config.unwrap();
    assert_eq!(host.privileged, Some(false));
    assert_eq!(host.readonly_rootfs, Some(true));
    assert_eq!(host.cap_drop, Some(vec!["ALL".to_owned()]));
    assert_eq!(
        host.security_opt,
        Some(vec!["no-new-privileges:true".to_owned()])
    );
    assert_eq!(host.network_mode.as_deref(), Some("none"));
    assert_eq!(host.memory, Some(256 * 1024 * 1024));
    assert_eq!(host.nano_cpus, Some(500_000_000));
    assert_eq!(host.pids_limit, Some(32));
    assert_eq!(host.oom_kill_disable, Some(false));
    assert!(host.devices.is_none());
    assert!(host.binds.is_none());
    assert!(host.port_bindings.is_none());
    let tmpfs: HashMap<_, _> = host.tmpfs.unwrap();
    assert_eq!(tmpfs["/tmp"], "rw,noexec,nosuid,nodev,size=16777216");
}

#[test]
fn synthetic_docker_config_rejects_pi_coding_launch_identity() {
    let config = DockerRuntimeConfig::new(IMAGE, "/var/run/docker.sock").unwrap();
    let mut pi_launch = launch(SyntheticScenario::Success);
    pi_launch.workload_kind = WorkloadKind::PiCodingV1;
    pi_launch.profile_version = "pi-coding-v1".to_owned();
    assert!(matches!(
        config.container_config(&pi_launch),
        Err(DockerConfigError::UnsupportedWorkload)
    ));
}

#[test]
fn container_image_must_be_pinned_by_digest() {
    assert!(
        DockerRuntimeConfig::new(
            "ghcr.io/cyanautomation/rusty-fossil-synthetic-worker:latest",
            "/var/run/docker.sock"
        )
        .is_err()
    );
    assert!(
        DockerRuntimeConfig::new(
            "ghcr.io/cyanautomation/rusty-fossil-synthetic-worker@sha256:bad",
            "/var/run/docker.sock"
        )
        .is_err()
    );
    assert!(
        DockerRuntimeConfig::new(
            "untrusted.example/other-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "/var/run/docker.sock"
        )
        .is_err()
    );
    assert!(
        DockerRuntimeConfig::new(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "/var/run/docker.sock"
        )
        .is_err()
    );
}

#[test]
fn controlled_scenario_is_the_only_worker_input() {
    for (scenario, expected) in [
        (SyntheticScenario::Success, "success"),
        (SyntheticScenario::ControlledFailure, "controlled_failure"),
        (SyntheticScenario::Timeout, "timeout"),
        (SyntheticScenario::Cancellation, "cancellation"),
        (SyntheticScenario::BoundedOutput, "bounded_output"),
        (
            SyntheticScenario::UnexpectedTermination,
            "unexpected_termination",
        ),
    ] {
        let request = DockerRuntimeConfig::new(IMAGE, "/var/run/docker.sock")
            .unwrap()
            .container_config(&launch(scenario))
            .unwrap();
        assert_eq!(
            request.env.unwrap(),
            vec![format!("FOSSIL_SCENARIO={expected}")]
        );
    }
}
