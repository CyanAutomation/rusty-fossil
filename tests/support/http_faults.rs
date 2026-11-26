use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub enum HttpFaultScenario {
    Status(u16),
    MalformedJson,
    Oversized { bytes: usize },
    Delayed { duration: Duration },
}

/// Mounts exactly one deterministic upstream fault for a method and path.
pub async fn mount_http_fault(
    server: &MockServer,
    request_method: &str,
    request_path: &str,
    scenario: HttpFaultScenario,
) {
    let response = match scenario {
        HttpFaultScenario::Status(status) => {
            ResponseTemplate::new(status).set_body_string("PRIVATE_UPSTREAM_DIAGNOSTIC")
        }
        HttpFaultScenario::MalformedJson => ResponseTemplate::new(200)
            .insert_header("content-type", "application/json")
            .set_body_string("{"),
        HttpFaultScenario::Oversized { bytes } => ResponseTemplate::new(200)
            .insert_header("content-type", "application/json")
            .set_body_string("x".repeat(bytes)),
        HttpFaultScenario::Delayed { duration } => ResponseTemplate::new(200)
            .set_delay(duration)
            .set_body_string("{}"),
    };
    Mock::given(method(request_method))
        .and(path(request_path))
        .respond_with(response)
        .expect(1)
        .mount(server)
        .await;
}
