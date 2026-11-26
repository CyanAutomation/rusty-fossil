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
            .set_body_string(oversized_json_body(bytes)),
        HttpFaultScenario::Delayed { duration } => ResponseTemplate::new(200)
            .set_delay(duration)
            .set_body_string("{}"),
    };
    Mock::given(method(request_method))
        .and(path(request_path))
        .respond_with(response)
        // Exact request counts also prove fault scenarios do not trigger retries.
        .expect(1)
        .mount(server)
        .await;
}

fn oversized_json_body(bytes: usize) -> String {
    const PREFIX: &str = "{\"padding\":\"";
    const SUFFIX: &str = "\"}";
    let padding_bytes = bytes.saturating_sub(PREFIX.len() + SUFFIX.len());
    format!("{PREFIX}{}{SUFFIX}", "x".repeat(padding_bytes))
}

#[cfg(test)]
mod tests {
    use super::oversized_json_body;

    #[test]
    fn oversized_body_is_valid_json_at_the_requested_size() {
        let bytes = 96 * 1024 + 1;
        let body = oversized_json_body(bytes);

        assert!(body.len() >= bytes);
        assert!(serde_json::from_str::<serde_json::Value>(&body).is_ok());
    }
}
