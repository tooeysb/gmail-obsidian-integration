"""
Integration tests for CorrelationIdMiddleware.

Verifies that every request receives an X-Request-ID response header,
that client-supplied IDs are echoed back unchanged, and that
server-generated IDs conform to the expected 8-character format.
"""

from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app, raise_server_exceptions=True)


class TestCorrelationIdMiddleware:
    """Tests for CorrelationIdMiddleware behaviour via the public root endpoint."""

    def test_response_includes_request_id_header(self):
        """Every response must contain an X-Request-ID header."""
        response = client.get("/")
        assert response.status_code == 200
        assert "x-request-id" in response.headers, "X-Request-ID header missing from response"
        # Value must be a non-empty string
        assert response.headers["x-request-id"].strip() != ""

    def test_custom_request_id_preserved(self):
        """A client-supplied X-Request-ID must be echoed back unchanged."""
        custom_id = "abc-12345"
        response = client.get("/", headers={"X-Request-ID": custom_id})
        assert response.status_code == 200
        assert response.headers["x-request-id"] == custom_id, (
            f"Expected X-Request-ID '{custom_id}', " f"got '{response.headers.get('x-request-id')}'"
        )

    def test_generated_request_id_is_short(self):
        """Without a client-supplied header the middleware generates an 8-char ID."""
        # Make a request without X-Request-ID so the middleware auto-generates one
        response = client.get("/")
        assert response.status_code == 200
        generated_id = response.headers["x-request-id"]
        assert (
            len(generated_id) == 8
        ), f"Expected auto-generated ID length of 8, got {len(generated_id)}: '{generated_id}'"

    def test_different_requests_get_different_ids(self):
        """Two requests without client headers should receive distinct IDs."""
        r1 = client.get("/")
        r2 = client.get("/")
        id1 = r1.headers["x-request-id"]
        id2 = r2.headers["x-request-id"]
        assert id1 != id2, "Successive auto-generated IDs should differ; " f"both were '{id1}'"

    def test_custom_id_any_value_preserved(self):
        """Middleware must not mutate the client-supplied header value."""
        # Use a longer, UUID-style value to confirm no truncation occurs
        long_id = "550e8400-e29b-41d4-a716-446655440000"
        response = client.get("/", headers={"X-Request-ID": long_id})
        assert response.headers["x-request-id"] == long_id

    def test_header_present_on_multiple_endpoints(self):
        """X-Request-ID should appear on all routed endpoints, not just root."""
        for path in ["/", "/health"]:
            response = client.get(path)
            assert "x-request-id" in response.headers, f"X-Request-ID missing on {path}"
