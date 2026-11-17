"""M7 HTTP 接口：页面可达、JSON 契约、DomainError -> 422、端到端归档复算。"""

LOG_TEXT = (
    '{"request_id":"a1","timestamp":"2026-11-15T01:05:00Z","service":"pay","status":200}\n'
    '{"request_id":"a2","timestamp":"2026-11-15T01:59:00Z","service":"pay","status":500}\n'
    '{"request_id":"a3","timestamp":"2026-11-15T02:05:00Z","service":"pay","status":200}'
)


def test_archive_page_renders(client):
    response = client.get("/archive")
    assert response.status_code == 200
    assert "panel" in response.text


def test_status_endpoint_reports_four_states_and_usage(client):
    data = client.get("/api/archive/status").json()
    assert set(data["states"]) == {"HOT", "MIGRATING", "ARCHIVED", "FAILED"}
    assert data["hot_used_bytes"] == 0
    assert data["hot_quota_bytes"] == 512
    assert data["online_bytes"] == 0


def test_full_ingest_migrate_readback_rebuild_flow(client):
    ingested = client.post("/api/archive/ingest", json={
        "batch_id": "b1", "text": LOG_TEXT, "source_name": "api-test",
        "ingest_key": "k1"}).json()
    assert ingested["state"] == "HOT" and ingested["line_count"] == 3

    # 幂等接入：同 ingest_key 不产生第二批
    again = client.post("/api/archive/ingest", json={
        "batch_id": "other", "text": LOG_TEXT, "ingest_key": "k1"}).json()
    assert again["idempotent"] is True and again["batch_id"] == "b1"

    migrated = client.post("/api/archive/migrate", json={"batch_id": "b1"}).json()
    assert migrated["state"] == "ARCHIVED"
    # 重试迁移不重复归档
    retry = client.post("/api/archive/migrate", json={"batch_id": "b1"}).json()
    assert retry["idempotent"] is True

    status = client.get("/api/archive/status").json()
    assert status["hot_used_bytes"] == 0
    assert status["states"]["ARCHIVED"]["batches"] == 1
    assert status["cold_used_bytes"] > 0

    rb = client.post("/api/archive/readback", json={"batch_id": "b1"}).json()
    assert rb["location"] == "COLD"
    assert [line["source_line"] for line in rb["lines"]] == [1, 2, 3]

    verify = client.post("/api/archive/verify", json={"batch_id": "b1"}).json()
    assert verify["ok"] is True

    rebuilt = client.post("/api/archive/rebuild-buckets",
                          json={"batch_id": "b1"}).json()
    assert rebuilt["conserved"] is True
    assert rebuilt["bucketed_count"] == 3
    hours = {b["hour"]: b for b in rebuilt["buckets"]}
    assert hours["2026-11-15T01:00:00Z"]["errors"] == 1

    batches = client.get("/api/archive/batches").json()["batches"]
    assert len(batches) == 1 and batches[0]["state"] == "ARCHIVED"


def test_hot_quota_returns_422_and_no_half_batch(client):
    small = '{"request_id":"x"}'
    client.post("/api/archive/ingest", json={"batch_id": "keep", "text": small})
    big = "\n".join(
        '{"request_id":"%03d","timestamp":"2026-11-15T01:00:00Z","service":"pay","status":200}' % i
        for i in range(40))
    resp = client.post("/api/archive/ingest",
                       json={"batch_id": "too-big", "text": big})
    assert resp.status_code == 422
    assert resp.json()["error"] == "HOT_QUOTA_EXCEEDED"
    batches = client.get("/api/archive/batches").json()["batches"]
    assert [b["batch_id"] for b in batches] == ["keep"]


def test_cold_corruption_returns_explicit_422(client, tmp_path):
    client.post("/api/archive/ingest", json={"batch_id": "b1", "text": LOG_TEXT})
    client.post("/api/archive/migrate", json={"batch_id": "b1"})
    arc_files = list((tmp_path / "cold").glob("*.loghour.arc"))
    path = arc_files[0]
    blob = bytearray(path.read_bytes())
    blob[-16] ^= 0x01
    path.write_bytes(bytes(blob))
    resp = client.post("/api/archive/readback", json={"batch_id": "b1"})
    assert resp.status_code == 422
    assert resp.json()["error"] == "COLD_ARCHIVE_CORRUPT"


def test_unknown_batch_422(client):
    resp = client.post("/api/archive/readback", json={"batch_id": "nope"})
    assert resp.status_code == 422
    assert resp.json()["error"] == "BATCH_NOT_FOUND"
