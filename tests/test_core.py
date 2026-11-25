"""Tests for tap-spicedb against a fake SpiceDB gRPC client.

The fake returns real authzed protobuf messages, so the conversion code runs as in
production. Run a POC against a real SpiceDB as the README describes.
"""

import io
import itertools
import json
import threading
import time
from contextlib import redirect_stdout
from typing import Any, Dict, List, Optional

import grpc
import pytest
from authzed.api.v1 import (
    ContextualizedCaveat,
    ObjectReference,
    ReadSchemaResponse,
    ReflectSchemaResponse,
    Relationship,
    RelationshipUpdate,
    SubjectReference,
    WatchResponse,
    ZedToken,
)
from authzed.api.v1.core_pb2 import Cursor
from authzed.api.v1.permission_service_pb2 import ExportBulkRelationshipsResponse
from authzed.api.v1.schema_service_pb2 import (
    ReflectionCaveat,
    ReflectionCaveatParameter,
    ReflectionDefinition,
    ReflectionPermission,
    ReflectionRelation,
    ReflectionTypeReference,
)
from google.protobuf.struct_pb2 import Struct
from singer_sdk.exceptions import ConfigValidationError

from tap_spicedb import client as client_module
from tap_spicedb.tap import TapSpiceDB

SNAPSHOT_TOKEN = "snap-1"
HOST = "spicedb.test"
IP_HOST = "127.0.0.1"
PORT = 50051
HOST_PORT = f"{HOST}:{PORT}"
BASE_CONFIG = {
    "host": HOST_PORT,
    "token": "test-key",
    "insecure": True,
    "watch_idle_timeout_seconds": 1,
    "watch_start_timeout_seconds": 1,
    "watch_max_duration_seconds": 10,
}


def rel(resource: str, relation: str, subject: str, **kwargs: Any) -> Relationship:
    """Build a Relationship from 'type:id' strings; subject may be 'type:id#rel'."""
    r_type, r_id = resource.split(":")
    s_obj, _, s_rel = subject.partition("#")
    s_type, s_id = s_obj.split(":")
    r = Relationship(
        resource=ObjectReference(object_type=r_type, object_id=r_id),
        relation=relation,
        subject=SubjectReference(
            object=ObjectReference(object_type=s_type, object_id=s_id),
            optional_relation=s_rel,
        ),
    )
    if "caveat" in kwargs:
        context = Struct()
        context.update(kwargs["context"])
        r.optional_caveat.CopyFrom(
            ContextualizedCaveat(caveat_name=kwargs["caveat"], context=context)
        )
    return r


class FakeRpcError(grpc.RpcError):
    """An RpcError with a status code, as grpc raises."""

    def __init__(self, code: grpc.StatusCode, details: str = "") -> None:
        """Store the code and details."""
        self._code, self._details = code, details

    def code(self) -> grpc.StatusCode:
        """Return the status code."""
        return self._code

    def details(self) -> str:
        """Return the details."""
        return self._details


class FakeWatchCall:
    """A Watch stream: yields the responses, then stays open until cancelled."""

    def __init__(self, items: List[Any]) -> None:
        """Store the responses (or an exception to raise)."""
        self.items = items
        self.cancelled = threading.Event()

    def __iter__(self) -> Any:
        """Yield the responses, then block like an idle stream."""
        for item in self.items:
            if isinstance(item, Exception):
                raise item
            yield item
        self.cancelled.wait()

    def cancel(self) -> None:
        """Close the stream."""
        self.cancelled.set()


class HeartbeatWatchCall(FakeWatchCall):
    """A Watch on PostgreSQL: the responses, then a heartbeat checkpoint every interval.

    SpiceDB writes a heartbeat revision every quantization interval (5 s by default)
    and Watch sends a checkpoint for each one, so the stream is never silent.
    """

    def __init__(self, items: List[Any], interval: float) -> None:
        """Store the responses, the interval and when each token was first sent."""
        super().__init__(items)
        self.interval = interval
        self.sent_at: Dict[str, float] = {}

    def __iter__(self) -> Any:
        """Yield each response, then heartbeat checkpoints, interval apart."""
        heartbeats = (
            watch_response(f"hb{n}", checkpoint=True) for n in itertools.count(1)
        )
        for item in itertools.chain(self.items, heartbeats):
            if self.cancelled.wait(self.interval):
                return
            self.sent_at.setdefault(item.changes_through.token, time.monotonic())
            yield item


class FakeClient:
    """Stands in for authzed's SyncClient."""

    def __init__(self) -> None:
        """Set up a small dataset."""
        self.relationships = [
            rel("document:1", "viewer", "user:alice"),
            rel("document:1", "viewer", "group:eng#member"),
            rel(
                "document:2",
                "viewer",
                "user:bob",
                caveat="ip_allowlist",
                context={"cidr": "10.0.0.0/8", "nested": {"a": 1}},
            ),
            rel("group:eng", "member", "user:carol"),
        ]
        self.watch_items: List[Any] = []
        self.heartbeat_interval: Optional[float] = None
        self.watch_call: Any = None
        self.export_errors: List[Exception] = []
        self.export_requests: List[Any] = []
        self.watch_requests: List[Any] = []
        self.expired_tokens: List[str] = []
        self.invalid_tokens: List[str] = []

    def ReadSchema(self, request: Any, timeout: Optional[float] = None) -> Any:
        """Return the schema text and the snapshot token."""
        return ReadSchemaResponse(
            schema_text="definition user {}", read_at=ZedToken(token=SNAPSHOT_TOKEN)
        )

    def ReflectSchema(self, request: Any, timeout: Optional[float] = None) -> Any:
        """Return a small reflected schema; OUT_OF_RANGE for an expired token."""
        if request.consistency.at_exact_snapshot.token in self.expired_tokens:
            raise FakeRpcError(grpc.StatusCode.OUT_OF_RANGE, "invalid revision")
        if request.consistency.at_exact_snapshot.token in self.invalid_tokens:
            raise FakeRpcError(
                grpc.StatusCode.INVALID_ARGUMENT, "invalid revision requested"
            )
        return ReflectSchemaResponse(
            definitions=[
                ReflectionDefinition(name="user"),
                ReflectionDefinition(
                    name="document",
                    comment="// a document",
                    relations=[
                        ReflectionRelation(
                            name="viewer",
                            parent_definition_name="document",
                            subject_types=[
                                ReflectionTypeReference(
                                    subject_definition_name="user",
                                    is_terminal_subject=True,
                                    optional_caveat_name="ip_allowlist",
                                ),
                                ReflectionTypeReference(
                                    subject_definition_name="group",
                                    optional_relation_name="member",
                                ),
                            ],
                        )
                    ],
                    permissions=[
                        ReflectionPermission(
                            name="view", parent_definition_name="document"
                        )
                    ],
                ),
            ],
            caveats=[
                ReflectionCaveat(
                    name="ip_allowlist",
                    expression="cidr == '10.0.0.0/8'",
                    parameters=[
                        ReflectionCaveatParameter(
                            name="cidr",
                            type="string",
                            parent_caveat_name="ip_allowlist",
                        )
                    ],
                )
            ],
        )

    def ExportBulkRelationships(self, request: Any) -> Any:
        """Stream the relationships in pages of two, failing first if asked to."""
        self.export_requests.append(request)
        assert request.consistency.at_exact_snapshot.token == SNAPSHOT_TOKEN
        rels = [
            r
            for r in self.relationships
            if not request.optional_relationship_filter.resource_type
            or r.resource.object_type
            == request.optional_relationship_filter.resource_type
        ]
        start = int(request.optional_cursor.token or 0)
        for i in range(start, len(rels), 2):
            if self.export_errors and i > start:
                raise self.export_errors.pop(0)
            yield ExportBulkRelationshipsResponse(
                relationships=rels[i : i + 2],
                after_result_cursor=Cursor(token=str(i + 2)),
            )

    def Watch(self, request: Any) -> FakeWatchCall:
        """Return a stream of the configured responses, with heartbeats if set."""
        self.watch_requests.append(request)
        if self.heartbeat_interval:
            self.watch_call = HeartbeatWatchCall(
                self.watch_items, self.heartbeat_interval
            )
        else:
            self.watch_call = FakeWatchCall(self.watch_items)
        return self.watch_call


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    """Replace the gRPC client with a fake."""
    fake_client = FakeClient()
    monkeypatch.setattr(
        client_module.SpiceDBConnection, "_build_client", lambda self: fake_client
    )
    monkeypatch.setattr(client_module.time, "sleep", lambda s: None)
    return fake_client


def run_tap(config: Dict[str, Any], state: Optional[dict] = None) -> Dict[str, Any]:
    """Run a sync; return records by stream and the last state."""
    tap = TapSpiceDB(config={**BASE_CONFIG, **config}, state=state or {})
    out = io.StringIO()
    with redirect_stdout(out):
        tap.sync_all()
    records: Dict[str, List[dict]] = {}
    last_state: dict = {}
    for line in out.getvalue().splitlines():
        message = json.loads(line)
        if message["type"] == "RECORD":
            records.setdefault(message["stream"], []).append(message["record"])
        elif message["type"] == "STATE":
            last_state = message["value"]
    return {"records": records, "state": last_state}


def bookmark(state: dict) -> Optional[str]:
    """Return the relationships stream's ZedToken bookmark."""
    return state["bookmarks"]["relationships"].get("zed_token")


def watch_response(
    token: str,
    *updates: Any,
    checkpoint: bool = False,
    metadata: Optional[dict] = None,
) -> Any:
    """Build a WatchResponse."""
    response = WatchResponse(
        updates=[RelationshipUpdate(operation=op, relationship=r) for op, r in updates],
        changes_through=ZedToken(token=token),
        is_checkpoint=checkpoint,
    )
    if metadata:
        response.optional_transaction_metadata.update(metadata)
    return response


def test_first_run_takes_a_snapshot(fake: FakeClient) -> None:
    """With no state, every relationship is read at one revision, and it is saved."""
    result = run_tap({})
    rels = result["records"]["relationships"]
    assert len(rels) == 4
    assert {r["operation"] for r in rels} == {"SNAPSHOT"}
    assert {r["revision"] for r in rels} == {SNAPSHOT_TOKEN}
    assert rels[1]["subject_relation"] == "member"
    assert rels[0]["subject_relation"] == "..."
    assert rels[2]["caveat_name"] == "ip_allowlist"
    assert rels[2]["caveat_context"] == {"cidr": "10.0.0.0/8", "nested": {"a": 1}}
    assert all(r["_sdc_deleted_at"] is None for r in rels)
    assert bookmark(result["state"]) == SNAPSHOT_TOKEN
    assert fake.watch_requests == []


def test_schema_streams(fake: FakeClient) -> None:
    """Schema text and the reflected definitions, relations, permissions, caveats."""
    records = run_tap({})["records"]
    assert records["schema"][0]["schema_text"] == "definition user {}"
    assert len(records["schema"][0]["schema_hash"]) == 64
    assert [d["definition_name"] for d in records["schema_definitions"]] == [
        "user",
        "document",
    ]
    relation = records["schema_relations"][0]
    assert (relation["definition_name"], relation["relation_name"]) == (
        "document",
        "viewer",
    )
    assert relation["subject_types"][0]["caveat_name"] == "ip_allowlist"
    assert relation["subject_types"][1]["subject_relation_name"] == "member"
    assert records["schema_permissions"][0]["permission_name"] == "view"
    caveat = records["schema_caveats"][0]
    assert caveat["parameters"] == [{"name": "cidr", "type": "string"}]


def test_incremental_run_watches_changes_including_deletes(fake: FakeClient) -> None:
    """With a saved token, Watch changes are emitted and the token advances."""
    alice = rel("document:1", "viewer", "user:alice")
    dave = rel("document:3", "viewer", "user:dave")
    fake.watch_items = [
        watch_response("t2", (RelationshipUpdate.OPERATION_CREATE, dave)),
        watch_response(
            "t3",
            (RelationshipUpdate.OPERATION_DELETE, alice),
            (RelationshipUpdate.OPERATION_TOUCH, dave),
        ),
        watch_response("t3", checkpoint=True),
    ]
    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    result = run_tap({}, state)

    assert fake.watch_requests[0].optional_start_cursor.token == SNAPSHOT_TOKEN
    rels = result["records"]["relationships"]
    assert [(r["operation"], r["resource_id"]) for r in rels] == [
        ("CREATE", "3"),
        ("DELETE", "1"),
        ("TOUCH", "3"),
    ]
    assert rels[1]["_sdc_deleted_at"] is not None
    assert rels[0]["_sdc_deleted_at"] is None
    assert rels[1]["revision"] == "t3"
    assert bookmark(result["state"]) == "t3"
    assert fake.export_requests == []


def test_no_changes_keeps_the_token(fake: FakeClient) -> None:
    """An idle Watch emits nothing and keeps the saved token."""
    state = {"bookmarks": {"relationships": {"zed_token": "t9"}}}
    result = run_tap({}, state)
    assert "relationships" not in result["records"]
    assert bookmark(result["state"]) == "t9"


def test_stopping_on_max_duration_keeps_only_complete_revisions(
    fake: FakeClient,
) -> None:
    """On the time limit the last revision isn't saved: it may be incomplete."""
    dave = rel("document:3", "viewer", "user:dave")
    fake.watch_items = [
        watch_response("t2", (RelationshipUpdate.OPERATION_CREATE, dave)),
        watch_response("t3", (RelationshipUpdate.OPERATION_DELETE, dave)),
    ]
    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    result = run_tap(
        {
            "watch_idle_timeout_seconds": 5,
            "watch_start_timeout_seconds": 5,
            "watch_max_duration_seconds": 1,
        },
        state,
    )
    assert len(result["records"]["relationships"]) == 2
    assert bookmark(result["state"]) == "t2"


def test_idle_end_saves_the_last_revision(fake: FakeClient) -> None:
    """Without a checkpoint, an idle stream means the last revision is complete."""
    dave = rel("document:3", "viewer", "user:dave")
    fake.watch_items = [
        watch_response("t2", (RelationshipUpdate.OPERATION_CREATE, dave))
    ]
    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    assert bookmark(run_tap({}, state)["state"]) == "t2"


def test_checkpoints_alone_end_the_run_as_caught_up(fake: FakeClient) -> None:
    """Heartbeat checkpoints move the token forward but don't keep the run going."""
    fake.heartbeat_interval = 0.05
    config = {**BASE_CONFIG, "watch_max_duration_seconds": 5}
    items = list(TapSpiceDB(config=config).connection.watch(SNAPSHOT_TOKEN, []))
    assert items[-1] == (None, True)
    assert all(r.is_checkpoint and not r.updates for r, _ in items[:-1])

    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    result = run_tap({"watch_max_duration_seconds": 5}, state)
    assert "relationships" not in result["records"]
    assert bookmark(result["state"]).startswith("hb")


def test_a_change_restarts_the_wait_between_checkpoints(fake: FakeClient) -> None:
    """Between checkpoints, the run ends the idle timeout after the last change."""
    dave = rel("document:3", "viewer", "user:dave")
    fake.heartbeat_interval = 0.05
    fake.watch_items = [watch_response(f"q{n}", checkpoint=True) for n in range(4)]
    fake.watch_items += [
        watch_response("t2", (RelationshipUpdate.OPERATION_CREATE, dave)),
        watch_response("t2", checkpoint=True),
    ]
    config = {**BASE_CONFIG, "watch_max_duration_seconds": 5}
    items = list(TapSpiceDB(config=config).connection.watch(SNAPSHOT_TOKEN, []))
    ended = time.monotonic()
    assert items[-1] == (None, True)
    assert [r.changes_through.token for r, _ in items[:-1] if r.updates] == ["t2"]
    assert fake.watch_call is not None
    idle = BASE_CONFIG["watch_idle_timeout_seconds"]
    assert ended - fake.watch_call.sent_at["t2"] >= idle


def test_expired_token_falls_back_to_a_snapshot(fake: FakeClient) -> None:
    """A token past the GC window (OUT_OF_RANGE) triggers a new snapshot."""
    fake.expired_tokens = ["old"]
    state = {"bookmarks": {"relationships": {"zed_token": "old"}}}
    result = run_tap({}, state)
    rels = result["records"]["relationships"]
    assert len(rels) == 4
    assert {r["operation"] for r in rels} == {"SNAPSHOT"}
    assert bookmark(result["state"]) == SNAPSHOT_TOKEN
    assert fake.watch_requests == []


def test_expired_token_fails_when_configured(fake: FakeClient) -> None:
    """With on_expired_token=fail the run stops instead."""
    fake.expired_tokens = ["old"]
    state = {"bookmarks": {"relationships": {"zed_token": "old"}}}
    with pytest.raises(client_module.ExpiredZedTokenError):
        run_tap({"on_expired_token": "fail"}, state)


def test_invalid_token_is_handled_like_an_expired_one(fake: FakeClient) -> None:
    """A malformed token (INVALID_ARGUMENT) takes a snapshot, or fails cleanly."""
    fake.invalid_tokens = ["garbage"]
    state = {"bookmarks": {"relationships": {"zed_token": "garbage"}}}
    result = run_tap({}, state)
    assert bookmark(result["state"]) == SNAPSHOT_TOKEN
    assert fake.watch_requests == []
    with pytest.raises(client_module.ExpiredZedTokenError, match="INVALID_ARGUMENT"):
        run_tap({"on_expired_token": "fail"}, state)


def test_a_missing_watch_service_is_explained(fake: FakeClient) -> None:
    """Without track_commit_timestamp, v1.48 returns UNIMPLEMENTED for WatchService."""
    fake.watch_items = [
        FakeRpcError(
            grpc.StatusCode.UNIMPLEMENTED,
            "unknown service authzed.api.v1.WatchService",
        )
    ]
    state = {"bookmarks": {"relationships": {"zed_token": "t1"}}}
    with pytest.raises(client_module.WatchUnavailableError) as error:
        run_tap({}, state)
    assert "track_commit_timestamp" in str(error.value)
    assert HOST_PORT in str(error.value)
    assert error.value.__cause__ is None


def test_watch_errors_other_than_expiry_fail_the_run(fake: FakeClient) -> None:
    """Any other non-retryable Watch error stops the run, as raised."""
    fake.watch_items = [
        FakeRpcError(grpc.StatusCode.FAILED_PRECONDITION, "watch is disabled")
    ]
    state = {"bookmarks": {"relationships": {"zed_token": "t1"}}}
    with pytest.raises(grpc.RpcError):
        run_tap({}, state)


def test_export_resumes_from_cursor_after_a_transient_error(fake: FakeClient) -> None:
    """An UNAVAILABLE mid-export resumes from the last cursor, without duplicates."""
    fake.export_errors = [FakeRpcError(grpc.StatusCode.UNAVAILABLE)]
    rels = run_tap({})["records"]["relationships"]
    assert len(rels) == 4
    assert [r.optional_cursor.token for r in fake.export_requests] == ["", "2"]


def test_resource_types_filter(fake: FakeClient) -> None:
    """resource_types limits the snapshot and the Watch."""
    result = run_tap({"resource_types": ["group"]})
    assert [r["resource_type"] for r in result["records"]["relationships"]] == ["group"]
    assert result["state"]["bookmarks"]["relationships"]["resource_types"] == ["group"]
    run_tap({"resource_types": ["group"]}, result["state"])
    assert list(fake.watch_requests[0].optional_object_types) == ["group"]


def test_changing_resource_types_takes_a_new_snapshot(fake: FakeClient) -> None:
    """A type added to the filter has history Watch can't bring: snapshot again."""
    state = run_tap({"resource_types": ["group"]})["state"]
    result = run_tap({"resource_types": ["group", "document"]}, state)
    rels = result["records"]["relationships"]
    assert {r["operation"] for r in rels} == {"SNAPSHOT"}
    assert len(rels) == 4
    assert fake.watch_requests == []


def test_watch_disconnect_ends_the_run_without_losing_changes(
    fake: FakeClient,
) -> None:
    """RESOURCE_EXHAUSTED (slow reader) ends the run; the last revision isn't saved."""
    dave = rel("document:3", "viewer", "user:dave")
    fake.watch_items = [
        watch_response("t2", (RelationshipUpdate.OPERATION_CREATE, dave)),
        watch_response("t3", (RelationshipUpdate.OPERATION_DELETE, dave)),
        FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "watch disconnected"),
    ]
    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    result = run_tap({}, state)
    assert len(result["records"]["relationships"]) == 2
    assert bookmark(result["state"]) == "t2"


def test_slow_first_watch_message_is_not_taken_as_caught_up(
    fake: FakeClient,
) -> None:
    """Before the first message the wait is watch_start_timeout_seconds."""
    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    tap = TapSpiceDB(config={**BASE_CONFIG, "watch_start_timeout_seconds": 7})
    stream = tap.streams["relationships"]
    calls = []
    real_next = client_module._next_response

    def spy(responses: Any, idle_timeout: float, deadline: float) -> Any:
        calls.append(idle_timeout)
        return real_next(responses, idle_timeout, deadline)

    fake.watch_items = [
        watch_response(
            "t2",
            (RelationshipUpdate.OPERATION_CREATE, rel("a:1", "r", "user:x")),
        )
    ]
    client_module._next_response = spy
    try:
        list(stream._watch(SNAPSHOT_TOKEN, state["bookmarks"]["relationships"]))
    finally:
        client_module._next_response = real_next
    assert calls == [7, 1]


def test_token_is_marked_secret() -> None:
    """Meltano and --about treat the pre-shared key as a secret."""
    properties = TapSpiceDB.config_jsonschema["properties"]
    assert properties["token"].get("secret") is True
    assert set(TapSpiceDB.config_jsonschema["required"]) == {"host", "token"}


def test_snapshot_mode_ignores_the_token_and_never_watches(fake: FakeClient) -> None:
    """extraction_mode=snapshot exports everything on every run."""
    state = {"bookmarks": {"relationships": {"zed_token": "t9"}}}
    result = run_tap({"extraction_mode": "snapshot"}, state)
    rels = result["records"]["relationships"]
    assert len(rels) == 4
    assert {r["operation"] for r in rels} == {"SNAPSHOT"}
    assert fake.watch_requests == []
    assert bookmark(result["state"]) == SNAPSHOT_TOKEN


def test_each_stream_is_keyed_on_its_own_columns(fake: FakeClient) -> None:
    """No label column: the pipeline (schema, state id) keeps environments apart."""
    records = run_tap({})["records"]
    assert len(records) == 6
    tap = TapSpiceDB(config=BASE_CONFIG)
    assert {name: s.primary_keys for name, s in tap.streams.items()} == {
        "relationships": [
            "resource_type",
            "resource_id",
            "relation",
            "subject_type",
            "subject_id",
            "subject_relation",
        ],
        "schema": ["schema_hash"],
        "schema_definitions": ["definition_name"],
        "schema_relations": ["definition_name", "relation_name"],
        "schema_permissions": ["definition_name", "permission_name"],
        "schema_caveats": ["caveat_name"],
    }
    for rows in records.values():
        assert all("environment" not in r and "source_name" not in r for r in rows)


def test_key_columns_are_never_empty(fake: FakeClient) -> None:
    """The Snowflake target loads "" as NULL into a NOT NULL key column."""
    dave = rel("document:3", "viewer", "user:dave")
    fake.watch_items = [
        watch_response("t2", (RelationshipUpdate.OPERATION_DELETE, dave))
    ]
    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    watched = run_tap({}, state)["records"]["relationships"]
    snapshot = run_tap({})["records"]["relationships"]
    keys = TapSpiceDB(config=BASE_CONFIG).streams["relationships"].primary_keys
    for row in snapshot + watched:
        assert all(row[k] for k in keys), row
    assert watched[0]["subject_relation"] == "..."


@pytest.mark.parametrize(
    "host, expected",
    [
        (HOST_PORT, HOST_PORT),
        (f"http://{HOST_PORT}", HOST_PORT),
        (f"https://{HOST}:443/", f"{HOST}:443"),
        (f"{IP_HOST}:{PORT}", f"{IP_HOST}:{PORT}"),
        (f"  {HOST_PORT} ", HOST_PORT),
        (f"[::1]:{PORT}", f"[::1]:{PORT}"),
    ],
)
def test_host_scheme_and_slash_are_dropped(host: str, expected: str) -> None:
    """gRPC wants host:port: a scheme makes every call fail as UNAVAILABLE."""
    assert client_module.normalize_host(host) == expected


@pytest.mark.parametrize("host", [HOST, f"grpc://x:{PORT}", f"x:{PORT}/path", ""])
def test_a_malformed_host_fails_fast_naming_it(host: str) -> None:
    """No retries and no gRPC traceback: a short error with the host in it."""
    with pytest.raises(client_module.InvalidHostError, match="host:port"):
        client_module.normalize_host(host)


def test_a_malformed_host_is_a_config_error(fake: FakeClient) -> None:
    """Caught with the config, so the CLI prints one error line, not a traceback."""
    with pytest.raises(ConfigValidationError, match="host:port") as error:
        TapSpiceDB(config={**BASE_CONFIG, "host": HOST})
    assert len(error.value.errors) == 1


@pytest.mark.parametrize("setting", ["host", "token"])
def test_a_required_string_cannot_be_empty(setting: str, fake: FakeClient) -> None:
    """An empty host or token is one config error at startup, not a gRPC failure."""
    with pytest.raises(ConfigValidationError, match="non-empty") as error:
        TapSpiceDB(config={**BASE_CONFIG, setting: ""})
    assert setting in str(error.value)


def test_the_version_is_in_the_startup_log() -> None:
    """A log line must show which code ran: v0.0.1 vs the fixed tap."""
    from importlib.metadata import version

    assert TapSpiceDB.plugin_version == version("tap-spicedb")


def test_connection_uses_the_normalized_host(fake: FakeClient) -> None:
    """A host with a scheme still runs, with a warning, on the plain host:port."""
    tap = TapSpiceDB(config={**BASE_CONFIG, "host": f"http://{HOST_PORT}"})
    assert tap.connection.host == HOST_PORT


def test_unavailable_after_the_retries_is_a_short_error(fake: FakeClient) -> None:
    """The error names the host and drops gRPC's long debug string."""
    tap = TapSpiceDB(config={**BASE_CONFIG, "max_retries": 2})
    calls: List[int] = []

    def always_unavailable(timeout: float) -> None:
        calls.append(1)
        raise FakeRpcError(grpc.StatusCode.UNAVAILABLE, "Misformatted domain name")

    with pytest.raises(client_module.SpiceDBUnavailableError) as error:
        tap.connection._with_retries(always_unavailable)
    assert len(calls) == 3  # the call and two retries
    assert HOST_PORT in str(error.value)
    assert "UNAVAILABLE" in str(error.value)
    assert error.value.__cause__ is None


def test_watch_records_carry_transaction_metadata(fake: FakeClient) -> None:
    """Metadata the writer attached to a transaction lands on its rows."""
    dave = rel("document:3", "viewer", "user:dave")
    fake.watch_items = [
        watch_response(
            "t2",
            (RelationshipUpdate.OPERATION_CREATE, dave),
            metadata={"actor": "admin-api", "request_id": "abc"},
        ),
        watch_response("t3", (RelationshipUpdate.OPERATION_DELETE, dave)),
    ]
    state = {"bookmarks": {"relationships": {"zed_token": SNAPSHOT_TOKEN}}}
    rels = run_tap({}, state)["records"]["relationships"]
    assert rels[0]["transaction_metadata"] == {
        "actor": "admin-api",
        "request_id": "abc",
    }
    assert rels[1]["transaction_metadata"] is None
