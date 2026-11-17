"""gRPC connection to SpiceDB for tap-spicedb."""

import logging
import queue
import re
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

import grpc
from authzed.api.v1 import (
    Consistency,
    InsecureClient,
    ReadSchemaRequest,
    ReflectSchemaRequest,
    RelationshipFilter,
    SyncClient,
    WatchRequest,
    ZedToken,
)
from authzed.api.v1.core_pb2 import Cursor
from authzed.api.v1.permission_service_pb2 import ExportBulkRelationshipsRequest
from authzed.api.v1.watch_service_pb2 import WatchKind
from grpcutil import bearer_token_credentials

# Errors worth retrying: the server is restarting, overloaded or the connection dropped.
RETRYABLE_CODES = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.RESOURCE_EXHAUSTED)


# Errors of a read at a saved ZedToken that mean Watch can't start from it
UNUSABLE_TOKEN_CODES = (
    grpc.StatusCode.OUT_OF_RANGE,
    grpc.StatusCode.INVALID_ARGUMENT,
)


class ExpiredZedTokenError(Exception):
    """The saved ZedToken is expired (past the GC window) or invalid."""


class InvalidHostError(ValueError):
    """The host setting isn't a gRPC host:port."""


class WatchUnavailableError(Exception):
    """SpiceDB doesn't serve Watch: on PostgreSQL, track_commit_timestamp is off."""


class SpiceDBUnavailableError(Exception):
    """SpiceDB still failed with a transient error after every retry."""


# host:port, where host is a DNS name, an IPv4 address or a bracketed IPv6 address
HOST_PORT = re.compile(r"^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+):[0-9]{1,5}$")


def normalize_host(host: str) -> str:
    """Return the gRPC target host:port, or raise InvalidHostError naming the host.

    gRPC wants no scheme: `http://<host>:<port>` fails with "Misformatted domain
    name" (UNAVAILABLE), which would only show after every retry. A leading http:// or
    https:// and a trailing slash are dropped; anything else that isn't host:port fails.
    """
    cleaned = host.strip()
    cleaned = re.sub(r"^https?://", "", cleaned, flags=re.IGNORECASE).rstrip("/")
    if not HOST_PORT.match(cleaned):
        raise InvalidHostError(
            f"host must be host:port without a scheme or path, got {host!r}"
            " (e.g. <host>:<port>)"
        )
    return cleaned


class SpiceDBConnection:
    """Thin wrapper over the authzed client: snapshot reads and a bounded Watch."""

    def __init__(self, config: dict, logger: logging.Logger) -> None:
        """Build the gRPC client from the tap config."""
        self.config = config
        self.logger = logger
        self.host = normalize_host(config["host"])
        if self.host != config["host"].strip():
            logger.warning(
                "host %r has a scheme or a trailing slash; using %r",
                config["host"],
                self.host,
            )
        self.client = self._build_client()

    def _unavailable(self, e: grpc.RpcError, retries: int) -> SpiceDBUnavailableError:
        """A short error naming the host, instead of gRPC's long one."""
        return SpiceDBUnavailableError(
            f"SpiceDB at {self.host} still returned {e.code().name} after {retries}"
            f" retries: {(e.details() or '')[:200]}"
        )

    def _build_client(self) -> Any:
        host = self.host
        token = self.config["token"]
        options: List[Tuple[str, Any]] = [
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ]
        if self.config.get("tls_server_name"):
            options.append(
                ("grpc.ssl_target_name_override", self.config["tls_server_name"])
            )
        if self.config.get("insecure"):
            self.logger.warning("Connecting to %s without TLS (insecure=true)", host)
            return InsecureClient(host, token, options=options)

        ca_cert = self.config.get("ca_cert")
        if not ca_cert and self.config.get("ca_cert_path"):
            with open(self.config["ca_cert_path"], encoding="utf-8") as f:
                ca_cert = f.read()
        credentials = bearer_token_credentials(
            token, ca_cert.encode("utf-8") if ca_cert else None
        )
        return SyncClient(host, credentials, options=options)

    def _with_retries(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Call a unary RPC, retrying transient errors with exponential backoff."""
        max_retries = self.config.get("max_retries", 5)
        for attempt in range(max_retries + 1):
            try:
                return fn(
                    *args, timeout=self.config["request_timeout_seconds"], **kwargs
                )
            except grpc.RpcError as e:
                if e.code() not in RETRYABLE_CODES:
                    raise
                if attempt == max_retries:
                    raise self._unavailable(e, max_retries) from None
                self.logger.warning(
                    "Retrying after %s (attempt %s)", e.code(), attempt + 1
                )
                time.sleep(2**attempt)

    # Snapshot reads

    def read_schema(self) -> Tuple[str, str]:
        """Return (schema_text, read_at token) at the latest revision."""
        response = self._with_retries(self.client.ReadSchema, ReadSchemaRequest())
        return response.schema_text, response.read_at.token

    def reflect_schema(self, revision: str) -> Any:
        """Return the ReflectSchemaResponse at the given revision."""
        return self._with_retries(
            self.client.ReflectSchema,
            ReflectSchemaRequest(consistency=_at(revision)),
        )

    def export_relationships(
        self, revision: str, resource_type: Optional[str] = None
    ) -> Iterator[Any]:
        """Yield every relationship at the revision, resuming from cursor on errors."""
        cursor: Optional[str] = None
        attempt = 0
        max_retries = self.config.get("max_retries", 5)
        while True:
            request = ExportBulkRelationshipsRequest(
                consistency=_at(revision),
                optional_limit=self.config["page_size"],
            )
            if resource_type:
                request.optional_relationship_filter.CopyFrom(
                    RelationshipFilter(resource_type=resource_type)
                )
            if cursor:
                request.optional_cursor.CopyFrom(Cursor(token=cursor))
            try:
                for response in self.client.ExportBulkRelationships(request):
                    yield from response.relationships
                    cursor = response.after_result_cursor.token or cursor
                    attempt = 0
                return
            except grpc.RpcError as e:
                if e.code() not in RETRYABLE_CODES:
                    raise
                if attempt == max_retries:
                    raise self._unavailable(e, max_retries) from None
                attempt += 1
                self.logger.warning(
                    "Export interrupted by %s; resuming from cursor (attempt %s)",
                    e.code(),
                    attempt,
                )
                time.sleep(2**attempt)

    # Incremental reads

    def check_revision(self, token: str) -> None:
        """Raise ExpiredZedTokenError if the token is older than the GC window.

        Watch doesn't check its start token (SpiceDB v1.48 `services/v1/watch.go`): on
        Postgres it returns whatever transactions GC left, silently skipping the rest.
        A read at the exact token goes through the consistency middleware, which
        does check it and returns OUT_OF_RANGE. A malformed token, or one from another
        SpiceDB, returns INVALID_ARGUMENT: also unusable, so treated the same way.
        """
        try:
            self.reflect_schema(token)
        except grpc.RpcError as e:
            if e.code() in UNUSABLE_TOKEN_CODES:
                raise ExpiredZedTokenError(f"{e.code().name}: {e.details()}") from e
            raise

    def watch(
        self, start_token: str, resource_types: List[str]
    ) -> Iterator[Tuple[Any, bool]]:
        """Yield (WatchResponse, False) from start_token until the stream goes idle.

        "Caught up" means no message for `watch_idle_timeout_seconds`, counted from
        the first message; before it the wait is `watch_start_timeout_seconds`, as
        the first read of a long backlog is one slow query. With checkpoints asked
        for, PostgreSQL sends one at once when caught up, so a quiet run ends after
        the idle timeout; the in-memory datastore sends nothing, so it waits the
        start timeout (tested on SpiceDB v1.48). The run also stops after
        `watch_max_duration_seconds`, or when SpiceDB drops the stream. The last item
        is (None, True) when the watch caught up, or (None, False) when it stopped
        early: the next run continues from the saved token.
        """
        request = WatchRequest(
            optional_start_cursor=ZedToken(token=start_token),
            optional_update_kinds=[
                WatchKind.WATCH_KIND_INCLUDE_RELATIONSHIP_UPDATES,
                WatchKind.WATCH_KIND_INCLUDE_CHECKPOINTS,
            ],
        )
        if resource_types:
            request.optional_object_types.extend(resource_types)

        idle_timeout = self.config["watch_start_timeout_seconds"]
        deadline = time.monotonic() + self.config["watch_max_duration_seconds"]
        call = self.client.Watch(request)
        responses = _read_in_background(call)
        try:
            while True:
                try:
                    item = _next_response(responses, idle_timeout, deadline)
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                        raise WatchUnavailableError(
                            f"SpiceDB at {self.host} doesn't serve the Watch API"
                            f" ({e.details()}). On PostgreSQL enable"
                            " `track_commit_timestamp = on`, or set"
                            " extraction_mode: snapshot"
                        ) from None
                    raise
                if item is _IDLE:
                    self.logger.info("Watch idle for %ss: caught up", idle_timeout)
                    yield None, True
                    return
                if item is _TIMED_OUT or isinstance(item, grpc.RpcError):
                    reason = "the time limit" if item is _TIMED_OUT else item.code()
                    self.logger.warning(
                        "Watch stopped on %s before catching up; the next run"
                        " continues from the saved ZedToken",
                        reason,
                    )
                    yield None, False
                    return
                idle_timeout = self.config["watch_idle_timeout_seconds"]
                yield item, False
        finally:
            call.cancel()


# Markers passed between the Watch reader thread and the tap
_END, _IDLE, _TIMED_OUT = object(), object(), object()


def _read_in_background(call: Any) -> "queue.Queue[Any]":
    """Read a gRPC stream on a thread, so the reader can time out while it's idle."""
    responses: "queue.Queue[Any]" = queue.Queue(maxsize=100)

    def pump() -> None:
        try:
            for response in call:
                responses.put(response)
            responses.put(_END)
        except grpc.RpcError as e:
            responses.put(e)

    threading.Thread(target=pump, daemon=True).start()
    return responses


def _next_response(
    responses: "queue.Queue[Any]", idle_timeout: float, deadline: float
) -> Any:
    """Return the next WatchResponse, _IDLE, _TIMED_OUT or a retryable RpcError.

    SpiceDB drops a Watch whose client reads too slowly (RESOURCE_EXHAUSTED, "watch
    disconnected"), e.g. while the target flushes a batch: that ends the run early
    rather than failing it.
    """
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return _TIMED_OUT
        try:
            item = responses.get(timeout=min(idle_timeout, remaining))
        except queue.Empty:
            if remaining > idle_timeout:
                return _IDLE
            continue  # waited less than the idle timeout: the deadline ends it
        if item is _END:
            raise RuntimeError("SpiceDB closed the Watch stream unexpectedly")
        if isinstance(item, grpc.RpcError) and item.code() not in RETRYABLE_CODES:
            raise item
        return item


def _at(revision: str) -> Consistency:
    return Consistency(at_exact_snapshot=ZedToken(token=revision))


def struct_to_dict(message: Any) -> Optional[Dict[str, Any]]:
    """Convert a protobuf Struct to a dict, or None when empty."""
    from google.protobuf.json_format import MessageToDict

    return MessageToDict(message) or None
