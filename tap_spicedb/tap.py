"""spicedb tap class."""

from functools import cached_property
from typing import Any, List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError

from tap_spicedb.client import InvalidHostError, SpiceDBConnection, normalize_host
from tap_spicedb.streams import (
    Relationships,
    Schema,
    SchemaCaveats,
    SchemaDefinitions,
    SchemaPermissions,
    SchemaRelations,
)

PLUGIN_NAME = "tap-spicedb"

STREAM_TYPES = [
    Relationships,
    Schema,
    SchemaDefinitions,
    SchemaRelations,
    SchemaPermissions,
    SchemaCaveats,
]


# A required string can't be empty: an empty host or token would only fail later,
# deep in a gRPC call, instead of as one config error at startup.
NON_EMPTY_STRING = th.CustomType({"type": "string", "minLength": 1})


class TapSpiceDB(Tap):
    """spicedb tap class."""

    name = "tap-spicedb"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            NON_EMPTY_STRING,
            required=True,
            description="gRPC endpoint as host:port, without a scheme, e.g. "
            "<host>:<port>, where host is a hostname or an IP address",
        ),
        th.Property(
            "token",
            NON_EMPTY_STRING,
            required=True,
            secret=True,
            description="SpiceDB pre-shared key",
        ),
        th.Property(
            "insecure",
            th.BooleanType,
            default=False,
            description="Connect without TLS. Only for an API with TLS disabled",
        ),
        th.Property(
            "ca_cert",
            th.StringType,
            description="PEM of the CA that signed the server certificate; needed for "
            "self-signed certificates. Takes precedence over ca_cert_path",
        ),
        th.Property(
            "ca_cert_path",
            th.StringType,
            description="Path to a PEM file of the CA that signed the server "
            "certificate",
        ),
        th.Property(
            "tls_server_name",
            th.StringType,
            description="Name to verify the server certificate against, when it "
            "differs from the host",
        ),
        th.Property(
            "extraction_mode",
            th.StringType,
            default="incremental",
            allowed_values=["incremental", "snapshot"],
            description="incremental: snapshot once, then Watch for changes. "
            "snapshot: export every relationship on every run, without Watch",
        ),
        th.Property(
            "resource_types",
            th.ArrayType(th.StringType),
            description="Extract only these resource types. Default: all",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=1000,
            description="Relationships per page of the snapshot export",
        ),
        th.Property(
            "watch_idle_timeout_seconds",
            th.IntegerType,
            default=30,
            description="End an incremental run after this long without a change",
        ),
        th.Property(
            "watch_start_timeout_seconds",
            th.IntegerType,
            default=180,
            description="End an incremental run if SpiceDB sends nothing for this "
            "long after it starts; the first read of a large backlog is slow. Keep "
            "it under the load balancer's idle timeout (4 min on Azure by default)",
        ),
        th.Property(
            "watch_max_duration_seconds",
            th.IntegerType,
            default=1800,
            description="End an incremental run after this long in any case",
        ),
        th.Property(
            "on_expired_token",
            th.StringType,
            default="snapshot",
            allowed_values=["snapshot", "fail"],
            description="When the saved ZedToken is older than SpiceDB's GC window: "
            "take a new snapshot, or fail the run",
        ),
        th.Property(
            "request_timeout_seconds",
            th.IntegerType,
            default=300,
            description="Timeout of each schema read",
        ),
        th.Property(
            "max_retries",
            th.IntegerType,
            default=5,
            description="Retries on UNAVAILABLE and RESOURCE_EXHAUSTED",
        ),
    ).to_dict()

    def _validate_config(self, *, raise_errors: bool = True) -> List[str]:
        """Validate the schema, then the host: one clean error line, not a traceback."""
        errors = super()._validate_config(raise_errors=raise_errors)
        if self.config.get("host"):  # a missing host is the schema check's error
            try:
                normalize_host(self.config["host"])
            except InvalidHostError as e:
                if raise_errors:
                    raise ConfigValidationError(str(e), errors=[str(e)]) from None
                errors.append(str(e))
        return errors

    @cached_property
    def connection(self) -> SpiceDBConnection:
        """Return the SpiceDB connection shared by all streams."""
        return SpiceDBConnection(self.config, self.logger)

    @cached_property
    def _schema_read(self) -> Any:
        return self.connection.read_schema()

    @property
    def schema_text(self) -> str:
        """Return the schema text, read once per run."""
        return self._schema_read[0]

    @property
    def snapshot_revision(self) -> str:
        """Return the revision all snapshot reads of this run use."""
        return self._schema_read[1]

    @cached_property
    def reflection(self) -> Any:
        """Return the ReflectSchema response at the snapshot revision."""
        return self.connection.reflect_schema(self.snapshot_revision)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


# CLI Execution:
cli = TapSpiceDB.cli
