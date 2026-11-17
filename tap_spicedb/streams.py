"""Stream classes for tap-spicedb."""

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from singer_sdk import Stream
from singer_sdk import typing as th
from singer_sdk.helpers._typing import TypeConformanceLevel

from tap_spicedb.client import ExpiredZedTokenError, struct_to_dict

# Relationships state key: the revision the target is complete up to.
BOOKMARK_KEY = "zed_token"
# ...and the resource_types filter that snapshot was taken with.
FILTER_KEY = "resource_types"

# SpiceDB's own relation of a subject that is an object, not a subject set
# (`Ellipsis` in spicedb `pkg/tuple/structs.go`). Never empty: it's part of the primary
# key, and the Snowflake target loads an empty string as NULL into a NOT NULL column.
NO_SUBJECT_RELATION = "..."


class TapSpiceDBStream(Stream):
    """SpiceDB stream class."""

    # Keep nested caveat_context keys as they come; RECURSIVE would drop every key of an
    # object declared without properties (Singer SDK behaviour).
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    @property
    def connection(self) -> Any:
        """Shared SpiceDB connection, one per tap run."""
        return self._tap.connection  # type: ignore[attr-defined]

    @property
    def revision(self) -> str:
        """The revision every snapshot read of this run is taken at."""
        return self._tap.snapshot_revision  # type: ignore[attr-defined]


class Relationships(TapSpiceDBStream):
    """Every stored relationship; the first run is a snapshot, later runs use Watch.

    Deletes come through as records with `_sdc_deleted_at` set. With
    extraction_mode=snapshot every run is a snapshot, and a deleted relationship is
    a row whose `revision` is older than the latest snapshot's.
    """

    name = "relationships"
    primary_keys = [
        "resource_type",
        "resource_id",
        "relation",
        "subject_type",
        "subject_id",
        "subject_relation",
    ]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("resource_type", th.StringType, required=True),
        th.Property("resource_id", th.StringType, required=True),
        th.Property("relation", th.StringType, required=True),
        th.Property("subject_type", th.StringType, required=True),
        th.Property("subject_id", th.StringType, required=True),
        th.Property(
            "subject_relation",
            th.StringType,
            required=True,
            description="'...' (SpiceDB's own marker) when the subject is an object, "
            "not a subject set",
        ),
        th.Property("caveat_name", th.StringType),
        th.Property("caveat_context", th.ObjectType()),
        th.Property("expires_at", th.DateTimeType),
        th.Property(
            "operation",
            th.StringType,
            description="SNAPSHOT, or the Watch operation: CREATE, TOUCH, DELETE",
        ),
        th.Property(
            "revision",
            th.StringType,
            description="ZedToken of the snapshot or of the Watch change",
        ),
        th.Property(
            "transaction_metadata",
            th.ObjectType(),
            description="Metadata the writer attached to the transaction (Watch only)",
        ),
        th.Property("_sdc_deleted_at", th.DateTimeType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Watch from the saved ZedToken, or take a snapshot when there is none."""
        state = self.get_context_state(context)
        token = state.get(BOOKMARK_KEY)
        if self.config["extraction_mode"] == "snapshot":
            pass  # every run is a snapshot; the saved token is ignored
        elif token and state.get(FILTER_KEY, []) != self._resource_types:
            # Watch would only bring future changes of a newly added type.
            self.logger.warning("resource_types changed: taking a new snapshot")
        elif token:
            try:
                self.connection.check_revision(token)
            except ExpiredZedTokenError as e:
                if self.config["on_expired_token"] != "snapshot":
                    raise
                self.logger.warning(
                    "Saved ZedToken expired (%s). Taking a new snapshot: relationships"
                    " deleted since the last run stay in the target until a full"
                    " refresh with a truncated table.",
                    e,
                )
            else:
                yield from self._watch(token, state)
                return
        yield from self._snapshot(state)

    @property
    def _resource_types(self) -> List[str]:
        return sorted(self.config.get("resource_types") or [])

    def _snapshot(self, state: dict) -> Iterable[Dict[str, Any]]:
        revision = self.revision
        self.logger.info("Snapshot of relationships at revision %s", revision)
        for resource_type in self._resource_types or [None]:
            for rel in self.connection.export_relationships(revision, resource_type):
                yield relationship_record(rel, "SNAPSHOT", revision)
        state[BOOKMARK_KEY] = revision
        state[FILTER_KEY] = self._resource_types

    def _watch(self, token: str, state: dict) -> Iterable[Dict[str, Any]]:
        self.logger.info("Watching relationship changes from revision %s", token)
        pending: Optional[str] = None
        for response, caught_up in self.connection.watch(token, self._resource_types):
            if response is None:
                if caught_up and pending:
                    state[BOOKMARK_KEY] = pending
                return
            changes_through = response.changes_through.token
            # A revision may span responses: save a token only once the next one starts.
            if pending and changes_through != pending:
                state[BOOKMARK_KEY] = pending
            pending = changes_through
            if response.schema_updated:
                self.logger.info("Schema changed at revision %s", changes_through)
            deleted_at = datetime.now(timezone.utc).isoformat()
            metadata = None
            if response.HasField("optional_transaction_metadata"):
                metadata = struct_to_dict(response.optional_transaction_metadata)
            for update in response.updates:
                operation = OPERATIONS.get(update.operation, "UNSPECIFIED")
                record = relationship_record(
                    update.relationship, operation, changes_through
                )
                record["transaction_metadata"] = metadata
                if operation == "DELETE":
                    record["_sdc_deleted_at"] = deleted_at
                yield record
            if response.is_checkpoint:
                state[BOOKMARK_KEY] = changes_through


# RelationshipUpdate.Operation values
OPERATIONS = {1: "CREATE", 2: "TOUCH", 3: "DELETE"}


def relationship_record(rel: Any, operation: str, revision: str) -> Dict[str, Any]:
    """Flatten a Relationship message into a record."""
    record: Dict[str, Any] = {
        "resource_type": rel.resource.object_type,
        "resource_id": rel.resource.object_id,
        "relation": rel.relation,
        "subject_type": rel.subject.object.object_type,
        "subject_id": rel.subject.object.object_id,
        "subject_relation": rel.subject.optional_relation or NO_SUBJECT_RELATION,
        "caveat_name": None,
        "caveat_context": None,
        "expires_at": None,
        "operation": operation,
        "revision": revision,
        "transaction_metadata": None,
        "_sdc_deleted_at": None,
    }
    if rel.HasField("optional_caveat"):
        record["caveat_name"] = rel.optional_caveat.caveat_name
        record["caveat_context"] = struct_to_dict(rel.optional_caveat.context)
    if rel.HasField("optional_expires_at"):
        record["expires_at"] = rel.optional_expires_at.ToDatetime(
            tzinfo=timezone.utc
        ).isoformat()
    return record


class Schema(TapSpiceDBStream):
    """The schema text, one row per distinct schema."""

    name = "schema"
    primary_keys = ["schema_hash"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("schema_hash", th.StringType, description="SHA-256 of schema_text"),
        th.Property("schema_text", th.StringType),
        th.Property("revision", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return the schema text read at the start of the run."""
        schema_text = self._tap.schema_text  # type: ignore[attr-defined]
        yield {
            "schema_hash": hashlib.sha256(schema_text.encode("utf-8")).hexdigest(),
            "schema_text": schema_text,
            "revision": self.revision,
        }


class SchemaDefinitions(TapSpiceDBStream):
    """Object definitions (resource and subject types), from ReflectSchema."""

    name = "schema_definitions"
    primary_keys = ["definition_name"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("definition_name", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("relation_count", th.IntegerType),
        th.Property("permission_count", th.IntegerType),
        th.Property("revision", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return one row per definition."""
        for d in self._tap.reflection.definitions:  # type: ignore[attr-defined]
            yield {
                "definition_name": d.name,
                "comment": d.comment or None,
                "relation_count": len(d.relations),
                "permission_count": len(d.permissions),
                "revision": self.revision,
            }


class SchemaRelations(TapSpiceDBStream):
    """Relations of each definition, with the subject types they allow."""

    name = "schema_relations"
    primary_keys = ["definition_name", "relation_name"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("definition_name", th.StringType),
        th.Property("relation_name", th.StringType),
        th.Property("comment", th.StringType),
        th.Property(
            "subject_types",
            th.ArrayType(
                th.ObjectType(
                    th.Property("subject_definition_name", th.StringType),
                    th.Property("subject_relation_name", th.StringType),
                    th.Property("caveat_name", th.StringType),
                    th.Property("is_terminal_subject", th.BooleanType),
                    th.Property("is_public_wildcard", th.BooleanType),
                )
            ),
        ),
        th.Property("revision", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return one row per relation."""
        for d in self._tap.reflection.definitions:  # type: ignore[attr-defined]
            for r in d.relations:
                yield {
                    "definition_name": d.name,
                    "relation_name": r.name,
                    "comment": r.comment or None,
                    "subject_types": [
                        {
                            "subject_definition_name": s.subject_definition_name,
                            "subject_relation_name": s.optional_relation_name or None,
                            "caveat_name": s.optional_caveat_name or None,
                            "is_terminal_subject": s.is_terminal_subject,
                            "is_public_wildcard": s.is_public_wildcard,
                        }
                        for s in r.subject_types
                    ],
                    "revision": self.revision,
                }


class SchemaPermissions(TapSpiceDBStream):
    """Permissions of each definition: names only, the API has no expression."""

    name = "schema_permissions"
    primary_keys = ["definition_name", "permission_name"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("definition_name", th.StringType),
        th.Property("permission_name", th.StringType),
        th.Property("comment", th.StringType),
        th.Property("revision", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return one row per permission."""
        for d in self._tap.reflection.definitions:  # type: ignore[attr-defined]
            for p in d.permissions:
                yield {
                    "definition_name": d.name,
                    "permission_name": p.name,
                    "comment": p.comment or None,
                    "revision": self.revision,
                }


class SchemaCaveats(TapSpiceDBStream):
    """Caveats: their CEL expression and typed parameters."""

    name = "schema_caveats"
    primary_keys = ["caveat_name"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("caveat_name", th.StringType),
        th.Property("expression", th.StringType),
        th.Property("comment", th.StringType),
        th.Property(
            "parameters",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType),
                    th.Property("type", th.StringType),
                )
            ),
        ),
        th.Property("revision", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return one row per caveat."""
        for c in self._tap.reflection.caveats:  # type: ignore[attr-defined]
            yield {
                "caveat_name": c.name,
                "expression": c.expression,
                "comment": c.comment or None,
                "parameters": [{"name": p.name, "type": p.type} for p in c.parameters],
                "revision": self.revision,
            }
