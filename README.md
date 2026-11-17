# `tap-spicedb`

Singer tap for [SpiceDB](https://authzed.com/docs), built with the [Meltano Singer SDK](https://sdk.meltano.com/en/v0.48.1/dev_guide.html). It extracts SpiceDB's raw relationships and its authorization schema over the gRPC API, for loading into Snowflake through Meltano.

The tap extracts **stored facts only**. It doesn't compute effective permissions (no `CheckPermission`/`LookupResources`) or flatten the relationship graph: that belongs downstream in dbt.

## Streams

| Stream | Source RPC | Primary key | Replication |
|---|---|---|---|
| `relationships` | `ExportBulkRelationships` (first run), then `Watch` | `resource_type`, `resource_id`, `relation`, `subject_type`, `subject_id`, `subject_relation` | Snapshot, then incremental by ZedToken; deletes as `_sdc_deleted_at`. Or a snapshot every run (`extraction_mode: snapshot`) |
| `schema` | `ReadSchema` | `schema_hash` | Full table: one row per distinct schema text |
| `schema_definitions` | `ReflectSchema` | `definition_name` | Full table |
| `schema_relations` | `ReflectSchema` | `definition_name`, `relation_name` | Full table; `subject_types` is an array |
| `schema_permissions` | `ReflectSchema` | `definition_name`, `permission_name` | Full table. Names only: the API doesn't return permission expressions, which are in `schema.schema_text` |
| `schema_caveats` | `ReflectSchema` | `caveat_name` | Full table; `parameters` is an array |

One set of tables holds **one SpiceDB environment**. There is no environment column: environments are kept apart by the pipeline. Each Meltano environment (`beta`, `prod`…) loads into its own Snowflake schema and uses its own state ID, as the other sources do (see [Add to Meltano](#add-to-meltano)). **Never point two SpiceDB instances at the same schema or state ID:** they would overwrite each other's rows without an error.

`relationships` columns: the seven key columns, `caveat_name`, `caveat_context` (object, a Snowflake `VARIANT`), `expires_at`, `operation` (`SNAPSHOT`, `CREATE`, `TOUCH`, `DELETE`), `revision` (the ZedToken), `transaction_metadata` (`VARIANT`: what the writer attached to the transaction, e.g. the actor; `Watch` rows only) and `_sdc_deleted_at`. `subject_relation` is `...` (SpiceDB's own marker, `Ellipsis` in `pkg/tuple/structs.go`) when the subject is an object rather than a subject set. It is never empty: the column is part of the primary key, and the Snowflake target loads an empty string as NULL into a NOT NULL column, which fails the load. The key never holds a NULL, so `MERGE` matches; downstream, `...` means a direct subject.

## How extraction works

Every run reads the schema first: `ReadSchema` returns the current revision (a ZedToken), and every snapshot read in the run happens at that exact revision, so all streams are consistent with each other.

| Run | `relationships` does | Saved in state (`bookmarks.relationships.zed_token`) |
|---|---|---|
| First run, or `--full-refresh` | Exports every relationship at the revision, page by page; an `UNAVAILABLE` mid-export resumes from the last cursor | The snapshot revision |
| Later runs | Checks the saved ZedToken is still valid, then `Watch`es from it; emits `CREATE`/`TOUCH` as upserts and `DELETE` with `_sdc_deleted_at` set. Ends when no change arrives for `watch_idle_timeout_seconds` (`watch_start_timeout_seconds` before the first one), after `watch_max_duration_seconds`, or when SpiceDB drops the stream | The last revision whose changes were all emitted |
| Saved token older than SpiceDB's GC window | `on_expired_token: snapshot` (default) takes a new snapshot; `fail` stops the run | The new snapshot revision |
| `extraction_mode: snapshot` | Every run exports every relationship at that run's revision; no `Watch`, so no `track_commit_timestamp` needed. A deleted relationship is a row whose `revision` isn't the latest snapshot's: filter on it downstream | The snapshot revision, unused in this mode |
| `resource_types` changed since the last snapshot | Takes a new snapshot: `Watch` would only bring the new types' future changes | The new snapshot revision and filter |

**Why the token check:** in SpiceDB v1.48 `Watch` doesn't validate its start token (`internal/services/v1/watch.go`). From an expired token on Postgres, it silently returns only the transactions GC left. The tap first reads the schema at that exact token (`ReflectSchema`, `at_exact_snapshot`); that read goes through SpiceDB's consistency check and fails with `OUT_OF_RANGE` when the token is expired.

**No change is lost on failure.** The SDK writes state every 10,000 records and at the end of the stream, and the token only moves past a revision once all its changes are emitted. A run that fails, hits the time limit or is disconnected replays from the last saved token; the replayed rows merge on the primary key.

## Configuration

| Setting | Required | Default | Description |
|---|---|---|---|
| `host` | yes | | gRPC `host:port` **without a scheme**, e.g. `<host>:<port>`, where host is a hostname or an IP address. `http://` and `https://` prefixes are dropped with a warning (gRPC fails on them with `Misformatted domain name`); anything else that isn't `host:port` stops the run with an error naming the host |
| `token` | yes | | The SpiceDB pre-shared key, supplied from your secret store. Secret |
| `extraction_mode` | | `incremental` | `incremental`: one snapshot, then `Watch`. `snapshot`: a full export every run, the fallback when `Watch` isn't available |
| `insecure` | | `false` | Plaintext gRPC, for an API with TLS disabled (local SpiceDB) |
| `ca_cert` | | | PEM of the CA that signed the server certificate. Our clusters use cert-manager self-signed certificates, so this is likely needed |
| `ca_cert_path` | | | The same, as a file path. `ca_cert` wins if both are set |
| `tls_server_name` | | | Name to check the certificate against, when it differs from `host` |
| `resource_types` | | all | Extract only these resource types (snapshot filter and `Watch` object types) |
| `page_size` | | `1000` | Relationships per export page. At most the server's `--max-bulk-export-relationships-limit` (10,000 by default) |
| `watch_idle_timeout_seconds` | | `30` | End an incremental run after this long without a change |
| `watch_start_timeout_seconds` | | `180` | Wait this long for the first change: the first read of a long backlog is one slow query. Keep it under the load balancer's idle timeout (4 min by default on Azure) |
| `watch_max_duration_seconds` | | `1800` | End an incremental run after this long in any case |
| `on_expired_token` | | `snapshot` | `snapshot` or `fail`, see above |
| `request_timeout_seconds` | | `300` | Timeout of each schema read |
| `max_retries` | | `5` | Retries, with exponential backoff, on `UNAVAILABLE` and `RESOURCE_EXHAUSTED` |

Run `tap-spicedb --about` for the full list, including the SDK's built-in settings (stream maps, flattening).

### Credentials

- Keep `token` (and `ca_cert`) in environment variables (`TAP_SPICEDB_TOKEN`), set from your secret store. Never commit a `config.json`: `.gitignore` excludes it and `*.pem`.
- SpiceDB's pre-shared key is not scoped: it can write as well as read. The tap only calls read RPCs (`ReadSchema`, `ReflectSchema`, `ExportBulkRelationships`, `Watch`).

## Prerequisites for a SpiceDB environment

| Need | Why | Check |
|---|---|---|
| Network route from the Meltano runner to the SpiceDB gRPC port | SpiceDB is usually on a private network | Your network or platform team |
| `track_commit_timestamp = on` on the PostgreSQL datastore | `Watch` needs it. Without it SpiceDB v1.48 doesn't serve `Watch` at all (`UNIMPLEMENTED: unknown service authzed.api.v1.WatchService`), and the run stops with one error that says what to enable. Not needed with `extraction_mode: snapshot` | DevOps |
| Run more often than SpiceDB's GC window (`--datastore-gc-window`, 24h by default) | A token older than the window can't be watched from | DevOps, for the window |
| SpiceDB ≥ v1.35 | `ExportBulkRelationships` in `PermissionsService`, `ReflectSchema`, `Watch` checkpoints. We run v1.48.0 | |

## Limitations

- **A new snapshot doesn't delete.** After `--full-refresh` or an expired token, relationships deleted since the last run stay in Snowflake as live rows. Truncate the table before a full refresh.
- **`_sdc_deleted_at` is the extraction time**, not the time of the delete: `Watch` doesn't return commit timestamps.
- **Expiring relationships** (`expires_at`) don't emit a `DELETE` when they expire; filter on `expires_at` downstream.
- **Schema streams are full table and never delete:** a definition removed from the schema keeps its last row. The current schema is the rows of the latest `_SDC_EXTRACTED_AT`.
- **How long a quiet incremental run takes depends on the datastore** (tested on SpiceDB v1.48). On PostgreSQL a caught-up `Watch` sends a checkpoint at once, so the run ends after about `watch_idle_timeout_seconds` (30 s by default) and the saved token moves forward. On the in-memory `serve-testing` no checkpoint arrives, so a quiet run waits `watch_start_timeout_seconds` (3 min by default): lower it for local tests.
- **No gRPC keepalive** (from SpiceDB's source, not run). SpiceDB v1.48 sets no keepalive enforcement policy (`pkg/cmd/util/util.go`), so gRPC's default applies and it rejects client pings more often than every 5 minutes, longer than the Azure load balancer's 4-minute idle timeout. A connection the load balancer drops while idle ends the run like a disconnect: the next run continues, and nothing is lost.
- **SpiceDB drops a Watch whose reader is slow** (from SpiceDB's source, not run; `RESOURCE_EXHAUSTED`, "watch disconnected": its buffer is 1,024 revisions with a 1s write timeout), e.g. while the target flushes a batch. The run ends early and the next one continues; a backlog that never finishes needs more frequent runs.
- "Caught up" is inferred from `watch_idle_timeout_seconds` without a change. A source that changes constantly ends on `watch_max_duration_seconds` instead, and the next run continues. A long backlog can make the first message slow (one slow read), so the first wait is `watch_start_timeout_seconds`.

## Testing locally

```bash
pipx install poetry
poetry install
poetry run pytest            # offline tests against a fake gRPC client
poetry run tap-spicedb --about
```

The tests drive the whole tap (`sync_all`) through snapshot, incremental changes including deletes, expired tokens, interrupted exports and the time limit, with real `authzed` protobuf messages.

### Against a local SpiceDB

```bash
export SPICEDB_LOCAL_PORT=<free local port>
docker run --rm -p $SPICEDB_LOCAL_PORT:50051 authzed/spicedb:v1.48.0 serve-testing
export TAP_SPICEDB_LOCAL_HOST=<local host>:$SPICEDB_LOCAL_PORT   # host:port of the container
```

`serve-testing` runs an in-memory SpiceDB without TLS that accepts any key. Write a schema and some relationships with [`authzed-py`](https://github.com/authzed/authzed-py) (already installed with the tap) or [`zed`](https://github.com/authzed/zed), then run the tap into [`target-jsonl`](https://github.com/andyh1203/target-jsonl). Install `target-jsonl` in its own virtual environment: its dependencies conflict with the tap's.

```bash
python3 -m venv /tmp/tj-venv && /tmp/tj-venv/bin/pip install -U pip target-jsonl
mkdir -p output/first output/second
cat > config.json <<EOF
{"host": "$TAP_SPICEDB_LOCAL_HOST", "token": "any", "insecure": true,
 "watch_idle_timeout_seconds": 5, "watch_start_timeout_seconds": 10}
EOF
echo '{"destination_path": "output/first"}' > target-first.json
echo '{"destination_path": "output/second"}' > target-second.json

# first run: a snapshot. The last line target-jsonl prints is the STATE
poetry run tap-spicedb --config config.json | /tmp/tj-venv/bin/target-jsonl -c target-first.json | tail -1 > state.json
# create, touch and delete a few relationships, then the incremental run:
poetry run tap-spicedb --config config.json --state state.json | /tmp/tj-venv/bin/target-jsonl -c target-second.json
```

Expect every relationship as `SNAPSHOT` in `output/first`, and only the changes in `output/second`: `TOUCH` for creates and updates, and `DELETE` with `_sdc_deleted_at` set. The short Watch timeouts only keep a local run fast; with the defaults a run with no changes waits 3 minutes. A second run from the same state with no changes should emit no relationships.

`config.json` holds the key: delete it when you're done. `output/` is gitignored; so is `config.json`.

## Add to Meltano

In your Meltano project's `meltano.yml`; pin `pip_url` to a tag so a later merge doesn't reach prod untested. **Declare every setting you use,** with its `kind`: an undeclared one shows as "Custom, possibly unsupported by the plugin" and isn't validated. **Set `host` per Meltano environment,** one `environments:` entry per SpiceDB instance (`beta`, `beta-eu`, `prod`…). The DAG gives each environment its own **target schema and state ID**, which is what keeps environments apart:

```yml
plugins:
  extractors:
    - name: tap-spicedb
      namespace: tap_spicedb
      pip_url: git+https://github.com/degreed-data-engineering/tap-spicedb@v0.0.6
      capabilities:
        - state
        - catalog
        - discover
        - about
      settings:
        - name: host
          kind: string
        - name: token
          kind: password
          sensitive: true
        - name: insecure
          kind: boolean
        - name: ca_cert
          kind: string
        - name: watch_idle_timeout_seconds
          kind: integer
        - name: watch_start_timeout_seconds
          kind: integer
      config:
        ca_cert: $TAP_SPICEDB_CA_CERT   # the token is read from TAP_SPICEDB_TOKEN
environments:
  - name: beta
    config:
      plugins:
        extractors:
          - name: tap-spicedb
            config:
              host: $TAP_SPICEDB_BETA_HOST   # host:port, no http://
  - name: prod
    config:
      plugins:
        extractors:
          - name: tap-spicedb
            config:
              host: $TAP_SPICEDB_PROD_HOST   # host:port
```

Set `TAP_SPICEDB_BETA_HOST` and `TAP_SPICEDB_PROD_HOST` (each `host:port`) in the runner's environment; the names are examples, one per SpiceDB instance.

Run it with the environment, its own schema and a state ID per environment, so each SpiceDB keeps its own ZedToken between runs, as the other DAGs do:

```bash
meltano --environment=beta el tap-spicedb target-snowflake --state-id=spicedb-beta
```

To test in a Meltano project:

1. Open a shell in your Meltano project
2. `meltano install extractor tap-spicedb`
3. Set the token, and pick the environment (the tap's own `meltano.yml` has a `local` one for a local SpiceDB, which reads `TAP_SPICEDB_LOCAL_HOST`): `export TAP_SPICEDB_TOKEN=...`
4. `meltano --environment=<name> invoke tap-spicedb --discover > catalog.json`
5. `meltano --environment=<name> invoke tap-spicedb > output.txt`

`host` and `token` are required: without them the tap stops with, for example, `'host' is a required property`. If `meltano.yml` reads `$TAP_SPICEDB_HOST` instead of a Meltano environment, export it in the same shell (or pass it with `-e` to Docker).
