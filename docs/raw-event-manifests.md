# Raw Event Manifests

This note defines the target raw storage contract for JSONL agent logs. It is a
design contract for the event-CAS work; it does not require user-visible behavior
changes by itself.

## Goals

- Store each stable JSONL record once by content hash.
- Represent each observed source-file version as an ordered manifest of record
  objects.
- Reconstruct the exact raw log bytes for export, raw-blob sync, and transcript
  debugging.
- Let appends and forks share unchanged prefix objects across paths and sessions.
- Keep existing `raw_artifacts` whole-file storage valid during migration.

## Current Model

Historious currently stores whole source files in `raw_artifacts`, keyed by a
file-content hash. Events point back to that whole-file hash through
`events.raw_artifact_hash` and `events.metadata_json.raw_artifact_hash`.

Append compaction can repoint older append-prefix artifacts to the newest
whole-file artifact for the same path. That keeps fewer blobs, but growing JSONL
logs still require full-file reads and make forked sessions duplicate old bytes
under different paths.

## Target Tables

The manifest model adds three storage concepts alongside `raw_artifacts`.

### `raw_objects`

Content-addressed raw record bytes.

- `hash TEXT PRIMARY KEY`: BLAKE3 over the exact raw bytes stored for this
  object.
- `media_type TEXT NOT NULL`: usually `application/jsonl-record`.
- `size INTEGER NOT NULL`: byte length of the object.
- `content BLOB NOT NULL`: empty in SQLite when the blob exists under the normal
  `blobs/` CAS path, matching `raw_artifacts` behavior.
- `first_seen_at TEXT NOT NULL`

For JSONL sources, the object bytes are the exact line slice from the source
file, including its trailing newline when present. This preserves reconstruction
without depending on JSON normalization.

### `raw_manifests`

One observed version of a source identity.

- `hash TEXT PRIMARY KEY`: stable hash of manifest metadata plus ordered entry
  object hashes, offsets, lengths, and event hints.
- `source_id TEXT NOT NULL`
- `source_kind TEXT NOT NULL`
- `source_identity TEXT NOT NULL`: the adapter identity, usually absolute path.
- `path TEXT`: source path when one exists.
- `external_session_id TEXT`: session/thread id used for the parsed session.
- `full_size INTEGER NOT NULL`: total source bytes represented.
- `mtime_ms INTEGER`: filesystem mtime when available.
- `media_type TEXT NOT NULL`: source media type, usually `application/jsonl`.
- `entry_count INTEGER NOT NULL`
- `created_at TEXT NOT NULL`
- `metadata_json TEXT NOT NULL`: parser version, reconstruction policy, and
  source-specific details.

The latest current version for a local file is the newest manifest matching
`source_kind + source_identity` whose `full_size` and `mtime_ms` match the
candidate file. Historical lookup is by manifest hash, not by path.

### `raw_manifest_entries`

The ordered record list for a manifest.

- `manifest_hash TEXT NOT NULL`
- `ordinal INTEGER NOT NULL`
- `object_hash TEXT NOT NULL`
- `byte_offset INTEGER NOT NULL`
- `byte_len INTEGER NOT NULL`
- `raw_line_hash TEXT NOT NULL`: same value as `object_hash` for JSONL records.
- `parsed_event_hash TEXT`: existing stable hash over parsed JSON value.
- `event_id TEXT`: Historious event id once parsed.
- `external_event_id TEXT`: source event id when available.
- `metadata_json TEXT NOT NULL`
- primary key: `(manifest_hash, ordinal)`

`byte_offset` and `byte_len` are source-file coordinates for that manifest, not
global object coordinates. A forked session can reuse the same `object_hash`
sequence while having different offsets or source identity metadata.

## Hash Rules

- Raw object hash is over exact bytes only.
- Parsed event hash remains the existing stable JSON hash used by `events.hash`.
- Manifest hash is over a canonical JSON object containing:
  - source kind and identity,
  - external session id,
  - full size and optional mtime,
  - parser/reconstruction policy version,
  - ordered entry tuples `(ordinal, object_hash, byte_offset, byte_len,
    parsed_event_hash, external_event_id)`.
- Blob filenames continue to use the existing BLAKE3 `blobs/` layout.

Raw-byte hashes and parsed-event hashes intentionally stay separate. Raw bytes
give exact reconstruction; parsed hashes give semantic duplicate detection and
stable event ids.

## Invariants

- Concatenating `raw_objects` in manifest-entry order must reproduce the exact
  source bytes represented by the manifest.
- `sum(byte_len)` must equal `raw_manifests.full_size` for JSONL manifests.
- Manifest entries must be contiguous: each entry offset is the previous offset
  plus previous length.
- A parsed `events` row for a JSONL record should point to its manifest context
  through metadata while `events.raw_artifact_hash` remains available for legacy
  callers until all readers are manifest-aware.
- Importing the same object hash with different bytes is an error.
- Whole-file `raw_artifacts` remain valid and must not be deleted merely because
  manifests exist.

## Source Coverage

Manifest storage applies to local append-style JSONL sources:

- Codex JSONL sessions.
- Claude Code JSONL project sessions.
- pi JSONL agent sessions.
- OpenClaw JSONL sessions.
- Hermes JSONL sessions.

Fallback whole-file storage remains explicit for:

- OpenCode SQLite databases.
- Treechat API captures.
- Hermes/OpenClaw `.json` files until a JSON-array record splitter is designed.
- Any opaque, non-JSONL, binary, or single-document source.

Fallback sources continue to emit `RawArtifact` records and use
`raw_artifacts` as they do today.

## Migration Contract

1. Add the new tables without changing existing imports.
2. Teach import to write manifests for JSONL files while still writing enough
   compatibility metadata for existing event, transcript, export, and prune
   paths.
3. Keep `raw_artifacts` rows and blobs for existing stores. Backfill manifests
   from raw whole-file blobs only as an explicit repair/migration step.
4. Make readers prefer manifests when present and fall back to `raw_artifacts`.
5. Once all raw reconstruction and export paths are manifest-aware, whole-file
   blobs for manifest-backed JSONL versions may be compacted by policy, not by
   default migration.

The migration must be additive and safe for local development databases. A
partially migrated store should still list, search, and export sessions using the
legacy raw-artifact path.

## Archive Transport

Archive schema v1 stays readable. Transport can add new record kinds in a
backward-compatible phase:

- `raw_object`: metadata plus optional inline content, like `raw_artifact`.
- `raw_manifest`
- `raw_manifest_entry`

Export options that omit raw artifacts should also omit raw objects and manifests
unless a caller asks for manifest metadata explicitly. Import should accept older
archives with only `raw_artifact` records and newer archives with manifests. When
importing manifest records without object content, missing blob reporting should
include missing raw object hashes the same way it reports missing raw artifact
hashes today.

## Reader Contract

Raw reconstruction for a session or event should resolve in this order:

1. Manifest metadata on the event or session.
2. Latest manifest for `source_kind + source_identity` when reconstructing a
   current local source.
3. Legacy `events.raw_artifact_hash` and `raw_artifacts`.

Transcript and history views should continue to read parsed `events` and
`history_items`; they should not need raw bytes except for raw export/debug
commands. This keeps search behavior stable while storage changes underneath.

## Cleanup Policy

Manifest-backed JSONL cleanup should be conservative:

- Delete an unreferenced raw object only when no manifest entry references it and
  no legacy raw artifact requires the same blob.
- Delete a manifest only when its source/session scope is pruned.
- Never remove the newest manifest for a source identity during append
  compaction.
- Keep whole-file raw artifacts until a dedicated compatibility cleanup proves
  every needed reconstruction path is manifest-backed.
