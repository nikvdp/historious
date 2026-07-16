# Repository Agent Rules

These rules apply to changes involving update, report, indexing, analytics, and projection code.

## Keep Personal Data Out of Repository Artifacts

- NEVER put personal, private, identifying, or secret data in `AGENTS.md` or other persistent repository artifacts, including documentation, tests, fixtures, logs, and commit messages.
- Treat local databases and command output as private by default. Use generic placeholders in examples and never copy real records into the repository.
- Document reusable technical invariants, not details about a contributor, account, workstation, or dataset.

## Use Behavioral Acceptance Criteria

- Translate requirements into observable command behavior before implementing them. Do not replace a requirement with an easier proxy.
- When behavior must match an existing command, reuse its underlying semantics and shared implementation where practical; visual similarity alone is insufficient.
- Give each nontrivial requirement a check that would fail if that behavior regressed.

## Keep the Change Small

- Use the smallest complete fix and preserve unrelated behavior.
- Prefer an existing code path over a parallel implementation or new abstraction.
- Establish correct behavior and its checks before undertaking a broader performance rewrite.

## Progress Must Stay Alive

- Emit an initial progress state immediately before slow work begins.
- Drive meters from real active-work units such as files, rows, events, sessions, or bytes.
- A meter and its adjacent text MUST use the same numerator and denominator. If work is hierarchical, label overall and active-phase progress separately.
- Counts MUST be monotonic within a phase. Make phase transitions explicit before resetting a count.
- Combine work-unit thresholds with elapsed-time thresholds. While slow work continues, update visible liveness at least once per second even when the measured count has not changed; never fabricate completed work.
- Do not leave long SQL queries, scans, transactions, or batches unobservable. Chunk them, instrument them, or provide truthful liveness until measured progress resumes.
- For report maintenance, reuse the native `histo update` progress semantics and terminal style, not merely its row renderer. Feed the view the active operation's real `current` and `total` values.
- Interactive progress updates in place. Redirected output emits bounded periodic lines. Machine events retain a stable schema from start through completion.
- Emit completion only after the represented work is durably complete.

## Preserve Report and Update Fast Paths

- A current `histo report` reads the stored snapshot without rebuilding projections, mutating derived data, or showing maintenance progress.
- Missing or stale report data may enter on-demand maintenance, but the expensive work must be visible immediately and obey the progress rules above.
- `histo report --no-update` performs no refresh or derived-data mutation. `histo report --update` makes a forced rebuild explicit.
- Ordinary and daemon `histo update` runs remain free of expensive analytics classification unless an explicit repair mode requests it.
- Measure time to first output, longest silent interval, total runtime, and warm/current runtime independently.

## Use Scalable Database Operations

- Measure the observed slow path first, then inspect query plans and indexes before redesigning it.
- Avoid N+1 queries, repeated database opens, repeated full-table scans, offset pagination on large tables, and per-row or unnecessarily small transactions.
- Stream or batch through indexed ranges with bounded memory and temporary-disk use.
- Keep primary-database writer transactions short. Long reads, classification, staging, and progress calculation must not hold the main writer lock.
- Build large replacement projections in bounded staging storage, then swap them in one short atomic transaction.
- Preserve the last valid projection until replacement commits; failure or interruption must not expose partial derived state.
- Progress reporting must not add repeated count scans, lock contention, or material overhead.

## Handle Data Safely

- Use isolated temporary databases and synthetic fixtures by default.
- Never mutate, repair, rebuild, delete, vacuum, or benchmark against a private/live database without explicit authorization for that operation. Keep live checks read-only otherwise.
- Test interruption and failure on isolated data. Confirm the prior valid state survives and the next run can recover.

## Verify the User-Facing Behavior

- Exercise the exact command and output mode that changed, not only an internal helper or renderer.
- Cover the relevant states: new or empty database, current snapshot, missing snapshot, stale snapshot, forced refresh, skipped refresh, and failed/interrupted refresh.
- Use production-shaped scale when query performance or progress cadence is part of the requirement; tiny fixtures cannot validate either.
- Check time to first progress, maximum silent interval, monotonicity, meter/text agreement, phase transitions, total runtime, warm-path runtime, writer-lock duration, rollback, and machine-event shape as applicable.
- Capture real interactive and redirected command output when terminal behavior changes.
- Treat any observed stall, timeout, memory failure, misleading meter, or unexplained regression as a failed acceptance check even if the command eventually succeeds.
- Never claim a progress or performance fix from eventual completion alone.