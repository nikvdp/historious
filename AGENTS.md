# Repository Agent Rules

These rules apply throughout the app, including every command, background operation with a user-facing status, and shared progress component. Update, report, indexing, analytics, import, and projection code are not exceptions.

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

## Progress Must Explain the Whole Operation

- Progress is an app-wide product requirement, not a cosmetic feature or an `update`-only rule. See `vision.md`.
- Before slow work starts, show the operation's full phase plan, current phase number and total, and remaining work. Keep this scope visible throughout the operation.
- Every multi-phase operation MUST have a persistent overall meter in addition to any phase-local meters. Overall progress MUST never reset or move backward when phases, sources, batches, or nested tasks change.
- Keep the overall denominator stable. Include conditional work in the plan and explicitly mark skipped work. Never silently append another expensive phase after the meter fills.
- Label overall units honestly. A count of completed phases is not a percentage of elapsed time or total data. Do not sum unlike units or invent equal-duration weights.
- Help users judge the remaining wait. Show elapsed time, measured work and remaining phases. Show a remaining-time estimate only when measured throughput or comparable timings support it; otherwise say the estimate is unavailable. A spinner or seconds ticker is liveness, not work progress.
- Drive phase meters from real active-work units such as files, rows, events, sessions, or bytes. A meter and its adjacent text MUST use the same numerator and denominator.
- Counts MUST be monotonic within a phase. Label phase-local resets explicitly and keep the overall meter visible. Repeated passes MUST have distinct names or pass numbers and appear in the operation's scope.
- Combine work-unit thresholds with elapsed-time thresholds. While slow work continues, update visible liveness at least once per second even when the measured count has not changed; never fabricate completed work.
- A long-running stage with only an animated spinner or elapsed ticker is not acceptable. Expose measured subwork with bounded batches or instrumentation. During an indivisible operation, name the actual operation, its last measured boundary, and what remains; do not leave a stale label from earlier work.
- During indivisible storage operations, show measured I/O when available. Label read/write totals as I/O, not unique input bytes or logical completion. A changing clock alone is insufficient.
- Preparation, index construction, SQL aggregation, replacement, flushing, and commit are work. Include them in the plan and report their own progress; never hide them behind a full meter or a generic `projecting` label.
- Emit phase completion only after the represented work is durably complete. If row processing reaches its total before finalization, label it as row processing and visibly enter the planned finalization phase. Overall completion requires successful finalization; failure or interruption MUST NOT appear complete.
- Reuse shared progress semantics across commands, not just a row renderer. For report maintenance, feed the view the active operation's real `current` and `total` values.
- Interactive progress updates in place. Redirected output emits bounded periodic lines. Both MUST communicate the same overall scope and actual work. Preserve the overall meter and phase coordinates on narrow terminals.
- Never truncate a progress numerator or denominator to preserve descriptive text. Compact the row or omit its local bar on narrow terminals; keep complete counts and the overall scope readable.
- Machine events retain a stable schema from start through completion, with separate overall and phase-local coordinates.

## Preserve Report and Update Fast Paths

- A current `histo report` reads the stored snapshot without rebuilding projections, mutating derived data, or showing maintenance progress.
- Missing or stale report data may enter on-demand maintenance, but the expensive work must be visible immediately and obey the progress rules above.
- `histo report --no-update` performs no refresh or derived-data mutation. `histo report --update` makes a forced rebuild explicit.
- Ordinary and daemon `histo update` runs remain free of expensive analytics classification unless an explicit repair mode requests it.
- Measure time to first output, longest silent interval, total runtime, and warm/current runtime independently.

## Use Scalable Database Operations

- Measure the observed slow path first, then inspect query plans and indexes before redesigning it.
- Avoid N+1 queries, repeated database opens, repeated full-table scans, offset pagination on large tables, and per-row or unnecessarily small transactions.
- Reuse unchanged derived rows and index entries during version upgrades. A per-row projector version is a creation stamp, not a content change. Compare semantic fields without that stamp; track the verified projection version separately.
- Do not repeatedly fetch large payload rows just to compare small metadata fields. Use bounded metadata staging and facts already loaded into the active batch.
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
- Check time to first progress, maximum silent interval, measured-work gaps, stable overall denominator, monotonic overall and phase-local counts, remaining phase counts, meter/text agreement, phase transitions, total runtime, warm-path runtime, writer-lock duration, rollback, and machine-event shape as applicable.
- Capture real interactive and redirected command output when terminal behavior changes.
- Treat any observed stall, timeout, memory failure, misleading meter, or unexplained regression as a failed acceptance check even if the command eventually succeeds.
- Specifically exercise the transition after a phase meter fills. A full meter followed by an unexplained reset, another unnamed pass, or prolonged ticker-only work fails acceptance.
- Never claim a progress or performance fix from eventual completion alone.