# Historious product principles

## Users must be able to judge the wait

Every long-running operation must explain what it is doing, how much work it has completed, and what remains. This applies throughout the app, not just to `histo update`. Progress is part of command correctness.

A multi-phase operation shows its full phase plan before slow work starts. A persistent overall meter, current phase number and total, and remaining phase names stay visible while phase-local counters change. The overall meter never resets or moves backward. Conditional phases are accounted for up front and explicitly skipped when unnecessary.

Phase meters measure real work in named units, such as files, events, rows, sessions, or bytes. Their counts match the adjacent text. Overall phase completion is labeled as phases, not as a time percentage. Unequal phases must not imply equal duration. Show elapsed time and use measured throughput or comparable timings for a remaining-time estimate when reliable. Otherwise state that the estimate is unavailable rather than inventing one.

A spinner or seconds ticker proves only that the display is alive. It does not tell users whether work is advancing. Long-running stages must expose measured subwork. Update liveness at least once per second without inventing completed work. If a step cannot expose intermediate counts, name that step, retain its last measured boundary, and explain the work still to come.

Storage operations can keep working without exposing row callbacks. Show measured read/write totals during these steps when the platform provides them. Label these as I/O, not as unique input bytes or a completion percentage. Keep complete counter values visible even when descriptive text must be shortened.

A full meter is a promise about the work it represents. Preparation, index construction, aggregation, replacement, flushing, and committing cannot disappear behind a full processing meter. Show these as planned work with their own counters or explicit state. Distinguish repeated passes. Do not fill a meter and replace it with an unexplained zero or a generic label while more expensive work runs.

Completion means the represented work has committed successfully. A failed or interrupted operation must preserve the last valid data and must not look complete. Normal terminal output, narrow terminals, redirected logs, and machine events communicate the same scope and progress, with separate overall and phase-local coordinates.

## Verify the waiting experience

Exercise the actual command on isolated, representative data. Check the first output, phase boundaries, skipped work, the period after every full meter, and durable completion. Measure silent intervals and measured-work gaps as well as total runtime. A command that eventually succeeds but leaves users watching a ticker without meaningful work information fails acceptance.

Keep fast paths fast. A current read-only command should not rebuild data merely to show progress. Progress reporting must not add repeated scans, long writer locks, or substantial overhead.

Reuse valid data during maintenance. A projection version change must not force unchanged history and search-index entries to be rewritten just to update a creation stamp. Track verification versions separately, stage actual changes, and publish them only after checking that the input is still current.
