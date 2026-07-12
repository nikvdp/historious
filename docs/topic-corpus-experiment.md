# Topic Corpus Experiment

## Decision

Do not ship unsupervised topic clustering in the report. None of the three
tested approaches met the fixed quality bar, and their nearest-centroid samples
were dominated by coding-agent workflow, repeated tasks, and dialogue acts
rather than stable subject areas.

The report should omit topics for now instead of presenting noisy clusters as
insight. Revisit the feature only with a stronger unit of analysis, such as
deduplicated project-level documents or a user-defined taxonomy.

## Pass Bar

The bar was fixed before running the comparison:

- centroid silhouette at least `0.18`; and
- nearest-centroid samples form coherent subject areas, not dialogue acts.

Both conditions had to pass. A numerical improvement alone was not sufficient.

## Method

The experiment read the local derived corpus after provenance projection and
made no network or LLM calls. Sessions were ordered by stable session id and
sampled deterministically down to 1,500 documents per corpus.

- Human dense: substantive human messages aggregated by session.
- Human and assistant dense: substantive human and assistant messages
  aggregated by session.
- Combined TF-IDF: the same human and assistant documents represented by 1,024
  locally derived terms.

Messages shorter than 80 characters were removed, documents shorter than 300
characters were removed, and each document was capped at 8,000 characters. The
dense runs used the existing cached Snowflake Arctic Embed XS quantized model.
All three methods used the same deterministic k-means sweep over
`k = 8, 12, 16, 20, 24, 28, 32` and the same centroid-silhouette calculation.

## Results

| k | Human dense | Human + assistant dense | Combined TF-IDF |
|---:|---:|---:|---:|
| 8 | 0.1036 | 0.1020 | 0.0757 |
| 12 | 0.1020 | 0.1045 | 0.0902 |
| 16 | 0.1256 | 0.0969 | 0.1029 |
| 20 | 0.1162 | 0.1183 | 0.0999 |
| 24 | 0.1357 | 0.1224 | 0.1032 |
| 28 | 0.1303 | **0.1427** | **0.1176** |
| 32 | **0.1438** | 0.1355 | 0.0968 |

No method reached `0.18`. Human-only dense was narrowly best, while adding
assistant prose did not improve the score.

| Method | Best k | Min cluster | Median cluster | Max cluster |
|---|---:|---:|---:|---:|
| Human dense | 32 | 2 | 46.5 | 123 |
| Human + assistant dense | 28 | 12 | 52.5 | 141 |
| Combined TF-IDF | 28 | 7 | 43 | 162 |

The distributions also included tiny clusters at the selected k values, while
the largest clusters absorbed repeated workflows.

## Sample Inspection

Manual inspection confirmed that the largest clusters reflected repeated
workflow shapes rather than coherent subject areas. Adding assistant prose made
that bias stronger, and exact or near-duplicate sessions influenced several
centroids. No raw or representative corpus excerpts are retained in this note.

## Recommendation

Cut the current automatic topic section from the report. Keep the existing
clustering implementation out of the user-facing path and avoid deterministic
labels for clusters that failed the semantic bar.

A future experiment should first deduplicate repeated sessions and introduce a
more meaningful grouping signal, such as repository/project identity or a
user-maintained taxonomy. That should be treated as a new measured experiment,
not as a tuning pass over this failed corpus design.
