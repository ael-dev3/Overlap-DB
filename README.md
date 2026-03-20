# Overlap-DB

Reusable data snapshots for Overlap.

## Current dataset

- `data/neynar-score-gte-0.99.users.json`
- `data/neynar-score-gte-0.99.summary.json`
- `data/neynar-score-gte-0.99.progress.json`
- `unranked/fids-scanned.partial.json`

Current committed snapshot:

- `status: partial`
- `users: 2509`
- `fidsScanned: 2748290`
- `userBatches: 28303`
- `shard 1: complete`
- `shard 2: paused with resume token recorded in progress.json`
- `unranked folder: offline scan-state metadata only; individual non-ranked FIDs were not persisted`

These files are generated from:

1. Neynar Snapchain FIDs API (`/v1/fids`) to enumerate all FIDs
2. Neynar bulk user API (`/v2/farcaster/user/bulk/`) with
   `x-neynar-experimental: true`
3. local filtering where `score >= threshold` or
   `experimental.neynar_user_score >= threshold`

## Requirements

- Node.js `22.11.0+`
- `NEYNAR_API_KEY` in the environment

## Usage

Backfill the `>= 0.99` cohort:

```bash
NEYNAR_API_KEY=... npm run backfill:0.99
```

Override crawler pacing if needed:

```bash
NEYNAR_API_KEY=... node scripts/backfill-neynar-users.mjs --threshold 0.99 --concurrency 4 --min-interval-ms 125
```

Validate the generated files:

```bash
npm run validate
```

Export the current local cache into committed snapshot files without making additional API calls:

```bash
npm run export:cache
```

## Notes

- The backfill scans both Snapchain data shards (`1` and `2`), which Neynar
  documents as the user-data shards.
- The crawler respects Neynar rate-limit headers and resumes from `.cache/` if a run is interrupted.
- The committed dataset is meant to be a snapshot, not a live database.
- If an operator stops the crawl before both shards finish, the exported summary is marked
  `status: "partial"` and the progress file records the resume token per shard.
