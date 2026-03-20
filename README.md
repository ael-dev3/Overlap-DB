# Overlap-DB

Reusable data snapshots for Overlap.

## Current dataset

- `data/neynar-score-gte-0.99.users.json`
- `data/neynar-score-gte-0.99.summary.json`
- `data/neynar-score-gte-0.99.progress.json`
- `data/neynar-score-gte-0.99.users.with-context-tags.last-24h.json`
- `data/neynar-score-gte-0.99.context-tags.last-24h.summary.json`
- `data/snapchain-active-context.users.last-24h.json`
- `data/snapchain-active-context.last-24h.summary.json`
- `unranked/fids-scanned.partial.json`

Current committed snapshot:

- `status: partial`
- `users: 2509`
- `fidsScanned: 2748290`
- `userBatches: 28303`
- `shard 1: complete`
- `shard 2: paused with resume token recorded in progress.json`
- `unranked folder: offline scan-state metadata only; individual non-ranked FIDs were not persisted`
- `24h direct Snapchain context: 14460 active authors, 1029 tagged authors`
- `24h high-score cohort with direct Snapchain tags: 50 tagged users out of 2509`

These files are generated from:

1. Neynar Snapchain FIDs API (`/v1/fids`) to enumerate all FIDs
2. Neynar bulk user API (`/v2/farcaster/user/bulk/`) with
   `x-neynar-experimental: true`
3. local filtering where `score >= threshold` or
   `experimental.neynar_user_score >= threshold`
4. direct Snapchain hub reads via the local
   `Clawberto-Farcaster-Context` skill repo for recent cast context and
   hub profile metadata (`/v1/events`, `/v1/userDataByFid`,
   `/v1/userNameProofsByFid`)

## Requirements

- Node.js `22.11.0+`
- `NEYNAR_API_KEY` in the environment
- Python `3.9+` for direct Snapchain context enrichment
- local checkout of `Clawberto-Farcaster-Context` at
  `/Users/marko/Clawberto-Farcaster-Context`

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

Build a direct Snapchain active-author dataset and enrich the current high-score cohort from the same 24-hour window:

```bash
npm run tag:context:24h
```

Build a 7-day context pass only for the existing `>=0.99` cohort:

```bash
npm run tag:cohort:7d
```

Validate all committed datasets, including direct Snapchain context artifacts:

```bash
npm run validate
```

## Context tagging

The direct Snapchain context pass writes two families of artifacts:

- `data/snapchain-active-context.users.last-24h.json`
  Active authors seen in the rolling window, each with:
  - `fid`
  - `snapchainProfile`
  - `contextTags.assignedRoles`
  - `contextTags.assignedTopics`
  - `contextTags.activity`
  - `contextTags.keywords`
  - deterministic evidence snippets
- `data/neynar-score-gte-0.99.users.with-context-tags.last-24h.json`
  The existing high-score Neynar cohort merged with the same direct
  Snapchain `contextTags`.

Current role taxonomy:

- `builder`
- `trader`
- `creator`
- `artist`

Current topic taxonomy:

- `ai`
- `agents`
- `base`
- `bitcoin`
- `content`
- `defi`
- `ethereum`
- `gaming`
- `hyperliquid`
- `mini_apps`
- `nfts`
- `social`
- `solana`
- `trading`

The tagger is deterministic and intentionally conservative:

- it uses direct hub events only, not Neynar content APIs
- it requires repeated lexical evidence before assigning roles/topics
- `builder` is gated by technical/developer-style language to avoid noisy
  `build/built` social chatter
- summaries and datasets are cross-checked by `npm run validate`

## Notes

- The backfill scans both Snapchain data shards (`1` and `2`), which Neynar
  documents as the user-data shards.
- The crawler respects Neynar rate-limit headers and resumes from `.cache/` if a run is interrupted.
- The committed dataset is meant to be a snapshot, not a live database.
- If an operator stops the crawl before both shards finish, the exported summary is marked
  `status: "partial"` and the progress file records the resume token per shard.
- The direct Snapchain context pass reuses the traversal logic from the
  local `Clawberto-Farcaster-Context` skill repo but emits repo-stable JSON
  artifacts for Overlap-DB.
