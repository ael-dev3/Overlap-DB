import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const threshold = readThreshold(process.argv.slice(2));
const cachePath = resolve(process.cwd(), ".cache", `backfill-gte-${threshold.toFixed(2)}.json`);
const outputUsersPath = resolve(
  process.cwd(),
  "data",
  `neynar-score-gte-${threshold.toFixed(2)}.users.json`,
);
const outputSummaryPath = resolve(
  process.cwd(),
  "data",
  `neynar-score-gte-${threshold.toFixed(2)}.summary.json`,
);
const outputProgressPath = resolve(
  process.cwd(),
  "data",
  `neynar-score-gte-${threshold.toFixed(2)}.progress.json`,
);

const raw = JSON.parse(await readFile(cachePath, "utf8"));
const users = [...(Array.isArray(raw.matches) ? raw.matches : [])].sort((left, right) => {
  if (right.score !== left.score) {
    return right.score - left.score;
  }

  return left.fid - right.fid;
});

const generatedAt = new Date().toISOString();
const cacheFile = `.cache/backfill-gte-${threshold.toFixed(2)}.json`;
const summary = {
  generatedAt,
  minScore: threshold,
  numUsers: users.length,
  status: "partial",
  statusReason: "Operator requested export from local cache before scan completion.",
  scoreFieldPriority: ["experimental.neynar_user_score", "score"],
  source: {
    fidsApi: "https://snapchain-api.neynar.com/v1/fids",
    bulkUsersApi: "https://api.neynar.com/v2/farcaster/user/bulk/",
  },
  stats: {
    fidsScanned: Number(raw?.stats?.fidsScanned) || 0,
    matches: Number(raw?.stats?.matches) || users.length,
    rateLimitRetries: Number(raw?.stats?.rateLimitRetries) || 0,
    retries: Number(raw?.stats?.retries) || 0,
    userBatches: Number(raw?.stats?.userBatches) || 0,
    usersHydrated: Number(raw?.stats?.usersHydrated) || 0,
  },
  shards: normalizeShards(raw?.shards),
};

const progress = {
  exportedAt: generatedAt,
  minScore: threshold,
  cacheFile,
  resumeAvailable: true,
  shards: normalizeShards(raw?.shards),
  stats: summary.stats,
};

await mkdir(dirname(outputUsersPath), { recursive: true });
await writeFile(outputUsersPath, `${JSON.stringify(users, null, 2)}\n`, "utf8");
await writeFile(outputSummaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
await writeFile(outputProgressPath, `${JSON.stringify(progress, null, 2)}\n`, "utf8");

console.log(
  JSON.stringify({
    event: "exported",
    outputProgressPath,
    outputSummaryPath,
    outputUsersPath,
    status: summary.status,
    users: users.length,
  }),
);

function normalizeShards(value) {
  const shards = {};

  for (const [key, shard] of Object.entries(value ?? {})) {
    shards[key] = {
      done: Boolean(shard?.done),
      nextPageToken:
        typeof shard?.nextPageToken === "string" ? shard.nextPageToken : "",
    };
  }

  return shards;
}

function readThreshold(argv) {
  const thresholdFlagIndex = argv.indexOf("--threshold");
  const thresholdValue =
    thresholdFlagIndex >= 0 ? Number(argv[thresholdFlagIndex + 1]) : 0.99;

  if (!Number.isFinite(thresholdValue) || thresholdValue <= 0 || thresholdValue > 1) {
    throw new Error("Threshold must be a number between 0 and 1");
  }

  return thresholdValue;
}
