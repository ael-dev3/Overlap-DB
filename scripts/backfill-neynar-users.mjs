import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const apiKey = process.env.NEYNAR_API_KEY?.trim();
const options = readOptions(process.argv.slice(2));
const threshold = options.threshold;
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
const cachePath = resolve(process.cwd(), ".cache", `backfill-gte-${threshold.toFixed(2)}.json`);
const shardIds = [1, 2];
const fidsPageSize = 1000;
const batchSize = 100;
const concurrency = options.concurrency;
const requestLimiter = createRateLimiter(options.minIntervalMs);

if (!apiKey) {
  console.error("NEYNAR_API_KEY is required");
  process.exit(1);
}

const state = await loadState(cachePath, threshold);
const usersByFid = new Map(state.matches.map((user) => [user.fid, user]));

for (const shardId of shardIds) {
  let pageToken = state.shards[String(shardId)]?.nextPageToken ?? "";
  let done = state.shards[String(shardId)]?.done ?? false;

  while (!done) {
    const page = await fetchFidPage({ apiKey, pageSize: fidsPageSize, pageToken, shardId });
    state.stats.fidsScanned += page.fids.length;

    await runWithConcurrency(chunk(page.fids, batchSize), concurrency, async (fidBatch) => {
      const users = await fetchBulkUsers(apiKey, fidBatch);
      state.stats.userBatches += 1;
      state.stats.usersHydrated += users.length;

      for (const user of users) {
        const normalized = normalizeUser(user);

        if (normalized.score < threshold) {
          continue;
        }

        usersByFid.set(normalized.fid, normalized);
      }
    });

    state.matches = [...usersByFid.values()];
    state.stats.matches = usersByFid.size;
    state.shards[String(shardId)] = {
      done: !page.nextPageToken,
      nextPageToken: page.nextPageToken ?? "",
    };
    state.updatedAt = new Date().toISOString();

    await persistState(cachePath, state);

    pageToken = page.nextPageToken ?? "";
    done = !pageToken;

    console.log(
      JSON.stringify({
        event: "progress",
        fidsScanned: state.stats.fidsScanned,
        matches: state.stats.matches,
        nextPageToken: pageToken ? "set" : "",
        shardId,
        userBatches: state.stats.userBatches,
      }),
    );
  }
}

const users = [...usersByFid.values()].sort((left, right) => {
  if (right.score !== left.score) {
    return right.score - left.score;
  }

  return left.fid - right.fid;
});

const generatedAt = new Date().toISOString();
const summary = {
  generatedAt,
  minScore: threshold,
  numUsers: users.length,
  scoreFieldPriority: ["experimental.neynar_user_score", "score"],
  shards: shardIds,
  source: {
    fidsApi: "https://snapchain-api.neynar.com/v1/fids",
    bulkUsersApi: "https://api.neynar.com/v2/farcaster/user/bulk/",
  },
  stats: state.stats,
};

await mkdir(dirname(outputUsersPath), { recursive: true });
await writeFile(outputUsersPath, `${JSON.stringify(users, null, 2)}\n`, "utf8");
await writeFile(outputSummaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
await rm(cachePath, { force: true });

console.log(
  JSON.stringify({
    event: "done",
    generatedAt,
    outputSummaryPath,
    outputUsersPath,
    threshold,
    users: users.length,
  }),
);

async function fetchBulkUsers(key, fids) {
  if (fids.length === 0) {
    return [];
  }

  const url = new URL("https://api.neynar.com/v2/farcaster/user/bulk/");
  url.searchParams.set("fids", fids.join(","));

  const response = await retryingFetch(url, {
    headers: {
      accept: "application/json",
      "x-api-key": key,
      "x-neynar-experimental": "true",
    },
  });

  const payload = await response.json();
  return Array.isArray(payload.users) ? payload.users : [];
}

async function fetchFidPage({ apiKey: key, pageSize, pageToken, shardId }) {
  const url = new URL("https://snapchain-api.neynar.com/v1/fids");
  url.searchParams.set("shard_id", String(shardId));
  url.searchParams.set("pageSize", String(pageSize));
  if (pageToken) {
    url.searchParams.set("pageToken", pageToken);
  }

  const response = await retryingFetch(url, {
    headers: {
      accept: "application/json",
      "x-api-key": key,
    },
  });

  const payload = await response.json();
  return {
    fids: Array.isArray(payload.fids)
      ? payload.fids
          .map((fid) => Number(fid))
          .filter((fid) => Number.isInteger(fid) && fid > 0)
      : [],
    nextPageToken:
      typeof payload.nextPageToken === "string" && payload.nextPageToken.length > 0
        ? payload.nextPageToken
        : "",
  };
}

function normalizeUser(user) {
  const experimentalScore =
    typeof user?.experimental?.neynar_user_score === "number"
      ? user.experimental.neynar_user_score
      : null;
  const score = typeof user?.score === "number" ? user.score : null;
  const resolvedScore = experimentalScore ?? score ?? -1;

  return {
    fid: Number(user.fid),
    username: user.username ?? "",
    displayName: user.display_name ?? user.displayName ?? "",
    bio: user.profile?.bio?.text ?? "",
    pfpUrl: user.pfp_url ?? "",
    score: resolvedScore,
    rawScore: score,
    experimentalScore,
    custodyAddress: user.custody_address ?? "",
    followerCount:
      typeof user.follower_count === "number" ? user.follower_count : 0,
    followingCount:
      typeof user.following_count === "number" ? user.following_count : 0,
    verifiedAddresses: {
      eth:
        user.verified_addresses?.eth_addresses?.filter((value) => typeof value === "string") ??
        [],
      sol:
        user.verified_addresses?.sol_addresses?.filter((value) => typeof value === "string") ??
        [],
      primary: {
        eth:
          typeof user.verified_addresses?.primary?.eth_address === "string"
            ? user.verified_addresses.primary.eth_address
            : "",
        sol:
          typeof user.verified_addresses?.primary?.sol_address === "string"
            ? user.verified_addresses.primary.sol_address
            : "",
      },
    },
    registeredAt: user.registered_at ?? "",
  };
}

async function retryingFetch(url, init, attempts = 6) {
  let lastError;

  for (let index = 0; index < attempts; index += 1) {
    try {
      await requestLimiter.waitTurn();
      const response = await fetch(url, init);

      if (response.ok) {
        return response;
      }

      if (response.status !== 429 && response.status < 500) {
        throw new Error(`HTTP ${response.status}`);
      }

      if (response.status === 429) {
        state.stats.rateLimitRetries += 1;
      } else {
        state.stats.retries += 1;
      }

      requestLimiter.deferFor(readRetryDelayMs(response.headers, index));
      lastError = new Error(`HTTP ${response.status}`);
    } catch (error) {
      state.stats.retries += 1;
      lastError = error;
    }

    const delayMs =
      lastError instanceof Error && lastError.message === "HTTP 429"
        ? Math.max(1_000, readBackoffDelayMs(index))
        : readBackoffDelayMs(index);
    await sleep(delayMs);
  }

  throw lastError;
}

async function runWithConcurrency(items, limit, worker) {
  const queue = [...items];

  await Promise.all(
    Array.from({ length: Math.min(limit, queue.length || 1) }, async () => {
      while (queue.length > 0) {
        const item = queue.shift();
        if (!item) {
          continue;
        }
        await worker(item);
      }
    }),
  );
}

function chunk(items, size) {
  const chunks = [];

  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }

  return chunks;
}

async function loadState(path, minScore) {
  try {
    const raw = await readFile(path, "utf8");
    return normalizeState(JSON.parse(raw), minScore);
  } catch {
    return normalizeState({}, minScore);
  }
}

async function persistState(path, state) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(state, null, 2)}\n`, "utf8");
}

function readOptions(argv) {
  const thresholdFlagIndex = argv.indexOf("--threshold");
  const thresholdValue =
    thresholdFlagIndex >= 0 ? Number(argv[thresholdFlagIndex + 1]) : 0.99;
  const concurrencyFlagIndex = argv.indexOf("--concurrency");
  const concurrencyValue =
    concurrencyFlagIndex >= 0 ? Number(argv[concurrencyFlagIndex + 1]) : 4;
  const minIntervalFlagIndex = argv.indexOf("--min-interval-ms");
  const minIntervalMs =
    minIntervalFlagIndex >= 0 ? Number(argv[minIntervalFlagIndex + 1]) : 125;

  if (!Number.isFinite(thresholdValue) || thresholdValue <= 0 || thresholdValue > 1) {
    throw new Error("Threshold must be a number between 0 and 1");
  }

  if (!Number.isInteger(concurrencyValue) || concurrencyValue <= 0 || concurrencyValue > 20) {
    throw new Error("Concurrency must be an integer between 1 and 20");
  }

  if (!Number.isFinite(minIntervalMs) || minIntervalMs < 0) {
    throw new Error("min-interval-ms must be a non-negative number");
  }

  return {
    concurrency: concurrencyValue,
    minIntervalMs,
    threshold: thresholdValue,
  };
}

function normalizeState(value, minScore) {
  return {
    matches: Array.isArray(value?.matches) ? value.matches : [],
    minScore,
    shards: value?.shards && typeof value.shards === "object" ? value.shards : {},
    stats: {
      fidsScanned: Number(value?.stats?.fidsScanned) || 0,
      matches: Number(value?.stats?.matches) || 0,
      rateLimitRetries: Number(value?.stats?.rateLimitRetries) || 0,
      retries: Number(value?.stats?.retries) || 0,
      userBatches: Number(value?.stats?.userBatches) || 0,
      usersHydrated: Number(value?.stats?.usersHydrated) || 0,
    },
    updatedAt:
      typeof value?.updatedAt === "string" && value.updatedAt.length > 0
        ? value.updatedAt
        : new Date().toISOString(),
  };
}

function createRateLimiter(minIntervalMs) {
  let nextAvailableAt = 0;
  let queue = Promise.resolve();

  return {
    deferFor(delayMs) {
      nextAvailableAt = Math.max(nextAvailableAt, Date.now() + Math.max(0, delayMs));
    },
    async waitTurn() {
      const current = queue;
      let release;
      queue = new Promise((resolve) => {
        release = resolve;
      });

      await current;
      const now = Date.now();
      const waitMs = Math.max(0, nextAvailableAt - now);
      nextAvailableAt = Math.max(nextAvailableAt, now + waitMs) + minIntervalMs;

      if (waitMs > 0) {
        await sleep(waitMs);
      }

      release();
    },
  };
}

function readRetryDelayMs(headers, attempt) {
  const retryAfter = headers.get("retry-after");
  if (retryAfter) {
    const retrySeconds = Number(retryAfter);
    if (Number.isFinite(retrySeconds)) {
      return retrySeconds * 1000;
    }

    const retryDate = Date.parse(retryAfter);
    if (Number.isFinite(retryDate)) {
      return Math.max(0, retryDate - Date.now());
    }
  }

  const rateLimitReset = Number(headers.get("x-ratelimit-reset"));
  if (Number.isFinite(rateLimitReset) && rateLimitReset > 0) {
    return Math.max(0, rateLimitReset * 1000 - Date.now()) + 250;
  }

  return readBackoffDelayMs(attempt);
}

function readBackoffDelayMs(attempt) {
  return Math.min(60_000, 1_000 * 2 ** attempt);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
