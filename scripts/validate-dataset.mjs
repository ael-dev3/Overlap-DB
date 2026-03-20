import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";

const cwd = process.cwd();
const dataDir = resolve(cwd, "data");
const usersPath = resolve(dataDir, "neynar-score-gte-0.99.users.json");
const summaryPath = resolve(dataDir, "neynar-score-gte-0.99.summary.json");

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

function countAssigned(values) {
  const counts = new Map();
  for (const value of values) {
    counts.set(value, (counts.get(value) ?? 0) + 1);
  }
  return Object.fromEntries([...counts.entries()].sort(([a], [b]) => a.localeCompare(b)));
}

function normalizeNumeric(value, fallback = 0) {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function validateBaseHighScoreDataset(users, summary) {
  if (!Array.isArray(users)) {
    throw new Error("Users dataset must be an array");
  }

  if (typeof summary?.minScore !== "number") {
    throw new Error("Summary minScore is missing");
  }

  const seenFids = new Set();
  for (const [index, user] of users.entries()) {
    if (!Number.isInteger(user.fid) || user.fid <= 0) {
      throw new Error(`Invalid fid at index ${index}`);
    }
    if (seenFids.has(user.fid)) {
      throw new Error(`Duplicate fid ${user.fid}`);
    }
    seenFids.add(user.fid);
    if (typeof user.score !== "number" || user.score < summary.minScore) {
      throw new Error(`Score below threshold at fid ${user.fid}`);
    }
  }

  for (let index = 1; index < users.length; index += 1) {
    const previous = users[index - 1];
    const current = users[index];
    if (previous.score < current.score) {
      throw new Error("Users are not sorted by descending score");
    }
    if (previous.score === current.score && previous.fid > current.fid) {
      throw new Error("Users with equal scores are not sorted by ascending fid");
    }
  }

  if (summary.numUsers !== users.length) {
    throw new Error("Summary user count does not match dataset length");
  }
}

function validateContextDataset(rows, summary, { expectedLength = null, label }) {
  if (!Array.isArray(rows)) {
    throw new Error(`${label}: context dataset must be an array`);
  }
  if (!isObject(summary?.enrichment)) {
    throw new Error(`${label}: enrichment summary is missing`);
  }
  if (expectedLength !== null && rows.length !== expectedLength) {
    throw new Error(`${label}: expected ${expectedLength} rows, got ${rows.length}`);
  }
  if (summary.enrichment.totalUsers !== rows.length) {
    throw new Error(`${label}: summary totalUsers does not match dataset length`);
  }

  const seenFids = new Set();
  let activeUsers = 0;
  let taggedUsers = 0;
  let profilesResolved = 0;
  const assignedRoles = [];
  const assignedTopics = [];

  for (const [index, row] of rows.entries()) {
    if (!Number.isInteger(row?.fid) || row.fid <= 0) {
      throw new Error(`${label}: invalid fid at index ${index}`);
    }
    if (seenFids.has(row.fid)) {
      throw new Error(`${label}: duplicate fid ${row.fid}`);
    }
    seenFids.add(row.fid);

    if (!isObject(row.contextTags)) {
      throw new Error(`${label}: missing contextTags for fid ${row.fid}`);
    }
    if (row.contextTags.windowHours !== summary.scanWindowHours) {
      throw new Error(`${label}: windowHours mismatch for fid ${row.fid}`);
    }

    const assignedRowRoles = Array.isArray(row.contextTags.assignedRoles) ? row.contextTags.assignedRoles : [];
    const assignedRowTopics = Array.isArray(row.contextTags.assignedTopics) ? row.contextTags.assignedTopics : [];
    assignedRoles.push(...assignedRowRoles);
    assignedTopics.push(...assignedRowTopics);

    const activity = isObject(row.contextTags.activity) ? row.contextTags.activity : null;
    if (!activity) {
      throw new Error(`${label}: missing activity block for fid ${row.fid}`);
    }
    const casts = normalizeNumeric(activity.casts);
    if (casts > 0) {
      activeUsers += 1;
    }
    if (assignedRowRoles.length > 0 || assignedRowTopics.length > 0) {
      taggedUsers += 1;
    }

    if (isObject(row.snapchainProfile) && Object.values(row.snapchainProfile).some(Boolean)) {
      profilesResolved += 1;
    }
  }

  const roleCounts = countAssigned(assignedRoles);
  const topicCounts = countAssigned(assignedTopics);

  if (activeUsers !== summary.enrichment.activeUsers) {
    throw new Error(`${label}: activeUsers mismatch`);
  }
  if (taggedUsers !== summary.enrichment.taggedUsers) {
    throw new Error(`${label}: taggedUsers mismatch`);
  }
  if (profilesResolved !== summary.enrichment.profilesResolved) {
    throw new Error(`${label}: profilesResolved mismatch`);
  }
  if (JSON.stringify(roleCounts) !== JSON.stringify(summary.enrichment.roleCounts ?? {})) {
    throw new Error(`${label}: roleCounts mismatch`);
  }
  if (JSON.stringify(topicCounts) !== JSON.stringify(summary.enrichment.topicCounts ?? {})) {
    throw new Error(`${label}: topicCounts mismatch`);
  }
}

const users = await readJson(usersPath);
const summary = await readJson(summaryPath);
validateBaseHighScoreDataset(users, summary);

const dataFiles = await readdir(dataDir);

for (const file of dataFiles) {
  const activeMatch = file.match(/^snapchain-active-context\.users\.last-(.+)\.json$/);
  if (activeMatch) {
    const label = activeMatch[1];
    const rows = await readJson(resolve(dataDir, file));
    const contextSummary = await readJson(
      resolve(dataDir, `snapchain-active-context.last-${label}.summary.json`),
    );
    validateContextDataset(rows, contextSummary, { label: `active-${label}` });
  }

  const cohortMatch = file.match(/^neynar-score-gte-0\.99\.users\.with-context-tags\.last-(.+)\.json$/);
  if (cohortMatch) {
    const label = cohortMatch[1];
    const rows = await readJson(resolve(dataDir, file));
    const contextSummary = await readJson(
      resolve(dataDir, `neynar-score-gte-0.99.context-tags.last-${label}.summary.json`),
    );
    validateContextDataset(rows, contextSummary, {
      label: `cohort-${label}`,
      expectedLength: users.length,
    });
  }
}

console.log(
  JSON.stringify({
    event: "validated",
    minScore: summary.minScore,
    users: users.length,
  }),
);
