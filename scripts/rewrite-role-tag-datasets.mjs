import { access, readFile, writeFile } from "node:fs/promises";
import { constants } from "node:fs";
import { resolve } from "node:path";

const dataDir = resolve(process.cwd(), "data");

const datasetPairs = [
  {
    users: resolve(dataDir, "snapchain-active-context.users.last-24h.json"),
    summary: resolve(dataDir, "snapchain-active-context.last-24h.summary.json"),
  },
  {
    users: resolve(dataDir, "neynar-score-gte-0.99.users.with-context-tags.last-24h.json"),
    summary: resolve(dataDir, "neynar-score-gte-0.99.context-tags.last-24h.summary.json"),
  },
];

async function exists(path) {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

async function writeJson(path, payload) {
  await writeFile(path, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function rewriteRow(row) {
  if (Array.isArray(row.roleTags) && row.roleTagMeta) {
    return row;
  }

  const contextTags = row.contextTags ?? {};
  const nextRow = { ...row };
  delete nextRow.contextTags;

  nextRow.roleTags = Array.isArray(contextTags.assignedRoles) ? contextTags.assignedRoles : [];
  nextRow.roleTagMeta = {
    windowHours: contextTags.windowHours ?? null,
    tagScores: contextTags.roleScores ?? {},
    tagHits: contextTags.roleHits ?? {},
    activity: contextTags.activity ?? {},
    keywords: contextTags.keywords ?? [],
    tagEvidence: contextTags.roleEvidence ?? {},
  };
  return nextRow;
}

function rewriteSummary(summary, rows) {
  const nextSummary = { ...summary };
  const roleCounts = new Map();
  let activeUsers = 0;
  let profilesResolved = 0;
  let taggedUsers = 0;

  for (const row of rows) {
    const roleTags = Array.isArray(row.roleTags) ? row.roleTags : [];
    const roleTagMeta = row.roleTagMeta ?? {};
    const activity = roleTagMeta.activity ?? {};

    if (Number(activity.casts ?? 0) > 0) {
      activeUsers += 1;
    }
    if (roleTags.length > 0) {
      taggedUsers += 1;
    }
    for (const tag of roleTags) {
      roleCounts.set(tag, (roleCounts.get(tag) ?? 0) + 1);
    }
    if (row.snapchainProfile && Object.values(row.snapchainProfile).some(Boolean)) {
      profilesResolved += 1;
    }
  }

  if (nextSummary.enrichment && typeof nextSummary.enrichment === "object") {
    nextSummary.enrichment = {
      activeUsers,
      profilesResolved,
      roleCounts: Object.fromEntries([...roleCounts.entries()].sort(([a], [b]) => a.localeCompare(b))),
      taggedUsers,
      totalUsers: rows.length,
    };
  }
  return nextSummary;
}

for (const pair of datasetPairs) {
  if (!(await exists(pair.users)) || !(await exists(pair.summary))) {
    continue;
  }

  const users = await readJson(pair.users);
  const summary = await readJson(pair.summary);

  const rewrittenUsers = Array.isArray(users) ? users.map(rewriteRow) : users;
  const rewrittenSummary = rewriteSummary(summary, rewrittenUsers);

  await writeJson(pair.users, rewrittenUsers);
  await writeJson(pair.summary, rewrittenSummary);
}

console.log(JSON.stringify({ event: "rewritten_role_tags" }));
