import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const usersPath = resolve(process.cwd(), "data", "neynar-score-gte-0.99.users.json");
const summaryPath = resolve(process.cwd(), "data", "neynar-score-gte-0.99.summary.json");

const users = JSON.parse(await readFile(usersPath, "utf8"));
const summary = JSON.parse(await readFile(summaryPath, "utf8"));

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

console.log(
  JSON.stringify({
    event: "validated",
    minScore: summary.minScore,
    users: users.length,
  }),
);
