import { access, readFile } from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { resolve } from "node:path";

const cwd = process.cwd();
const dataDir = resolve(cwd, "data");

const DATASET_CANDIDATES = {
  base: {
    usersPath: resolve(dataDir, "neynar-score-gte-0.99.users.json"),
    summaryPath: resolve(dataDir, "neynar-score-gte-0.99.summary.json"),
    hasRoles: false,
  },
  "cohort-24h": {
    usersPath: resolve(dataDir, "neynar-score-gte-0.99.users.with-context-tags.last-24h.json"),
    summaryPath: resolve(dataDir, "neynar-score-gte-0.99.context-tags.last-24h.summary.json"),
    hasRoles: true,
  },
  "active-24h": {
    usersPath: resolve(dataDir, "snapchain-active-context.users.last-24h.json"),
    summaryPath: resolve(dataDir, "snapchain-active-context.last-24h.summary.json"),
    hasRoles: true,
  },
};

function usage() {
  return [
    "Usage: node scripts/query-users.mjs [filters]",
    "",
    "Filters:",
    "  --dataset <base|cohort-24h|active-24h>   Dataset to query (default: cohort-24h if present, else base)",
    "  --fid <number>                           Exact Farcaster fid",
    "  --username <handle>                      Exact username (case-insensitive)",
    "  --address <value>                        Match custody / verified ETH / verified SOL address",
    "  --role <builder|creator|trader|artist>   Match role tag (context datasets only)",
    "  --query <text>                           Case-insensitive substring search over handle/name/bio/address/fid",
    "  --limit <number>                         Max results to print (default: 10)",
    "  --json                                   Emit JSON instead of text",
    "  --help                                   Show this help",
    "",
    "Examples:",
    "  node scripts/query-users.mjs --username farcaster",
    "  node scripts/query-users.mjs --address 0xdb83ae472f108049828db5f429595c4b5932b62c",
    "  node scripts/query-users.mjs --dataset cohort-24h --role builder --limit 5",
    "  node scripts/query-users.mjs --query base --limit 20",
  ].join("\n");
}

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const args = {
    dataset: null,
    fid: null,
    username: null,
    address: null,
    role: null,
    query: null,
    limit: 10,
    json: false,
    help: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (token === "--json") {
      args.json = true;
      continue;
    }
    if (token === "--help" || token === "-h") {
      args.help = true;
      continue;
    }
    if (!token.startsWith("--")) {
      fail(`Unexpected positional argument: ${token}`);
    }
    const key = token.slice(2);
    const value = argv[i + 1];
    if (value == null || value.startsWith("--")) {
      fail(`Missing value for ${token}`);
    }
    i += 1;
    if (!(key in args)) {
      fail(`Unknown flag: ${token}`);
    }
    args[key] = value;
  }

  if (args.fid != null) {
    const parsed = Number(args.fid);
    if (!Number.isInteger(parsed) || parsed <= 0) {
      fail("--fid must be a positive integer");
    }
    args.fid = parsed;
  }

  if (args.limit != null) {
    const parsed = Number(args.limit);
    if (!Number.isInteger(parsed) || parsed <= 0) {
      fail("--limit must be a positive integer");
    }
    args.limit = parsed;
  }

  if (args.role != null) {
    args.role = String(args.role).trim().toLowerCase();
    if (!new Set(["builder", "creator", "trader", "artist"]).has(args.role)) {
      fail("--role must be one of: builder, creator, trader, artist");
    }
  }

  return args;
}

async function fileExists(path) {
  try {
    await access(path, fsConstants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

async function resolveDataset(datasetArg) {
  if (datasetArg) {
    const dataset = DATASET_CANDIDATES[datasetArg];
    if (!dataset) {
      fail(`Unknown dataset: ${datasetArg}`);
    }
    if (!(await fileExists(dataset.usersPath))) {
      fail(`Dataset file not found for ${datasetArg}: ${dataset.usersPath}`);
    }
    return { name: datasetArg, ...dataset };
  }

  for (const name of ["cohort-24h", "base"]) {
    const dataset = DATASET_CANDIDATES[name];
    if (await fileExists(dataset.usersPath)) {
      return { name, ...dataset };
    }
  }

  fail("No known dataset files found under ./data");
}

function normalizeString(value) {
  return String(value ?? "").trim().toLowerCase();
}

function collectAddresses(user) {
  const out = new Set();
  const push = (value) => {
    if (typeof value === "string" && value.trim()) {
      out.add(value.trim());
    }
  };

  push(user.custodyAddress);

  const verified = user.verifiedAddresses;
  if (verified && typeof verified === "object") {
    for (const key of ["eth", "sol"]) {
      const values = verified[key];
      if (Array.isArray(values)) {
        for (const value of values) push(value);
      }
    }
    if (verified.primary && typeof verified.primary === "object") {
      push(verified.primary.eth);
      push(verified.primary.sol);
    }
  }

  return [...out];
}

function matchesQuery(user, args, dataset) {
  if (args.fid != null && user.fid !== args.fid) return false;
  if (args.username != null && normalizeString(user.username) !== normalizeString(args.username)) return false;
  if (args.address != null) {
    const want = normalizeString(args.address);
    const addresses = collectAddresses(user).map(normalizeString);
    if (!addresses.includes(want)) return false;
  }
  if (args.role != null) {
    if (!dataset.hasRoles) {
      fail(`--role requires a context dataset; ${dataset.name} does not expose roleTags`);
    }
    const tags = Array.isArray(user.roleTags) ? user.roleTags.map(normalizeString) : [];
    if (!tags.includes(args.role)) return false;
  }
  if (args.query != null) {
    const needle = normalizeString(args.query);
    const haystack = [
      user.fid,
      user.username,
      user.displayName,
      user.bio,
      user.custodyAddress,
      ...collectAddresses(user),
      ...(Array.isArray(user.roleTags) ? user.roleTags : []),
    ]
      .map((value) => String(value ?? ""))
      .join("\n")
      .toLowerCase();
    if (!haystack.includes(needle)) return false;
  }

  return true;
}

function summarizeResult(user) {
  const primary = user.verifiedAddresses?.primary ?? {};
  return {
    fid: user.fid,
    username: user.username,
    displayName: user.displayName,
    score: user.score,
    followerCount: user.followerCount,
    followingCount: user.followingCount,
    roleTags: Array.isArray(user.roleTags) ? user.roleTags : [],
    lastCastAt: user.roleTagMeta?.activity?.lastCastAt ?? null,
    custodyAddress: user.custodyAddress ?? null,
    primaryEth: primary.eth ?? null,
    primarySol: primary.sol ?? null,
    bio: user.bio ?? null,
  };
}

function renderText(results, metadata) {
  const lines = [
    `Dataset: ${metadata.dataset}`,
    `Rows scanned: ${metadata.totalRows}`,
    `Matches: ${results.length}`,
  ];

  for (const user of results) {
    const summary = summarizeResult(user);
    lines.push("");
    lines.push(`- fid: ${summary.fid}`);
    lines.push(`  username: ${summary.username ?? ""}`);
    lines.push(`  displayName: ${summary.displayName ?? ""}`);
    lines.push(`  score: ${summary.score ?? ""}`);
    lines.push(`  followers: ${summary.followerCount ?? 0}`);
    lines.push(`  roles: ${summary.roleTags.length ? summary.roleTags.join(", ") : "none"}`);
    if (summary.lastCastAt) lines.push(`  lastCastAt: ${summary.lastCastAt}`);
    if (summary.custodyAddress) lines.push(`  custody: ${summary.custodyAddress}`);
    if (summary.primaryEth) lines.push(`  primaryEth: ${summary.primaryEth}`);
    if (summary.primarySol) lines.push(`  primarySol: ${summary.primarySol}`);
    if (summary.bio) lines.push(`  bio: ${summary.bio}`);
  }

  return lines.join("\n");
}

const args = parseArgs(process.argv.slice(2));
if (args.help) {
  console.log(usage());
  process.exit(0);
}

if ([args.fid, args.username, args.address, args.role, args.query].every((value) => value == null)) {
  fail("Provide at least one filter: --fid, --username, --address, --role, or --query");
}

const dataset = await resolveDataset(args.dataset);
const users = await readJson(dataset.usersPath);
if (!Array.isArray(users)) {
  fail(`Dataset is not an array: ${dataset.usersPath}`);
}

const filtered = users.filter((user) => matchesQuery(user, args, dataset)).slice(0, args.limit);
const payload = {
  dataset: dataset.name,
  totalRows: users.length,
  matches: filtered.length,
  filters: {
    fid: args.fid,
    username: args.username,
    address: args.address,
    role: args.role,
    query: args.query,
    limit: args.limit,
  },
  results: filtered.map(summarizeResult),
};

if (args.json) {
  console.log(JSON.stringify(payload, null, 2));
} else {
  console.log(renderText(filtered, payload));
}
