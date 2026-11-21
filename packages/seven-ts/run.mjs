#!/usr/bin/env node
// node run.mjs <cell.ts|cell.mjs>: serve one cell file as a seven.worker@1 worker on stdin/stdout.
// The cell file needs no import: like Rust's #[cell], the runner brings cell, Quota, Receipt, summary and the scalar
// types into scope. `import ... from "@ce-net/seven"` works too.
import { createInterface } from "node:readline";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import * as seven from "./index.mjs";

const out = process.stdout;
console.log = console.info = console.error; // stdout carries frames only
const send = (f) => out.write(JSON.stringify(f) + "\n");

for (const [k, v] of Object.entries(seven)) if (k !== "default") globalThis[k] = v;

async function load(file) {
  const url = pathToFileURL(file).href;
  try {
    return (await import(url)).default;
  } catch (e) {
    if (e.code !== "ERR_UNKNOWN_FILE_EXTENSION") throw e;
    // a .ts file on a Node without type stripping: the seven-line form is plain JavaScript, so load its text
    const self = new URL("./index.mjs", import.meta.url).href;
    const src = readFileSync(file, "utf8").replace(/(from\s*["'])@ce-net\/seven(["'])/g, `$1${self}$2`);
    return (await import("data:text/javascript;base64," + Buffer.from(src).toString("base64"))).default;
  }
}

const file = process.argv[2];
if (!file) {
  console.error("usage: run.mjs <cell file>");
  process.exit(2);
}
const c = await load(file);
if (!c || typeof c.def !== "function") {
  console.error(`${file}: the default export is not a cell(...)`);
  process.exit(1);
}

let pending = null; // the resolver of the one open Cx
const lines = createInterface({ input: process.stdin, terminal: false });

function cxFor(inv) {
  let step = 0;
  const call = (layer, args) =>
    new Promise((resolve, reject) => {
      if (pending) return reject(new Error("one cx call at a time: await each before the next"));
      pending = { resolve, reject };
      send({ m: "Cx", request: inv.request, step: layer === "Store" ? step : step++, layer, args });
    });
  const charge = (live) => (m) => call("Pay", live ? { charge: m, live: true } : { charge: m });
  return {
    key: inv.request,
    op: inv.op,
    caller: inv.caller,
    pay: { charge: charge(false), live: () => ({ charge: charge(true) }) },
    ai: { ask: (quota, prompt) => call("Ai", { quota, prompt: String(prompt) }) },
    twin: {
      put(r) {
        const m = r && r[seven.META];
        if (!m) throw new Error("twin.put(r): r is not a record this cell declared");
        const body = seven.ordered(r, m.name, m.records);
        let p = null;
        return {
          audit: (of) => call("Twin", { record: m.name, body, of: of ?? null }),
          then: (res, rej) => (p ??= call("Twin", { record: m.name, body, of: null })).then(res, rej),
        };
      },
      all: (record) => call("Store", { all: record }),
    },
    fleet: { snapshot: () => call("Fleet", { snapshot: true }), restart: (cell) => call("Fleet", { restart: cell }) },
  };
}

async function invoke(inv) {
  const op = c.ops.get(inv.op);
  if (!op) return send({ m: "Return", err: { Input: `no op ${inv.op}` } });
  try {
    const ok = await op.fn(cxFor(inv), c.wrap(op.input, inv.input));
    send({ m: "Return", ok: ok === undefined ? null : ok });
  } catch (e) {
    send({ m: "Return", err: e instanceof seven.Refused ? e.seven : { Backend: String(e && e.message ? e.message : e) } });
  }
}

for await (const line of lines) {
  if (!line.trim()) continue;
  const f = JSON.parse(line);
  if (f.m === "Describe") send({ m: "Def", def: c.def() });
  else if (f.m === "Invoke") invoke(f);
  else if (f.m === "CxReply") {
    const p = pending;
    pending = null;
    if (!p) continue;
    if (f.err !== undefined && f.err !== null) p.reject(new seven.Refused(f.err));
    else p.resolve(f.ok);
  }
}
