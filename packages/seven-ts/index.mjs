// @ce-net/seven: a seven cell defined in TypeScript (or JavaScript). No dependencies.
//
//   export default cell("orders", { replicas: 3 })
//     .record("Order", { sku: Sku, qty: u32, paid: Money })
//     .op("place", { cap: "orders.place" }, async (cx, o) => { ... });
//
// The file is a worker beside one seven-host replica (seven.worker@1 over stdio). The host runs the gate before an
// op reaches this code, and every cx call below is a step in the replicated log: on a new leader the same request
// gets the recorded results back, so a charge happens once.

// scalar types, by their Rust names (the contract is the same whichever language wrote it)
export const Sku = "Sku", Money = "Money", u32 = "u32", u64 = "u64", i64 = "i64", f64 = "f64", bool = "bool", Str = "String", Value = "Value";

/// AI quotas: a bucket per calling agent (the host's SEVEN_AI_PER_DAY, default 20 a day), or a fixed count.
export const Quota = { agent: () => ({ agent: true }), perDay: (n) => ({ per_day: n }) };

/// What the orders example answers with.
export function Receipt(charge, note) {
  return { charge, note };
}

export const META = Symbol.for("ce-net.seven.record");

/// The record's fields in declaration order (Money as minor, cur), as the Rust runtime serialises it.
export function ordered(v, type, records) {
  if (v == null || typeof v !== "object") return v;
  if (type === "Money") return { minor: v.minor, cur: v.cur };
  const r = records && records.get(type);
  if (!r) return v;
  const out = {};
  for (const [f, t] of r.fields) out[f] = ordered(v[f], t, records);
  return out;
}

/// The prompt for a one-line note about a record (the same words as the Rust `summary`).
export function summary(o) {
  const m = o && o[META];
  if (!m) throw new Error("summary(o): o is not a record this cell declared");
  return `In one short sentence for the shop owner, note this new ${m.cell} ${m.name} (amounts are in minor units): ${JSON.stringify(ordered(o, m.name, m.records))}`;
}

export class Refused extends Error {
  constructor(err) {
    super(typeof err === "string" ? err : JSON.stringify(err));
    this.seven = err;
  }
}

class Cell {
  constructor(name, opts = {}) {
    this.name = name;
    this.opts = opts;
    this.records = new Map();
    this.ops = new Map();
    this.last = null;
  }
  record(name, fields) {
    this.records.set(name, { name, fields: Object.entries(fields) });
    this.last = name;
    return this;
  }
  op(name, opts, fn) {
    if (typeof opts === "function") [opts, fn] = [{}, opts];
    if (!opts.cap) throw new Error(`op ${name}: cap is required ("${this.name}.<right>")`);
    this.ops.set(name, {
      name,
      cap: opts.cap,
      input: opts.in ?? this.last ?? "Value",
      output: opts.out ?? "Receipt",
      grant: opts.grant === "passkey" || opts.grant === "Passkey" ? "Passkey" : "None",
      fn,
    });
    return this;
  }
  /// seven.worker@1 Describe
  def() {
    const home = this.opts.home === "device" || this.opts.home === "Device" ? "Device" : "Fleet";
    return {
      name: this.name,
      version: this.opts.version ?? 1,
      replicas: this.opts.replicas ?? null,
      home,
      skill: this.opts.skill ?? "",
      records: [...this.records.values()].map((r) => ({ name: r.name, fields: r.fields })),
      ops: [...this.ops.values()].map(({ fn, ...o }) => o),
    };
  }
  /// a record value as an op sees it: the fields, plus what summary and twin.put need to know
  wrap(type, v) {
    if (!this.records.has(type) || v == null || typeof v !== "object") return v;
    const o = { ...v };
    Object.defineProperty(o, META, { value: { cell: this.name, name: type, records: this.records } });
    return o;
  }
}

export function cell(name, opts) {
  return new Cell(name, opts);
}

export default { cell, Quota, Receipt, summary, Sku, Money, u32, u64, i64, f64, bool, Str, Value, Refused };
