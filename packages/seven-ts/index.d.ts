export type Money = { minor: number; cur: string };
export declare const Sku: "Sku", Money: "Money", u32: "u32", u64: "u64", i64: "i64", f64: "f64", bool: "bool", Str: "String", Value: "Value";
export type Quota = { agent: true } | { per_day: number };
export declare const Quota: { agent(): Quota; perDay(n: number): Quota };
export interface Cx {
  key: string; op: string; caller: string;
  pay: { charge(m: Money): Promise<any>; live(): { charge(m: Money): Promise<any> } };
  ai: { ask(q: Quota, prompt: string): Promise<any> };
  twin: { put(r: object): PromiseLike<any> & { audit(of: unknown): Promise<any> }; all(record: string): Promise<[string, any][]> };
  fleet: { snapshot(): Promise<any>; restart(cell: string): Promise<any> };
}
export interface OpOpts { cap: string; grant?: "passkey"; in?: string; out?: string }
export interface Cell {
  record(name: string, fields: Record<string, string>): Cell;
  op(name: string, opts: OpOpts, fn: (cx: Cx, input: any) => Promise<unknown>): Cell;
}
export declare function cell(name: string, opts?: { replicas?: number; version?: number; home?: "device" | "fleet"; skill?: string }): Cell;
export declare function Receipt(charge: unknown, note: unknown): { charge: unknown; note: unknown };
export declare function summary(o: object): string;
export declare class Refused extends Error { seven: unknown }
