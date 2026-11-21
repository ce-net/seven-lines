# Seven lines

```rust
#[ce::cell(name = "orders", version = 1, replicas = 3)]
#[record(cell = orders)] struct Order { sku: Sku, qty: u32, paid: Money }
#[op(cap = "orders.place")] async fn place(cx: Cx, o: Order) -> Result<Receipt> {
    let charge = cx.pay().charge(o.paid).await?;
    let note = cx.ai().ask(Quota::agent(), summary(&o)).await?;
    cx.twin().put(&o).audit(&charge).await?;
    Ok(Receipt::new(charge, note)) }
```

That file is all of [`examples/orders/src/main.rs`](examples/orders/src/main.rs). CI checks that it is 7 lines and
that it builds. `fn main` is generated. It gives you:

| Line | What it does |
|---|---|
| 1 `#[ce::cell(.., replicas = 3)]` | The prelude and a generated `main`: one binary is a replica (`serve`), its client (`call`, `status`), its deployer (`up`, `down`) and its proof (`prove`). Three replicas with one leader per epoch, elected by majority over a quorum log ([live-shop](https://github.com/ce-net/live-shop)'s `shop-replog`, which uses epoch fencing). |
| 2 `#[record(cell = orders)]` | serde, `Clone` and `Debug`, the `Record` binding to the cell, and a schema entry in `describe`. |
| 3 `#[op(cap = "orders.place")]` | The op runs only for an ed25519 capability chain that holds `orders:place` and roots at the owner key in `cell.lock`. The proof signs the op, the idempotency key and the body. Every refusal is written to the audit stream. |
| 4 `cx.pay().charge(..)` | A test-mode charge. The ledger row is a log entry keyed by (request, step). `cx.pay().live()` needs `pay:live` in the chain, and even then this build refuses, because it has no live provider. It never moves real money. |
| 5 `cx.ai().ask(Quota::agent(), ..)` | First the cell's own quota for the calling agent (kept in the log), then an ask through `sub.gateway`, the owner's subscriptions with the gateway's own bucket. With no gateway (CI), a local stand-in answers and is labelled `vendor: local`. |
| 6 `cx.twin().put(&o).audit(&charge)` | The record goes into the cell's twin, and a row goes into the append-only audit stream (`hash = sha256(prev, seq, kind, key, body)`). The returned receipt carries `seq` and `hash`. |
| 7 `Ok(Receipt::new(..)) }` | The output is stored under the request key, so every retry of the key gets the same receipt. |

## Exactly once across a leader's death

Every effect the op makes through `Cx` is step `n` of its request. When a step commits, it is in the log on a
majority of replicas. Suppose the leader dies after charging and before the order is done. The client retries the
same key at the next leader, which runs `place` again. That leader finds step 0 (the charge) already committed and
reuses it, so no second charge is made. It then asks the AI and writes the twin. Two leaders that race on the same
step add one ledger row between them, because the first committed entry wins. One gap: an AI ask that was in flight
when the leader died is asked again. Money is never charged twice.

## Run it (fresh clone, public dependencies only)

```sh
git clone https://github.com/ce-net/seven-lines && cd seven-lines
cargo test --workspace --release      # 4 machine + 7 in-process ops + 2 process tests (one kills the leader mid-order)
cargo build --release
target/release/orders describe
```

Three replicas on your own machine:

```sh
K=$(target/release/orders keygen)            # {"seed": .., "public": ..}; keep the seed in your secret store
# write cell.lock: [cell] name = "orders", root = "<public>", then three [[replica]] node/addr entries on 127.0.0.1
for i in 0 1 2; do SEVEN_AI=local SEVEN_AI_LOCAL_MS=1500 target/release/orders serve --me $i & done
SEVEN_OWNER_SEED=<seed> target/release/orders call place '{"sku":"TENT-2P","qty":1,"paid":{"minor":249900,"cur":"SEK"}}'
SEVEN_OWNER_SEED=<seed> target/release/orders prove     # kills the leader mid-order; restart it and it catches up
```

## `up`: one verb on ce-net

`orders up` reads `cell.lock` and runs four steps:

1. Build: `cell.build Build{repo, rev}` builds the commit, not the working tree, and publishes it to artifact.index.
2. Pin: the rev and each target's sha256 go into `cell.lock`.
3. Install: a replica on this node runs the pinned binary under `systemd-run --user` with `CPUQuota=50%` and
   `MemoryMax=256M`. A replica on another node is installed through that node's
   `cell.install Install{Commit{repo, rev}}`.
4. Start and replicate: `up` waits until one leader is ready and every replica names it.

No line of the cell names a machine. Only `cell.lock` does.

## The recorded run (2026-11-19)

Page: <https://ce-net.github.io/seven-lines/>. Raw files are in [`docs/runs/`](docs/runs/).

| | |
|---|---|
| up | cell.build 121.8 s (aarch64-unknown-linux-gnu, release `e3a063f8…`); 3 units started; a leader 0.8 s after start, with 3/3 replicas following it |
| order A | 4.9 s end to end; test charge `ch_test_71235790…`; AI note from `claude-haiku-5-5` through sub.gateway |
| refusal | a chain with only `orders:read` was refused for `place`, and the refusal is an audit row |
| order B | the leader got an external SIGKILL once its charge had committed; a new leader was ready **758 ms** later (term 67 to 68); the order finished in 5.3 s with 27 client tries; the new leader found 1 step committed and reused it |
| exactly once | 1 charge row for B on each of the 3 replicas, with the same id. The killed replica was restarted by systemd and caught up. Sending the same request again returned `replayed: true` with the same charge. The audit chains verify, and the audit heads are equal on all 3 |

Where the run happened: all three replicas were on one server, one replica plus two simulated nodes on other ports.
The replica on the second machine is in the deployment lock (kept outside the repository, named by `SEVEN_LOCK`), but `cell.install` there refused the install
(`Right("cell.install:install")`). The grant is requested and the proof runs again when it arrives.

## A second cell: `ops` (2026-11-21)

[`examples/ops/src/main.rs`](examples/ops/src/main.rs), whole file:

```rust
#[ce::cell(name = "ops", version = 1, replicas = 3, skill = "Watch the fleet; propose; apply only with a grant.")]
#[record(cell = ops)] struct Change { change: FleetChange, note: String }
#[op(cap = "ops.propose")] async fn propose(cx: Cx, q: String) -> Result<Change> {
    let fleet = cx.fleet().snapshot().await?; let note = cx.ai().ask(Quota::agent(), fleet.question(&q)).await?;
    let c = Change { change: fleet.proposal(), note: note.text }; cx.twin().put(&c).await?; Ok(c) }
#[op(cap = "ops.apply", grant = Passkey)] async fn apply(cx: Cx, c: Change) -> Result<Applied> {
    cx.fleet().apply(c.change).audit().await }
```

Three additions to the runtime make it work:

- `grant = Passkey` on `#[op]`: the call also carries a grant, one link signed by the owner key (in ce the owner key
  signs only after a passkey tap). The grant is bound to the caller's key and to the sha256 of this exact input, and it
  can be used once. A missing grant, a grant for another input, another caller or another signer, and a grant already
  spent on another request are all refused, and each refusal is an audit row. A spent grant is an audit row too.
  Without a grant the call answers `NeedsGrant { cell, op, caller, input_hash, summary }`: the client posts it as a
  card, the owner's tap signs a one-use grant for exactly that ask (`<cell> grant`, `seven::one_use`) with the root or
  an owner device key that cell.lock lists in `[cell] owners`, and the same call is retried with it (`--grant-file`).
- `SKILL.md`: `<cell> skill` prints it from `describe` (the `skill =` text, each op with its cap, grant kind, input and
  output types, a worked `call` line, the refusals); `up` writes it next to cell.lock and each example tests that the
  committed file equals it.
- `home = Device` on `#[cell]`: exactly one replica (`replicas = 1` is then the only count that compiles), and
  `serve` refuses to start on any device except the one cell.lock names.
- `cx.fleet()`: `snapshot()` reads the cells on this node (systemd user units, `SEVEN_FLEET_UNITS`, default
  `seven-*`) or whatever the deployment's `SEVEN_FLEET_CMD` prints for a whole mesh. `apply()` takes a closed enum
  and does exactly one kind today, `Restart { cell }` of a cell the snapshot lists. Everything else returns `NotYet`.
  Each read and each restart is one recorded step of the request, so a retry never restarts twice.

`cargo test -p ops` runs three replicas on loopback over a fixed fleet with one cell down. It checks that apply
without a grant is refused, that grants for another `Change`, for another caller and from a non-owner key are
refused, that a valid grant restarts the cell exactly once (and its retry is a replay), that the spent grant is
refused on a new request, and that plan, grant and result are audit rows linked by the hash of the `Change`.

### The recorded run (2026-11-21)

[`docs/runs/2026-11-21-ops-live.json`](docs/runs/2026-11-21-ops-live.json): a live system on one server, three
replicas capped at `CPUQuota=25%`, and a demo cell (`CPUQuota=5%`) stopped on purpose. "Which cell is unhealthy?"
gave `Restart { cell: "seven-demo-sick" }` in 3.9 s with the note from claude/haiku through sub.gateway. Apply
without a grant was refused (audit row 7). With a grant it restarted the unit in 96 ms (146 ms at the client), and
the unit was `active` afterwards. The plan row (6) and the grant row (8) carry the same sha256 `b100ef35…`, the
result is row 9, and the three audit heads were equal. The owner key was a fresh demo key made for the run, so the
passkey tap on the owner's phone is not part of this proof. The live system was stopped afterwards.

## The same cell in TypeScript and Python (2026-11-21)

```ts
export default cell("orders", { replicas: 3 })
  .record("Order", { sku: Sku, qty: u32, paid: Money })
  .op("place", { cap: "orders.place" }, async (cx, o) => {
    const charge = await cx.pay.charge(o.paid);
    const note   = await cx.ai.ask(Quota.agent(), summary(o));
    await cx.twin.put(o).audit(charge);
    return Receipt(charge, note); });
```

```py
@cell("orders", replicas=3)
class Orders:
    class Order(Record): sku: Sku; qty: u32; paid: Money
    @op(cap="orders.place")
    async def place(self, cx, o: Order) -> Receipt:
        charge = await cx.pay.charge(o.paid)
        return Receipt(charge, await cx.ai.ask(Quota.agent(), summary(o)), await cx.twin.put(o).audit(charge))
```

The first is all of [`examples/orders-ts/cell.ts`](examples/orders-ts/cell.ts), 7 lines (a test counts). The Python
form is 7 lines too and ran the same proof (below). Its package is not in this repository yet: new Python files are
refused here by the owner's rule that new code is type-safe Rust, so `ce-seven` waits for the owner's decision on an
SDK exception. One runtime, three
languages: replication, the gate, the log, `cx` and exactly-once stay in Rust. A TypeScript or Python cell is a worker
process beside each replica.

```
seven-host (replica: gate, quorum log, cx layers)  <- seven.worker@1, one JSON frame per line on stdio ->  worker (cell.ts | cell.py)
```

- `seven-host --worker "<runner> <file>" <command>` is the same binary as a Rust cell (`serve`, `call`, `grant`,
  `prove`, `up`, `describe`, `skill`). Its ops table comes from the worker's `Describe`, and each op body is an
  `Invoke` on the worker. The contract is [`crates/seven-worker-api`](crates/seven-worker-api/src/lib.rs):
  `Describe`, `Invoke`, and `Cx` (worker to host) with the layers `Pay | Ai | Twin | Store | Fleet`.
- The gate runs in the host before `Invoke`, so a refused caller never reaches the worker. Every `Cx` with an effect is
  a step of the request in the log, keyed by `(request, step)` as for Rust. On a new leader the retried `Invoke` gets
  the recorded results back, so the charge happens once.
- The worker runs in a fresh network namespace (`unshare -rn`; `SEVEN_WORKER_JAIL=off` skips it, `on` requires it),
  at `nice 10` inside the replica's own CPU cap. Its environment holds no seeds or keys (`PATH`, `HOME`, `LANG`,
  `TMPDIR`, `NODE_PATH`, `PYTHONPATH` only). Its only way out is `Cx`.
- `describe` carries `contract`, the sha256 of the rest of describe. The TypeScript, Python and Rust orders cells have
  the same value (a test checks it).
- Packages with no dependencies: [`packages/seven-ts`](packages/seven-ts) (`@ce-net/seven`, Node 18+; `.ts` files
  written in the seven-line form load on any Node, and full TypeScript syntax needs Node 22.6+ type stripping or
  Bun) and `ce-seven` (Python 3.11+, not committed; see above). As with `#[cell]`, the runner brings
  `cell`, `Quota`, `Receipt`, `summary` and the scalar types into scope, so a cell file needs no import line.

```sh
cargo build --release
W="node packages/seven-ts/run.mjs examples/orders-ts/cell.ts"
target/release/seven-host --worker "$W" describe
cargo test -p seven-host --release    # 7 lines, same contract, and the leader-kill proof (Python too where ce-seven is present)
```

Recorded run: [`docs/runs/2026-11-21-definers.json`](docs/runs/2026-11-21-definers.json). Three replicas per language on
one machine. The leader was killed right after the charge committed, and the new leader replayed it from the log:
typescript: failover 985 ms, order 2445 ms, resumed 1 step, exactly once true; python: failover 1068 ms, order 2432 ms, resumed 1 step, exactly once true. Every replica held one charge and one audit receipt for the order, and the
audit heads were equal. The AI was the local stand-in (`vendor: local`) and payments were in test mode.

## Not done yet

- The run across two machines is waiting on the install grant for the second node (see above).
- Peer traffic (`vote`, `append`) is accepted only from the addresses in `cell.lock` and runs over the mesh. It does not
  carry a capability chain yet. In ce, `ce-replica`'s gated peer contract replaces this.
- The twin and audit stream live in the cell's own replicated log. Publishing them to the owner's twin cells and
  `state.store` is not built.
- `pay.live`: the grant is checked, but no live provider exists and no red live badge exists. Test mode only.
- `up` for a TypeScript or Python cell: `up` pins the artifact named after the cell (`orders`), not `seven-host`, so
  `ce up` on a `cell.ts` or `cell.py` and `ce new orders --ts | --py` are not wired yet. `up` already passes
  `SEVEN_WORKER` to the replicas it starts. Today the replicas run as `seven-host --worker .. serve`, as the test does.
- `ce-seven`, the Python definer, is built and proven but not committed: the repository refuses new Python files under
  the owner's type-safe-Rust rule. It needs his decision on an exception for SDK packages.
- The grant's passkey tap: `NeedsGrant` and the device-key grant are in the runtime; the card on the owner's phone that
  answers a `NeedsGrant` (decision.card kind `grant`) is not wired to it yet, so proofs sign with a test key.
- `cx.fleet()` reads one node's systemd units unless `SEVEN_FLEET_CMD` supplies the mesh view (orchestrator,
  store health, key holdings, AI spend). That mesh command is deployment-side and not in this repository.
- Failover takes about 0.6 to 1.2 s with a 150 ms heartbeat (`SHOP_BEAT_MS`). Election timeouts are randomised from 4 to 8 beats.

Licence: Apache-2.0.
