# orders (v1)



Runs as 3 replicas over one quorum log; every effect happens once. Generated from `orders describe`; do not edit by hand.

## Ops

### `place`

- cap `orders.place` (chain right `orders:place`)
- input `Order`, answers `Receipt`
- call: `orders call place '<Order as JSON>' --key <idempotency key>`

## Records

- `Order` { sku: Sku, qty: u32, paid: Money }

## Refusals

- `Denied(why)`: the caller's chain does not hold the right (an audit row)
- `NeedsGrant(..)`: a Passkey op without the owner's one-use grant
- `NotLeader(addr)`, `NoQuorum`: retry with the same key; it runs once
- `Quota { agent, used, limit }`: the cell's AI bucket for that agent is spent today
- `Backend(why)`, `Input(why)`
