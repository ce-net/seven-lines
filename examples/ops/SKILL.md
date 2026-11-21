# ops (v1)

Watch the fleet; propose; apply only with a grant.

Runs as 3 replicas over one quorum log; every effect happens once. Generated from `ops describe`; do not edit by hand.

## Ops

### `apply`

- cap `ops.apply` (chain right `ops:apply`)
- input `Change`, answers `Applied`
- grant = Passkey: without a grant it answers `NeedsGrant { cell, op, caller, input_hash, summary }`; the owner's tap signs a one-use grant for that exact input (`grant`), and the same call is retried with it (`--grant-file`)
- call: `ops call apply '<Change as JSON>' --key <idempotency key> --grant-file <grant.json>`

### `propose`

- cap `ops.propose` (chain right `ops:propose`)
- input `String`, answers `Change`
- call: `ops call propose '<String as JSON>' --key <idempotency key>`

## Records

- `Change` { change: FleetChange, note: String }

## Refusals

- `Denied(why)`: the caller's chain does not hold the right (an audit row)
- `NeedsGrant(..)`: a Passkey op without the owner's one-use grant
- `NotLeader(addr)`, `NoQuorum`: retry with the same key; it runs once
- `Quota { agent, used, limit }`: the cell's AI bucket for that agent is spent today
- `Backend(why)`, `Input(why)`
