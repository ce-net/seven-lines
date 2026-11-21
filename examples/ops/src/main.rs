#[ce::cell(name = "ops", version = 1, replicas = 3, skill = "Watch the fleet; propose; apply only with a grant.")]
#[record(cell = ops)] struct Change { change: FleetChange, note: String }
#[op(cap = "ops.propose")] async fn propose(cx: Cx, q: String) -> Result<Change> {
    let fleet = cx.fleet().snapshot().await?; let note = cx.ai().ask(Quota::agent(), fleet.question(&q)).await?;
    let c = Change { change: fleet.proposal(), note: note.text }; cx.twin().put(&c).await?; Ok(c) }
#[op(cap = "ops.apply", grant = Passkey)] async fn apply(cx: Cx, c: Change) -> Result<Applied> {
    cx.fleet().apply(c.change).audit().await }
