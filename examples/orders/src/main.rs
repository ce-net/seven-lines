#[ce::cell(name = "orders", version = 1, replicas = 3)]
#[record(cell = orders)] struct Order { sku: Sku, qty: u32, paid: Money }
#[op(cap = "orders.place")] async fn place(cx: Cx, o: Order) -> Result<Receipt> {
    let charge = cx.pay().charge(o.paid).await?;
    let note = cx.ai().ask(Quota::agent(), summary(&o)).await?;
    cx.twin().put(&o).audit(&charge).await?;
    Ok(Receipt::new(charge, note)) }
