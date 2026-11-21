export default cell("orders", { replicas: 3 })
  .record("Order", { sku: Sku, qty: u32, paid: Money })
  .op("place", { cap: "orders.place" }, async (cx, o) => {
    const charge = await cx.pay.charge(o.paid);
    const note   = await cx.ai.ask(Quota.agent(), summary(o));
    await cx.twin.put(o).audit(charge);
    return Receipt(charge, note); });
