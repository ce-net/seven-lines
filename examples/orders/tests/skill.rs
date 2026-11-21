//! examples/orders/SKILL.md is what `orders skill` (and so `up`) writes: the file an agent reads equals describe.
#[test]
fn skill_md_is_what_describe_says() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_orders")).arg("skill").output().unwrap();
    assert_eq!(include_str!("../SKILL.md"), String::from_utf8(out.stdout).unwrap(), "examples/orders/SKILL.md is stale: run `orders skill > examples/orders/SKILL.md`");
}
