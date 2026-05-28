#[test]
fn stasis_tool_macro_ui_contracts() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/stasis_tool/pass_*.rs");
    t.compile_fail("tests/ui/stasis_tool/fail_*.rs");
}
