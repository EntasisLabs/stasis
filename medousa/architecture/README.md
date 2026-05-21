# Medousa Architecture Map

This directory maps the runtime architecture for:

- medousa-cli
- medousa-tui
- medousa-daemon

Focus areas:

- interaction boundaries
- state ownership and persistence
- runtime composition and tool orchestration
- where control flow crosses process boundaries

## Documents

1. [system-overview.md](system-overview.md)
2. [component-cli.md](component-cli.md)
3. [component-tui.md](component-tui.md)
4. [component-daemon.md](component-daemon.md)
5. [interaction-and-state-model.md](interaction-and-state-model.md)
6. [tui-performance-target-plan.md](tui-performance-target-plan.md)

## Primary Code Anchors

- `medousa/src/lib.rs`
- `medousa/src/tools.rs`
- `medousa/src/bin/medousa_cli.rs`
- `medousa/src/bin/medousa_tui.rs`
- `medousa/src/bin/medousa_daemon.rs`
- `medousa/src/session.rs`
- `medousa/src/events.rs`
- `medousa/src/daemon_api.rs`
