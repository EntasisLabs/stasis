# Command Center Dashboard Concept

## Purpose

This page documents an operator-facing command center surface for runtime control and observability.

It is intentionally a concept slice focused on:

1. cluster situational awareness.
2. forwarded command outcomes and quick replay entry points.
3. queue ownership risk and failover readiness.

## Visual Prototype

<div class="cc-shell">
  <header class="cc-header">
    <div>
      <p class="cc-kicker">Stasis Control Plane</p>
      <h2>Distributed Command Center</h2>
    </div>
    <div class="cc-badges">
      <span>reference concept</span>
      <span>operator mode</span>
    </div>
  </header>

  <section class="cc-kpi-grid">
    <article class="cc-card">
      <h3>Live Coordinators</h3>
      <p class="cc-value">3</p>
      <p class="cc-meta">2 healthy | 1 degraded</p>
    </article>
    <article class="cc-card">
      <h3>Forwarded Commands (1h)</h3>
      <p class="cc-value">129</p>
      <p class="cc-meta">7 failed | 4 replay candidates</p>
    </article>
    <article class="cc-card">
      <h3>Queue Ownership Conflicts</h3>
      <p class="cc-value">2</p>
      <p class="cc-meta">single-owner violations</p>
    </article>
    <article class="cc-card">
      <h3>Idempotent Dedupe Hits</h3>
      <p class="cc-value">11</p>
      <p class="cc-meta">cluster_forward_idempotent_hits_total</p>
    </article>
  </section>

  <section class="cc-panels">
    <article class="cc-panel">
      <h3>Recent Forward Outcomes</h3>
      <table>
        <thead>
          <tr>
            <th>When</th>
            <th>Target</th>
            <th>Command</th>
            <th>Result</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>08:44:12</td>
            <td>eu-west</td>
            <td>coordinator.failover</td>
            <td><span class="cc-pill fail">failed</span></td>
            <td><button>Replay</button></td>
          </tr>
          <tr>
            <td>08:43:51</td>
            <td>us-east</td>
            <td>queue_ownership.rebalance</td>
            <td><span class="cc-pill ok">accepted</span></td>
            <td><button disabled>Replay</button></td>
          </tr>
          <tr>
            <td>08:43:08</td>
            <td>eu-west</td>
            <td>coordinator.handoff</td>
            <td><span class="cc-pill ok">accepted</span></td>
            <td><button disabled>Replay</button></td>
          </tr>
        </tbody>
      </table>
    </article>

    <article class="cc-panel">
      <h3>Queue Ownership Heatmap</h3>
      <ul>
        <li><span>default</span><strong>node-a, node-c</strong></li>
        <li><span>priority</span><strong>node-b</strong></li>
        <li><span>dlq</span><strong>node-a</strong></li>
        <li><span>memory-ops</span><strong>node-c</strong></li>
      </ul>
      <p class="cc-footnote">This panel is intended to bind to queue ownership health and conflict tracing events.</p>
    </article>
  </section>
</div>

<style>
.cc-shell {
  --ink: #171b24;
  --muted: #506178;
  --surface: #f7f7f2;
  --surface-strong: #ece8da;
  --line: #d7d0bb;
  --accent: #b14a1f;
  --accent-2: #177e89;
  --ok: #2b7a0b;
  --fail: #a40f2a;
  color: var(--ink);
  background:
    radial-gradient(130% 70% at 100% 0%, rgba(177, 74, 31, 0.14), transparent 52%),
    radial-gradient(90% 50% at 0% 100%, rgba(23, 126, 137, 0.14), transparent 56%),
    linear-gradient(135deg, #fbfaf5 0%, #f6f2e7 100%);
  border: 1px solid var(--line);
  border-radius: 18px;
  padding: 22px;
  font-family: "IBM Plex Sans", "Manrope", "Segoe UI", sans-serif;
}

.cc-header {
  display: flex;
  gap: 16px;
  justify-content: space-between;
  align-items: flex-end;
  margin-bottom: 16px;
}

.cc-header h2 {
  margin: 4px 0 0;
  font-size: clamp(1.25rem, 2.6vw, 2rem);
  letter-spacing: -0.02em;
}

.cc-kicker {
  margin: 0;
  text-transform: uppercase;
  letter-spacing: 0.13em;
  font-size: 0.72rem;
  color: var(--muted);
}

.cc-badges {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.cc-badges span {
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 4px 10px;
  font-size: 0.78rem;
  background: var(--surface);
}

.cc-kpi-grid {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  margin-bottom: 14px;
}

.cc-card {
  border: 1px solid var(--line);
  border-radius: 14px;
  background: var(--surface);
  padding: 12px;
  animation: cc-rise 440ms ease both;
}

.cc-card h3 {
  margin: 0;
  font-size: 0.9rem;
  color: var(--muted);
  font-weight: 560;
}

.cc-value {
  margin: 5px 0;
  font-size: clamp(1.35rem, 3vw, 2rem);
  color: var(--ink);
  font-weight: 680;
}

.cc-meta {
  margin: 0;
  font-size: 0.8rem;
  color: var(--muted);
}

.cc-panels {
  display: grid;
  gap: 12px;
  grid-template-columns: 2.1fr 1fr;
}

.cc-panel {
  border: 1px solid var(--line);
  border-radius: 14px;
  background: color-mix(in srgb, var(--surface) 82%, white 18%);
  padding: 12px;
}

.cc-panel h3 {
  margin: 0 0 10px;
}

.cc-panel table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
}

.cc-panel th,
.cc-panel td {
  text-align: left;
  padding: 7px;
  border-bottom: 1px solid var(--line);
}

.cc-panel button {
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--surface-strong);
  padding: 5px 8px;
  cursor: pointer;
}

.cc-panel button[disabled] {
  opacity: 0.45;
  cursor: not-allowed;
}

.cc-pill {
  border-radius: 999px;
  padding: 3px 8px;
  font-size: 0.77rem;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  font-weight: 620;
}

.cc-pill.ok {
  color: var(--ok);
  background: rgba(43, 122, 11, 0.12);
}

.cc-pill.fail {
  color: var(--fail);
  background: rgba(164, 15, 42, 0.14);
}

.cc-panel ul {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: 8px;
}

.cc-panel li {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  border-bottom: 1px dashed var(--line);
  padding-bottom: 6px;
}

.cc-panel li span {
  color: var(--muted);
}

.cc-footnote {
  margin: 12px 0 0;
  color: var(--muted);
  font-size: 0.8rem;
}

@keyframes cc-rise {
  from {
    opacity: 0;
    transform: translateY(9px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@media (max-width: 900px) {
  .cc-panels {
    grid-template-columns: 1fr;
  }

  .cc-header {
    flex-direction: column;
    align-items: flex-start;
  }
}
</style>
