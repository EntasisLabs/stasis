// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item expanded "><a href="introduction.html"><strong aria-hidden="true">1.</strong> Introduction</a></li><li class="chapter-item expanded "><a href="getting-started.html"><strong aria-hidden="true">2.</strong> Getting Started</a></li><li class="chapter-item expanded "><a href="architecture-overview.html"><strong aria-hidden="true">3.</strong> Architecture Overview</a></li><li class="chapter-item expanded "><a href="runtime-job-design.html"><strong aria-hidden="true">4.</strong> Job Runtime Design</a></li><li class="chapter-item expanded "><a href="runtime-builder.html"><strong aria-hidden="true">5.</strong> Runtime Builder and Wiring Guide</a></li><li class="chapter-item expanded "><a href="recurring-jobs.html"><strong aria-hidden="true">6.</strong> Recurring Jobs</a></li><li class="chapter-item expanded "><a href="stasisd.html"><strong aria-hidden="true">7.</strong> stasisd Declarative Engine</a></li><li class="chapter-item expanded "><a href="stasisd-runbook.html"><strong aria-hidden="true">8.</strong> stasisd Operator Runbook</a></li><li class="chapter-item expanded "><a href="retention-replay.html"><strong aria-hidden="true">9.</strong> Retention and Replay</a></li><li class="chapter-item expanded "><a href="lineage-observability.html"><strong aria-hidden="true">10.</strong> Lineage and Observability</a></li><li class="chapter-item expanded "><a href="opentelemetry.html"><strong aria-hidden="true">11.</strong> OpenTelemetry</a></li><li class="chapter-item expanded "><a href="orchestration-patterns.html"><strong aria-hidden="true">12.</strong> Orchestration Patterns</a></li><li class="chapter-item expanded "><a href="chat-middleware.html"><strong aria-hidden="true">13.</strong> Chat Middleware Pipeline</a></li><li class="chapter-item expanded "><a href="grapheme-workflow-handlers.html"><strong aria-hidden="true">14.</strong> Grapheme Workflow Handlers</a></li><li class="chapter-item expanded "><a href="agent-coordination.html"><strong aria-hidden="true">15.</strong> Agent Coordination</a></li><li class="chapter-item expanded "><a href="memory-operations.html"><strong aria-hidden="true">16.</strong> Memory Operations Reference</a></li><li class="chapter-item expanded "><a href="identity-memory-layer.html"><strong aria-hidden="true">17.</strong> Identity Memory Layer</a></li><li class="chapter-item expanded "><a href="environment-configuration.html"><strong aria-hidden="true">18.</strong> Environment Configuration</a></li><li class="chapter-item expanded "><a href="llm-providers.html"><strong aria-hidden="true">19.</strong> LLM Providers (genai 0.6.x)</a></li><li class="chapter-item expanded "><a href="agent-platform-contracts.html"><strong aria-hidden="true">20.</strong> Agent Platform Runtime Contracts</a></li><li class="chapter-item expanded "><a href="extension-points.html"><strong aria-hidden="true">21.</strong> Extension Points and Port Contracts</a></li><li class="chapter-item expanded "><a href="stasis-tool-macro.html"><strong aria-hidden="true">22.</strong> Stasis Tool Macro</a></li><li class="chapter-item expanded "><a href="control-plane-endpoint-routing.html"><strong aria-hidden="true">23.</strong> Control Plane and Endpoint Routing</a></li><li class="chapter-item expanded "><a href="command-center-dashboard.html"><strong aria-hidden="true">24.</strong> Dashboard Concept</a></li><li class="chapter-item expanded "><a href="dashboard-operations-guide.html"><strong aria-hidden="true">25.</strong> Dashboard Operations Guide</a></li><li class="chapter-item expanded "><a href="cookbook.html"><strong aria-hidden="true">26.</strong> Cookbook Overview</a></li><li class="chapter-item expanded "><a href="cookbook/platform-builder-external-participant.html"><strong aria-hidden="true">27.</strong> Platform Builder: External Participants</a></li><li class="chapter-item expanded "><a href="cookbook/production-agentic-workflows.html"><strong aria-hidden="true">28.</strong> Production Agentic Workflows</a></li><li class="chapter-item expanded "><a href="cookbook/runtime-dashboard-bootstrap.html"><strong aria-hidden="true">29.</strong> Runtime and Dashboard Bootstrap</a></li><li class="chapter-item expanded "><a href="cookbook/workflow-builder-starting-object.html"><strong aria-hidden="true">30.</strong> Workflow Builder Starting Object</a></li><li class="chapter-item expanded "><a href="cookbook/identity-memory-change-control.html"><strong aria-hidden="true">31.</strong> Identity Memory Change Control</a></li><li class="chapter-item expanded "><a href="cookbook/memory-maintenance-rollups.html"><strong aria-hidden="true">32.</strong> Memory Maintenance and Rollups</a></li><li class="chapter-item expanded "><a href="surrealdb-schema.html"><strong aria-hidden="true">33.</strong> SurrealDB Schema</a></li><li class="chapter-item expanded "><a href="adr.html"><strong aria-hidden="true">34.</strong> Architecture Decision Records</a></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
