// implement-plan: execute the current session plan end-to-end with quality gates.
// Zero-arg default: uses this session's plan.md and auto-derives Go packages to test.
// Optional args: { plan?: path override, packages?: " ./pkg/ ..." override, branch?: string }

let meta = #{
    name: "implement-plan",
    description: "Implement the current session plan (or args.plan), auto-detect packages, then gate on ponytail, idioms, tests, coverage, and optimization",
    when_to_use: "After approving a session plan — run with no args to implement plan.md end-to-end with ship-quality checks",
    phases: [
        #{ title: "Orient", detail: "Resolve session plan.md and map work + packages" },
        #{ title: "Implement", detail: "Implement the entire plan with minimal, idiomatic Go" },
        #{ title: "Audit", detail: "Parallel: ponytail, idioms, tests, coverage" },
        #{ title: "Remediate", detail: "Fix audit findings; re-run tests" },
        #{ title: "Optimize", detail: "Stdlib, memory, allocs, network/IO performance" },
        #{ title: "Verify", detail: "Final green gate and ship report" },
    ],
};

// --- Schemas -----------------------------------------------------------------

let workplan_schema = #{
    "type": "object",
    "required": ["plan_path", "summary", "test_packages", "phases", "success_criteria", "out_of_scope"],
    "properties": #{
        "plan_path": #{ "type": "string", "description": "Absolute path to the plan file that will be implemented" },
        "summary": #{ "type": "string" },
        "test_packages": #{
            "type": "array",
            "items": #{ "type": "string" },
            "minItems": 1,
            "maxItems": 32,
            "description": "Go package patterns to test/cover, e.g. ./vfs/ — derived from plan, never user-required",
        },
        "phases": #{
            "type": "array", "maxItems": 16,
            "items": #{
                "type": "object",
                "required": ["id", "title", "work"],
                "properties": #{
                    "id": #{ "type": "string" },
                    "title": #{ "type": "string" },
                    "work": #{ "type": "string" },
                    "files": #{ "type": "array", "items": #{ "type": "string" } },
                },
            },
        },
        "success_criteria": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 24 },
        "out_of_scope": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 16 },
    },
};

let implement_schema = #{
    "type": "object",
    "required": ["done", "summary", "files_changed", "tests_run", "tests_passed", "remaining"],
    "properties": #{
        "done": #{ "type": "boolean" },
        "summary": #{ "type": "string" },
        "files_changed": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 64 },
        "tests_run": #{ "type": "string" },
        "tests_passed": #{ "type": "boolean" },
        "remaining": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 24 },
        "notes": #{ "type": "string" },
    },
};

let findings_schema = #{
    "type": "object",
    "required": ["findings", "verdict"],
    "properties": #{
        "verdict": #{ "type": "string", "enum": ["pass", "fail"] },
        "summary": #{ "type": "string" },
        "findings": #{
            "type": "array", "maxItems": 20,
            "items": #{
                "type": "object",
                "required": ["severity", "location", "issue", "fix"],
                "properties": #{
                    "severity": #{ "type": "string", "enum": ["blocker", "major", "minor"] },
                    "location": #{ "type": "string" },
                    "issue": #{ "type": "string" },
                    "fix": #{ "type": "string" },
                    "evidence": #{ "type": "string" },
                },
            },
        },
        "metrics": #{ "type": "object" },
    },
};

let remediate_schema = #{
    "type": "object",
    "required": ["done", "summary", "fixed", "deferred", "tests_passed"],
    "properties": #{
        "done": #{ "type": "boolean" },
        "summary": #{ "type": "string" },
        "fixed": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 40 },
        "deferred": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 20 },
        "tests_passed": #{ "type": "boolean" },
        "coverage_note": #{ "type": "string" },
    },
};

let optimize_schema = #{
    "type": "object",
    "required": ["done", "summary", "changes", "tests_passed"],
    "properties": #{
        "done": #{ "type": "boolean" },
        "summary": #{ "type": "string" },
        "changes": #{
            "type": "array", "maxItems": 30,
            "items": #{
                "type": "object",
                "required": ["area", "what", "why"],
                "properties": #{
                    "area": #{ "type": "string", "enum": ["stdlib", "memory", "allocs", "network", "io", "concurrency", "other"] },
                    "what": #{ "type": "string" },
                    "why": #{ "type": "string" },
                    "location": #{ "type": "string" },
                },
            },
        },
        "skipped": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 20 },
        "tests_passed": #{ "type": "boolean" },
    },
};

let verify_schema = #{
    "type": "object",
    "required": ["ship_ready", "summary", "checks"],
    "properties": #{
        "ship_ready": #{ "type": "boolean" },
        "summary": #{ "type": "string" },
        "checks": #{
            "type": "array", "minItems": 1, "maxItems": 16,
            "items": #{
                "type": "object",
                "required": ["name", "pass", "detail"],
                "properties": #{
                    "name": #{ "type": "string" },
                    "pass": #{ "type": "boolean" },
                    "detail": #{ "type": "string" },
                },
            },
        },
        "blockers": #{ "type": "array", "items": #{ "type": "string" }, "maxItems": 20 },
        "coverage_summary": #{ "type": "string" },
    },
};

// --- Helpers -----------------------------------------------------------------

// Safe length: agent canned/partial outputs may omit arrays (unit).
fn alen(x) {
    if x == () { return 0; }
    if type_of(x) == "array" { return x.len(); }
    0
}

fn as_str(x) {
    if x == () { return ""; }
    if type_of(x) == "string" { return x; }
    x.to_string()
}

// --- Args (all optional) -----------------------------------------------------
// Default: current session plan.md + packages derived from the plan/diff.

let plan_override = "";
if args != () && args.plan != () {
    plan_override = as_str(args.plan);
}
let packages_override = "";
if args != () && args.packages != () {
    packages_override = as_str(args.packages);
}
let branch = "";
if args != () && args.branch != () {
    branch = as_str(args.branch);
}

let plan_override_json = json_encode(plan_override);
let packages_override_json = json_encode(packages_override);
let branch_json = json_encode(branch);

// Shared project law (embedded so cold agents do not invent process).
let law = "";
law += "PROJECT LAW (Tacklr / Agents.md) — obey strictly:\n";
law += "1) Go: modern idioms — iterators, generics where they pay, stdlib slices/maps/sync/strings, ";
law += "error wrapping (%w), new(T) expressions. Prefer stdlib over third-party packages.\n";
law += "2) Ponytail ladder: YAGNI → reuse in-repo → stdlib → existing deps → shortest correct code. ";
law += "No speculative abstractions, no one-impl interfaces, no factories for one product, no deps for a few lines.\n";
law += "3) Testing philosophy (MANDATORY):\n";
law += "   - Prefer high-value, outcome-oriented integration tests over unit-test sprawl.\n";
law += "   - Test outcomes that SHOULD happen; never write tests whose primary claim is that something ";
law += "does NOT exist / did NOT happen (those are brittle and low signal).\n";
law += "   - Minimal mocking; prefer in-memory implementations / testcontainers.\n";
law += "   - AAA pattern; self-documenting test names named after the case.\n";
law += "   - Aim for ~100% coverage of CHANGED code via fewest tests that cover distinct return paths; ";
law += "no duplicate return-path coverage across cases.\n";
law += "4) Do not commit unless asked. Run gofmt, go test, golangci-lint (or go vet) on touched packages.\n";
law += "5) Use tools: read_file, grep, list_dir, run_terminal_command. Inspect real code; never answer from memory.\n";

// --- Phase: Orient -----------------------------------------------------------

phase("Orient");
if plan_override != "" {
    log("Orient: plan override " + plan_override);
} else {
    log("Orient: resolve current session plan.md (no args.plan)");
}

let orient_prompt = "";
orient_prompt += law;
orient_prompt += "\nTASK: Orient for full plan implementation. Zero-config when possible.\n";
orient_prompt += "Optional plan path override (JSON-encoded; empty = use session plan): " + plan_override_json + "\n";
orient_prompt += "Optional packages override (JSON-encoded; empty = YOU derive packages): " + packages_override_json + "\n";
orient_prompt += "Optional base branch (JSON-encoded): " + branch_json + "\n\n";
orient_prompt += "### A) Resolve plan_path (required)\n";
orient_prompt += "If the override is non-empty: use it (absolute or repo-relative; resolve to absolute).\n";
orient_prompt += "Else resolve the CURRENT SESSION plan file:\n";
orient_prompt += "1) Read env GROK_SESSION_ID (shell: printenv GROK_SESSION_ID).\n";
orient_prompt += "2) Session plan lives at:\n";
orient_prompt += "   ~/.grok/sessions/<urlencode(cwd, safe='')>/<GROK_SESSION_ID>/plan.md\n";
orient_prompt += "   where urlencode uses percent-encoding of the absolute cwd with empty safe set\n";
orient_prompt += "   (Python: urllib.parse.quote(os.getcwd(), safe='') ).\n";
orient_prompt += "3) If GROK_SESSION_ID is missing, pick the most recently modified plan.md under\n";
orient_prompt += "   ~/.grok/sessions/<urlencode(cwd)>/*/plan.md for this workspace.\n";
orient_prompt += "4) read_file the plan in full. If missing/empty, set summary to explain failure and\n";
orient_prompt += "   still return schema fields (plan_path may be the attempted path; test_packages=['./...']).\n\n";
orient_prompt += "### B) Map the work\n";
orient_prompt += "1) Explore the live codebase for files/APIs the plan touches (grep, list_dir, read_file).\n";
orient_prompt += "2) Produce an implementation workplan covering the plan IN ITS ENTIRETY\n";
orient_prompt += "   (every in-scope success criterion). No scope creep beyond the plan.\n";
orient_prompt += "3) Record explicit out_of_scope from the plan.\n\n";
orient_prompt += "### C) Derive test_packages (user never has to pass packages)\n";
orient_prompt += "If packages override is non-empty: split on whitespace into test_packages.\n";
orient_prompt += "Else derive from plan file paths + git status/diff of likely touched dirs:\n";
orient_prompt += "- Prefer concrete patterns like ./vfs/ ./vfsindex/ (trailing slash).\n";
orient_prompt += "- Map each touched .go file to its package dir via go list or path rules.\n";
orient_prompt += "- Include the harness root ./ only if the plan changes root package files.\n";
orient_prompt += "- Never leave test_packages empty; if unsure, use the smallest set covering planned files.\n";
orient_prompt += "- Do NOT default to ./... unless the plan truly spans most of the module.\n\n";
orient_prompt += "Return structured output only. plan_path MUST be the absolute path you read.\n";

let orient = agent(orient_prompt, #{
    label: "orient",
    capability_mode: "execute",
    output_schema: workplan_schema,
});

if orient == () || !orient.success || orient.output == () {
    complete(#{
        ship_ready: false,
        summary: "Orient agent failed; cannot implement without a workplan.",
        stage: "orient",
    });
}

let work = orient.output;
let work_json = json_encode(work);

// Canonical plan path for all later phases (session plan or override).
let plan_path = as_str(work.plan_path);
if plan_path == "" {
    complete(#{
        ship_ready: false,
        summary: "Orient did not resolve plan_path (session plan.md missing?). Open plan mode or pass args.plan.",
        stage: "orient",
        workplan: work,
    });
}
let plan_json = json_encode(plan_path);
log("Workplan ready for " + plan_path + ": " + as_str(work.summary));

// --- Phase: Implement --------------------------------------------------------

phase("Implement");
log("Implementing plan in full: " + plan_path);

let impl_prompt = "";
impl_prompt += law;
impl_prompt += "\nTASK: IMPLEMENT THE ENTIRE PLAN. Leave nothing in-scope unfinished.\n";
impl_prompt += "Plan path: " + plan_json + "\n";
impl_prompt += "Structured workplan (JSON): " + work_json + "\n\n";
impl_prompt += "Rules for this phase:\n";
impl_prompt += "- Re-read the plan file and every file you will edit before changing it.\n";
impl_prompt += "- Implement every phase in the workplan that is in scope. Skip only listed out_of_scope items.\n";
impl_prompt += "- Ponytail FULL: shortest correct solution; delete redundancy as you go; no speculative APIs.\n";
impl_prompt += "- Prefer editing existing files over adding layers. Fewest files that work.\n";
impl_prompt += "- After changes: gofmt, go test on workplan test_packages, fix failures before finishing.\n";
impl_prompt += "- Add/adjust ONLY outcome-oriented tests required by the plan and coverage of new return paths.\n";
impl_prompt += "- Do NOT add mock-heavy suites or negative 'feature does not exist' tests.\n";
impl_prompt += "- Do NOT git commit unless the plan explicitly requires it.\n";
impl_prompt += "- capability: you may write files and run commands.\n\n";
impl_prompt += "When done, set done=true only if all in-scope success criteria are met and tests_passed=true.\n";
impl_prompt += "If blocked, done=false and list remaining blockers precisely.\n";

let impl = agent(impl_prompt, #{
    label: "implement",
    capability_mode: "all",
    output_schema: implement_schema,
});

if impl == () || !impl.success || impl.output == () {
    complete(#{
        ship_ready: false,
        summary: "Implement agent failed.",
        stage: "implement",
        workplan: work,
    });
}

log("Implement: done=" + as_str(impl.output.done) + " tests_passed=" + as_str(impl.output.tests_passed));
let impl_json = json_encode(impl.output);

if impl.output.done != true || impl.output.tests_passed != true {
    // One recovery pass before audits
    log("Implementation incomplete; one recovery pass");
    let recover_prompt = "";
    recover_prompt += law;
    recover_prompt += "\nTASK: Finish remaining in-scope plan work and get tests green.\n";
    recover_prompt += "Plan path: " + plan_json + "\n";
    recover_prompt += "Workplan: " + work_json + "\n";
    recover_prompt += "Prior implement result: " + impl_json + "\n";
    recover_prompt += "Complete remaining items. Run tests. No scope creep. Ponytail full.\n";
    let recover = agent(recover_prompt, #{
        label: "implement-recover",
        capability_mode: "all",
        output_schema: implement_schema,
        resume_from: impl.agent_id,
    });
    if recover != () && recover.success && recover.output != () {
        impl = recover;
        impl_json = json_encode(impl.output);
    }
}

// --- Phase: Audit (parallel) -------------------------------------------------

phase("Audit");
log("Parallel quality audits: ponytail, Go idioms, tests, coverage");

// Packages: from orient (or rare args.packages override already applied in orient).
// Never require the user to pass packages.
let pkg_focus = "";
if packages_override != "" {
    pkg_focus = packages_override;
} else if alen(work.test_packages) > 0 {
    let i = 0;
    while i < alen(work.test_packages) {
        if i > 0 { pkg_focus += " "; }
        pkg_focus += as_str(work.test_packages[i]);
        i += 1;
    }
}
if pkg_focus == "" {
    // Last resort only — orient is supposed to fill test_packages.
    pkg_focus = "./...";
}
let pkg_json = json_encode(pkg_focus);
log("Package focus: " + pkg_focus);
let files_json = json_encode(impl.output.files_changed);

let audit_jobs = [];

// 1) Ponytail / over-engineering
let p_ponytail = "";
p_ponytail += law;
p_ponytail += "\nTASK: PONYTAIL-REVIEW the implementation (over-engineering only).\n";
p_ponytail += "Plan: " + plan_json + "\nWorkplan: " + work_json + "\nImplement result: " + impl_json + "\n";
p_ponytail += "Files changed (hint): " + files_json + "\n\n";
p_ponytail += "Use git diff / read_file / grep on the actual code. Do not invent findings.\n";
p_ponytail += "Hunt ONLY: delete (dead/speculative), stdlib replacements, yagni abstractions, ";
p_ponytail += "shrink opportunities, reinvented helpers that already exist in-repo.\n";
p_ponytail += "OUT OF SCOPE for this auditor: pure correctness bugs, pure security, pure perf ";
p_ponytail += "(other auditors cover those). Still report if a bloated design causes obvious waste.\n";
p_ponytail += "severity: blocker = must delete/simplify before ship; major = should; minor = nice.\n";
p_ponytail += "verdict=pass only if zero blocker and zero major findings.\n";
p_ponytail += "Each finding needs location, issue, concrete fix (what to delete/replace with).\n";
audit_jobs.push(#{
    prompt: p_ponytail,
    label: "audit:ponytail",
    capability_mode: "read-only",
    output_schema: findings_schema,
});

// 2) Go idioms
let p_idioms = "";
p_idioms += law;
p_idioms += "\nTASK: Audit GO IDIOMS & style on the changed code.\n";
p_idioms += "Plan: " + plan_json + "\nFiles hint: " + files_json + "\nPackages: " + pkg_json + "\n\n";
p_idioms += "Use tools: git diff, read_file, go vet / golangci-lint if available via shell (read-only intent; ";
p_idioms += "running linters is allowed). Inspect real diffs.\n";
p_idioms += "Check for: proper error wrapping, context.Context first param, avoid interface{} when generics fit, ";
p_idioms += "slices/maps/strings stdlib, clear exported docs, no stutter names, correct sync usage, ";
p_idioms += "no unnecessary goroutines, table-driven tests only when they improve outcomes, ";
p_idioms += "fmt.Errorf with %w, closed channels/resources, io best practices.\n";
p_idioms += "verdict=pass only if zero blocker/major. Evidence must cite file:line or symbol.\n";
audit_jobs.push(#{
    prompt: p_idioms,
    label: "audit:go-idioms",
    capability_mode: "execute",
    output_schema: findings_schema,
});

// 3) Test guideline adherence
let p_tests = "";
p_tests += law;
p_tests += "\nTASK: Audit TESTS for guideline adherence (outcome-oriented philosophy).\n";
p_tests += "Plan: " + plan_json + "\nImplement: " + impl_json + "\nPackages: " + pkg_json + "\n\n";
p_tests += "Read every new/changed *_test.go related to this work. Use git diff and read_file.\n";
p_tests += "FAIL (blocker/major) when you find:\n";
p_tests += "- Mock-heavy tests that could use in-memory or real test doubles already in-repo\n";
p_tests += "- Tests whose main assertion is that something does NOT exist / did NOT happen\n";
p_tests += "- Tests of private implementation details that don't assert a user-visible outcome\n";
p_tests += "- Duplicate return-path coverage across cases (same branch proven twice)\n";
p_tests += "- Missing outcome coverage for new public return paths introduced by the plan\n";
p_tests += "- Brittle snapshots of incidental formatting without outcome value\n";
p_tests += "PASS examples: integration path proves replace-by-block_id updates body; index props present; ";
p_tests += "rev mismatch rejects write.\n";
p_tests += "verdict=pass only if zero blocker/major. Each finding must propose a concrete rewrite toward outcomes.\n";
audit_jobs.push(#{
    prompt: p_tests,
    label: "audit:tests",
    capability_mode: "read-only",
    output_schema: findings_schema,
});

// 4) Coverage
let p_cover = "";
p_cover += law;
p_cover += "\nTASK: Measure and analyze CODE COVERAGE for packages touched by the plan.\n";
p_cover += "Packages focus (JSON): " + pkg_json + "\nPlan: " + plan_json + "\nFiles: " + files_json + "\n\n";
p_cover += "Steps (use shell):\n";
p_cover += "1) Run: go test <packages> -coverprofile=/tmp/implement-plan-cover.out -count=1\n";
p_cover += "2) Run: go tool cover -func=/tmp/implement-plan-cover.out | tail -n 50 (and inspect low files)\n";
p_cover += "3) Map uncovered lines to NEW/CHANGED code from the plan (git diff). Ignore unrelated legacy gaps unless ";
p_cover += "trivial to close while here.\n";
p_cover += "4) Findings = specific uncovered return paths in changed code + the minimal outcome-oriented test to add.\n";
p_cover += "Put overall % and hotspots in metrics if possible (e.g. metrics.total_coverage string).\n";
p_cover += "verdict=pass only if changed code has no critical uncovered return paths (blocker/major empty).\n";
p_cover += "Do not demand 100% of the entire monorepo — focus on this plan's delta.\n";
audit_jobs.push(#{
    prompt: p_cover,
    label: "audit:coverage",
    capability_mode: "execute",
    output_schema: findings_schema,
});

let audits = parallel(audit_jobs);

// Collect findings fail-closed
let all_findings = [];
let audit_summaries = [];
let i = 0;
let audit_labels = ["ponytail", "go-idioms", "tests", "coverage"];
while i < alen(audits) {
    let r = audits[i];
    let label = audit_labels[i];
    if r == () || !r.success || r.output == () {
        all_findings.push(#{
            severity: "blocker",
            location: label,
            issue: "Audit agent failed or returned unusable output",
            fix: "Re-run audit; treat as failed gate",
            evidence: "success=false or empty output",
            source: label,
        });
        audit_summaries.push(label + ": FAILED_AGENT");
    } else {
        let v = r.output.verdict;
        let n = alen(r.output.findings);
        let j = 0;
        while j < n {
            let f = r.output.findings[j];
            // Copy into a new map so we can tag source without mutating schema objects.
            all_findings.push(#{
                severity: f.severity,
                location: f.location,
                issue: f.issue,
                fix: f.fix,
                evidence: f.evidence,
                source: label,
            });
            j += 1;
        }
        let vs = if v == () { "unknown" } else { as_str(v) };
        audit_summaries.push(label + ":" + vs + " findings=" + n.to_string());
    }
    i += 1;
}

let findings_json = json_encode(all_findings);
log("Audits complete: " + alen(audit_summaries).to_string() + " panels; raw findings=" + alen(all_findings).to_string());

// --- Phase: Remediate --------------------------------------------------------

phase("Remediate");

let has_actionable = false;
let k = 0;
while k < alen(all_findings) {
    let sev = all_findings[k].severity;
    if sev == "blocker" || sev == "major" {
        has_actionable = true;
    }
    k += 1;
}

let remediate_out = #{
    done: true,
    summary: "No blocker/major findings; skipped remediate agent",
    fixed: [],
    deferred: [],
    tests_passed: impl.output.tests_passed,
    coverage_note: "",
};

if has_actionable {
    log("Remediating blocker/major findings");
    let rem_prompt = "";
    rem_prompt += law;
    rem_prompt += "\nTASK: Fix ALL blocker and major audit findings. Minor may be deferred with reason.\n";
    rem_prompt += "Plan: " + plan_json + "\nWorkplan: " + work_json + "\n";
    rem_prompt += "Implement snapshot: " + impl_json + "\n";
    rem_prompt += "Findings JSON: " + findings_json + "\n\n";
    rem_prompt += "Rules:\n";
    rem_prompt += "- Apply ponytail deletions/simplifications first (shorter code preferred).\n";
    rem_prompt += "- Fix idiom issues with the smallest correct change.\n";
    rem_prompt += "- Rewrite bad tests into outcome-oriented integration tests; delete mock theater.\n";
    rem_prompt += "- Add minimal tests only for uncovered NEW return paths (no duplicate path coverage).\n";
    rem_prompt += "- Re-run go test on " + pkg_focus + " and fix until green.\n";
    rem_prompt += "- Prefer gofmt + golangci-lint clean on touched packages.\n";
    rem_prompt += "Set done=true only when blockers/majors are fixed and tests_passed=true.\n";
    let rem = agent(rem_prompt, #{
        label: "remediate",
        capability_mode: "all",
        output_schema: remediate_schema,
    });
    if rem != () && rem.success && rem.output != () {
        remediate_out = rem.output;
    } else {
        remediate_out = #{
            done: false,
            summary: "Remediate agent failed",
            fixed: [],
            deferred: [],
            tests_passed: false,
            coverage_note: "",
        };
    }
} else {
    log("No blocker/major findings — remediate skipped");
}

let rem_json = json_encode(remediate_out);

// --- Phase: Optimize ---------------------------------------------------------

phase("Optimize");
log("Optimization pass: stdlib, memory, allocs, network/IO");

let opt_prompt = "";
opt_prompt += law;
opt_prompt += "\nTASK: OPTIMIZE the implemented, remediated code for real efficiency — without over-engineering.\n";
opt_prompt += "Plan: " + plan_json + "\nPackages: " + pkg_json + "\n";
opt_prompt += "Files hint: " + files_json + "\nRemediate: " + rem_json + "\n\n";
opt_prompt += "Use tools to inspect hot paths in CHANGED code (read_file, grep, git diff). ";
opt_prompt += "Optional: go test -bench if benchmarks already exist; do not invent large bench suites.\n\n";
opt_prompt += "Hunt and fix when benefit is clear:\n";
opt_prompt += "- stdlib: replace hand-rolled with slices/maps/strings/bytes/strconv/cmp/sync primitives\n";
opt_prompt += "- memory/allocs: avoid unnecessary copies; reuse buffers where correct; pre-size slices; ";
opt_prompt += "don't retain huge strings; prefer streaming/line windows for large files if already in design\n";
opt_prompt += "- network: reuse clients, correct context timeouts/cancel, avoid chatty RPCs, batch when appropriate\n";
opt_prompt += "- io: avoid ReadAll on large bodies if streaming exists; close bodies; buffer sensibly\n";
opt_prompt += "- concurrency: no extra goroutines; avoid lock contention; prefer clear serial code if fast enough\n\n";
opt_prompt += "PONYTAIL CONSTRAINT: do not add caches, pools, or parallel frameworks without measured need. ";
opt_prompt += "If unsure, put the idea in skipped with reason 'need profile'. Prefer algorithmic clarity.\n";
opt_prompt += "After edits: gofmt + go test " + pkg_focus + " must pass. tests_passed must reflect reality.\n";
opt_prompt += "If already optimal, changes=[] and skipped notes why; done=true; still run tests.\n";

let opt = agent(opt_prompt, #{
    label: "optimize",
    capability_mode: "all",
    output_schema: optimize_schema,
});

let opt_out = #{
    done: false,
    summary: "Optimize agent failed",
    changes: [],
    skipped: [],
    tests_passed: false,
};
if opt != () && opt.success && opt.output != () {
    opt_out = opt.output;
}
let opt_json = json_encode(opt_out);
log("Optimize: changes=" + alen(opt_out.changes).to_string() + " tests_passed=" + as_str(opt_out.tests_passed));

// --- Phase: Verify -----------------------------------------------------------

phase("Verify");
log("Final verification gate");

let ver_prompt = "";
ver_prompt += law;
ver_prompt += "\nTASK: FINAL SHIP GATE. Fail closed. Verify outcomes with tools — do not trust prior agents.\n";
ver_prompt += "Plan path: " + plan_json + "\nWorkplan success criteria: " + json_encode(work.success_criteria) + "\n";
ver_prompt += "Implement: " + impl_json + "\nRemediate: " + rem_json + "\nOptimize: " + opt_json + "\n";
ver_prompt += "Packages: " + pkg_json + "\n\n";
ver_prompt += "Independently run and record checks (execute shell + read code):\n";
ver_prompt += "A) go test " + pkg_focus + " -count=1  (must pass)\n";
ver_prompt += "B) go test " + pkg_focus + " -coverprofile=/tmp/implement-plan-final.out -count=1 && ";
ver_prompt += "go tool cover -func=/tmp/implement-plan-final.out | tail -n 20\n";
ver_prompt += "C) golangci-lint run on touched packages if installed, else go vet " + pkg_focus + "\n";
ver_prompt += "D) Re-read plan success criteria; for each, confirm code+tests prove the outcome (cite evidence)\n";
ver_prompt += "E) Spot-check tests: still outcome-oriented? no new mock theater or 'does not exist' tests?\n";
ver_prompt += "F) Spot-check: no obvious over-engineering reintroduced during optimize\n\n";
ver_prompt += "ship_ready=true ONLY if A–C pass and all in-scope success criteria are evidenced.\n";
ver_prompt += "Populate checks[] with name/pass/detail for each. blockers[] for anything failing.\n";
ver_prompt += "coverage_summary: one short string of overall coverage for focus packages.\n";

let ver = agent(ver_prompt, #{
    label: "verify",
    capability_mode: "execute",
    output_schema: verify_schema,
});

let ship_ready = false;
let ver_summary = "Verify agent failed";
let ver_checks = [];
let ver_blockers = [];
let cover_summary = "";
if ver != () && ver.success && ver.output != () {
    ship_ready = ver.output.ship_ready == true;
    ver_summary = as_str(ver.output.summary);
    if type_of(ver.output.checks) == "array" { ver_checks = ver.output.checks; }
    if type_of(ver.output.blockers) == "array" { ver_blockers = ver.output.blockers; }
    if ver.output.coverage_summary != () { cover_summary = as_str(ver.output.coverage_summary); }
}

// Fail closed if prior critical stages failed even if verify is optimistic
if impl.output.done != true || impl.output.tests_passed != true {
    ship_ready = false;
    ver_blockers.push("Implementation not fully done or tests not green after implement");
}
if has_actionable && (remediate_out.done != true || remediate_out.tests_passed != true) {
    ship_ready = false;
    ver_blockers.push("Remediation incomplete or tests not green");
}
if opt_out.tests_passed != true {
    ship_ready = false;
    ver_blockers.push("Optimize phase left tests failing");
}

// --- Report ------------------------------------------------------------------

let report = "";
report += "# implement-plan report\n\n";
report += "- **plan**: " + as_str(plan_path) + "\n";
report += "- **ship_ready**: " + ship_ready.to_string() + "\n";
report += "- **packages**: " + pkg_focus + "\n";
report += "- **coverage**: " + cover_summary + "\n\n";
report += "## Workplan\n\n" + as_str(work.summary) + "\n\n";
report += "## Implement\n\n" + as_str(impl.output.summary) + "\n\n";
report += "## Audits\n\n";
let ai = 0;
while ai < alen(audit_summaries) {
    report += "- " + audit_summaries[ai] + "\n";
    ai += 1;
}
report += "\n### Findings count: " + alen(all_findings).to_string() + "\n\n";
report += "## Remediate\n\n" + as_str(remediate_out.summary) + "\n\n";
report += "## Optimize\n\n" + as_str(opt_out.summary) + "\n\n";
report += "## Verify\n\n" + ver_summary + "\n\n";
if alen(ver_blockers) > 0 {
    report += "### Blockers\n\n";
    let bi = 0;
    while bi < alen(ver_blockers) {
        report += "- " + as_str(ver_blockers[bi]) + "\n";
        bi += 1;
    }
    report += "\n";
}
if alen(ver_checks) > 0 {
    report += "### Checks\n\n";
    let ci = 0;
    while ci < alen(ver_checks) {
        let c = ver_checks[ci];
        let mark = if c.pass == true { "PASS" } else { "FAIL" };
        report += "- [" + mark + "] " + as_str(c.name) + ": " + as_str(c.detail) + "\n";
        ci += 1;
    }
    report += "\n";
}
report += "## Success criteria (from orient)\n\n";
let si = 0;
while si < alen(work.success_criteria) {
    report += "- " + as_str(work.success_criteria[si]) + "\n";
    si += 1;
}

let report_path = write_scratch_file("implement-plan-report.md", report);
log("Report written: " + report_path + " ship_ready=" + ship_ready.to_string());

complete(#{
    ship_ready: ship_ready,
    summary: ver_summary,
    plan: plan_path,
    packages: pkg_focus,
    coverage_summary: cover_summary,
    implement: impl.output,
    audit_summaries: audit_summaries,
    findings_count: alen(all_findings),
    remediate: remediate_out,
    optimize: opt_out,
    checks: ver_checks,
    blockers: ver_blockers,
    report_path: report_path,
});
