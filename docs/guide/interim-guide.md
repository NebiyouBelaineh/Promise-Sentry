# Interim Submission Guide (Thursday 03:00 UTC)

## Data Inventory - What We Have

| Week | Project | Output Location | Format | Records | Match to Canonical |
|------|---------|----------------|--------|---------|-------------------|
| 1 | Roo-Code | `.orchestration/agent_trace.jsonl` | JSONL | 6 | ~90% - needs `description` field, `code_refs` restructuring |
| 2 | LangGraph Auditor | `output/evidence.json` (x9 files) | JSON | ~9 | ~85% - numeric scores, no PASS/FAIL/WARN enum, needs restructuring |
| 3 | PaperMind AI | `.refinery/extractions/` (16 JSON files) + `facts.db` (28,818 rows) | JSON + SQLite | 16 docs / 28K facts | ~60% - different schema (pages/tables/figures), no `extracted_facts` array with confidence in 0-1 format |
| 4 | Brownfield Cartographer | `.cartography/*/lineage_graph.json` | JSON | 5 projects (34-1775 nodes each) | ~70% - has nodes/edges but different field names (`transform_id` vs `node_id`, no `snapshot_id`, no `captured_at`) |
| 5 | Veritas Stream | `data/seed_events.jsonl` | JSONL | 1,198 | ~65% - has `stream_id`/`event_type`/`payload`/`recorded_at` but missing `event_id`, `aggregate_id`, `sequence_number`, `metadata` block |

**Key gaps:**
- Week 3 extractions don't have the canonical `extracted_facts[].confidence` structure - they use pages/tables/figures
- Week 4 lineage graphs exist with real data (best: `ol-data-platform` with 1775 nodes, 2359 edges) but need field mapping
- Week 5 events are close but missing several required fields
- All weeks need **migration scripts** to map to canonical schemas

---

## Interim Deliverables Checklist

### GitHub Repository (6 items)

1. **DOMAIN_NOTES.md** - All 5 Phase 0 questions answered, min 800 words
2. **generated_contracts/week3_extractions.yaml** - Bitol YAML, min 8 clauses
3. **generated_contracts/week5_events.yaml** - Bitol YAML, min 6 clauses + dbt counterparts
4. **contracts/generator.py** - Runnable, evaluators will test it
5. **contracts/runner.py** - Runnable, evaluators will test it
6. **validation_reports/** - At least 1 real validation report
7. **outputs/** - At least 50 records each in `week3/extractions.jsonl` and `week5/events.jsonl`

### PDF Report (4 sections)

1. Data Flow Diagram - 5 systems with annotated schema arrows
2. Contract Coverage Table - every inter-system interface listed
3. First Validation Run Results - real numbers from real data
4. Reflection (max 400 words)

---

## Step-by-Step Implementation Plan

### Step 0: Project Setup & Directory Structure
**Time estimate: ~30 min**

Create the required directory structure:

```
Promise-Sentry/
├── contracts/
│   ├── generator.py
│   ├── runner.py
│   ├── attributor.py          # (final submission)
│   ├── schema_analyzer.py     # (final submission)
│   ├── ai_extensions.py       # (final submission)
│   └── report_generator.py    # (final submission)
├── generated_contracts/
├── validation_reports/
├── violation_log/
├── schema_snapshots/
├── enforcer_report/
├── outputs/
│   ├── week1/intent_records.jsonl
│   ├── week2/verdicts.jsonl
│   ├── week3/extractions.jsonl
│   ├── week4/lineage_snapshots.jsonl
│   ├── week5/events.jsonl
│   └── traces/runs.jsonl
├── outputs/migrate/           # migration scripts
├── DOMAIN_NOTES.md
└── docs/
```

Install dependencies:
```bash
pip install ydata-profiling pandas numpy scikit-learn jsonschema pyyaml gitpython
```

---

### Step 1: Data Migration Scripts
**Time estimate: ~2-3 hours**

We must write migration scripts to convert each week's actual output format into the canonical schemas. These go in `outputs/migrate/`.

#### 1a. Week 3 Migration (PaperMind AI -> extraction_record)

**Source:** `.refinery/extractions/*.json` (16 files) + `facts.db` (28,818 facts)

**Problem:** PaperMind outputs `{doc_id, pages[], tables[], figures[], confidence_score}` per document. The canonical schema expects `{doc_id, extracted_facts[], entities[], extraction_model, ...}`.

**Migration approach:**
- Read each extraction JSON from `.refinery/extractions/`
- Read corresponding facts from `facts.db` (keyed by `doc_id`)
- Map `facts.db` rows (`fact_key`, `fact_value`, `unit`, `page_ref`) to `extracted_facts[]` entries
- Derive `entities[]` from fact values (NER on fact_value text)
- Use the `confidence_score` from `extraction_ledger.jsonl` per doc
- Generate `source_hash` from the source file
- Write to `outputs/week3/extractions.jsonl`

**Target:** 50+ records (16 docs x multiple facts per doc = should exceed 50)

#### 1b. Week 5 Migration (Veritas Stream -> event_record)

**Source:** `data/seed_events.jsonl` (1,198 events)

**Problem:** Events have `{stream_id, event_type, event_version, payload, recorded_at}` but missing `event_id`, `aggregate_id`, `aggregate_type`, `sequence_number`, `metadata`, `schema_version`, `occurred_at`.

**Migration approach:**
- Add `event_id` (generate UUID for each)
- Map `stream_id` to `aggregate_id` (extract ID portion) and `aggregate_type` (extract type prefix)
- Compute `sequence_number` per aggregate (monotonic counter per stream_id)
- Create `metadata` block from payload fields or defaults
- Set `occurred_at` = `recorded_at` (or slightly before)
- Map `event_version` to `schema_version`
- Write to `outputs/week5/events.jsonl`

**Target:** 1,198 records (already exceeds 50)

#### 1c. Week 4 Migration (Brownfield Cartographer -> lineage_snapshot)

**Source:** `.cartography/*/lineage_graph.json` (5 projects)

**Migration approach:**
- Use `ol-data-platform` (largest: 1775 nodes, 2359 edges) as primary
- Map `transform_id` -> `node_id`, `node_type` -> `type`, etc.
- Add `snapshot_id`, `codebase_root`, `git_commit`, `captured_at`
- Map `edge_type` -> `relationship`
- Write to `outputs/week4/lineage_snapshots.jsonl`

#### 1d. Week 1 Migration (Roo-Code -> intent_record)

**Source:** `.orchestration/agent_trace.jsonl` (6 records) + `active_intents.yaml`

**Migration approach:**
- Merge trace records with intent descriptions from YAML
- Map `files[].conversations[].ranges` -> `code_refs[]`
- Add `governance_tags` from `related[].type`
- Write to `outputs/week1/intent_records.jsonl`

#### 1e. Week 2 Migration (LangGraph Auditor -> verdict_record)

**Source:** `output/evidence*.json` (9 files)

**Migration approach:**
- Map `thread_id` -> `verdict_id`
- Map `final_report.criteria[]` -> `scores{}`
- Derive `overall_verdict` (PASS/FAIL/WARN) from `overall_score` thresholds
- Write to `outputs/week2/verdicts.jsonl`

#### 1f. LangSmith Trace Export

**Action:** Export traces from LangSmith to `outputs/traces/runs.jsonl` (50+ traces).

---

### Step 2: DOMAIN_NOTES.md
**Time estimate: ~2 hours**

Answer all 5 Phase 0 questions with concrete examples from your own systems:

1. **Backward vs breaking schema changes** - 3 examples each from your Weeks 1-5 schemas
2. **Confidence scale change trace** - Work through the 0.0-1.0 -> 0-100 failure using your Week 3 PaperMind data and Week 4 Cartographer. Include actual confidence distribution from your data.
3. **Blame chain using lineage graph** - Step-by-step graph traversal logic using your Week 4 cartographer output
4. **LangSmith trace_record contract** - Bitol YAML with structural, statistical, and AI-specific clauses
5. **Contract staleness** - How contracts get stale, how your architecture prevents it

Must include actual script output (e.g., confidence distribution stats from your data).

---

### Step 3: ContractGenerator (`contracts/generator.py`)
**Time estimate: ~3-4 hours**

Build in 4 stages as specified in the Practitioner Manual:

#### Stage 1: Load and profile data
- `load_jsonl(path)` - read JSONL files
- `flatten_for_profile(records)` - flatten nested arrays to DataFrame
- Print `df.describe()` and `df.dtypes`

#### Stage 2: Structural profiling per column
- For each column: name, dtype, null_fraction, cardinality_estimate, sample_values
- For numeric columns: min, max, mean, percentiles, stddev

#### Stage 3: Translate profiles to Bitol YAML clauses
- Apply mapping rules (null_fraction -> required, confidence -> min/max, _id -> uuid, _at -> date-time, etc.)

#### Stage 4: Inject lineage context and write YAML
- Load Week 4 lineage snapshot
- Find downstream consumers for each contract column
- Write contract YAML + dbt schema.yml counterpart
- Write schema snapshot to `schema_snapshots/{contract_id}/{timestamp}.yaml`

**CLI interface:**
```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/
```

**Must generate:**
- `generated_contracts/week3_extractions.yaml` (min 8 clauses)
- `generated_contracts/week3_extractions_dbt.yml`
- `generated_contracts/week5_events.yaml`
- `generated_contracts/week5_events_dbt.yml`

---

### Step 4: ValidationRunner (`contracts/runner.py`)
**Time estimate: ~3-4 hours**

#### Structural checks (implement first):
- Required field present (null_fraction == 0.0)
- Type match (number columns are numeric)
- Enum conformance (values in allowed set)
- UUID pattern validation
- Date-time format validation

#### Statistical checks (implement second):
- Range check (min/max from contract)
- Statistical drift (z-score against baselines)

**CLI interface:**
```bash
python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/week3_baseline.json
```

**Output:** Structured JSON report with `report_id`, `contract_id`, `total_checks`, `passed`, `failed`, `warned`, `errored`, `results[]` - each result with `check_id`, `status`, `severity`, `actual_value`, `expected`, `message`.

After first run, write baselines to `schema_snapshots/baselines.json`.

---

### Step 5: Run Validation & Capture Results
**Time estimate: ~1 hour**

1. Run generator on Week 3 and Week 5 data
2. Run validation on clean data (establish baselines)
3. Capture output to `validation_reports/`
4. Review results - document any real violations found

---

### Step 6: PDF Report Draft (Markdown)
**Time estimate: ~2 hours**

Create a markdown draft with:

1. **Data Flow Diagram** - Draw/describe 5 systems with arrows and schema annotations
2. **Contract Coverage Table** - List every inter-system interface with coverage status
3. **First Validation Run Results** - Paste real numbers from Step 5
4. **Reflection** - What you discovered about your own systems (max 400 words)

---

### Step 7: Final Review & Push
**Time estimate: ~1 hour**

- Verify all files exist in correct locations
- Run generator and runner one more time to confirm they work
- Run through the Thursday checklist
- Push to GitHub
- Convert PDF and upload to Google Drive

---

## Task Assignment

| # | Task | Description | Assignee | Notes |
|---|------|-------------|----------|-------|
| 0 | **Project Setup** | Create directory structure, install deps, `requirements.txt` | Claude 100% | Scaffolding only |
| 1a | **Week 3 Migration Script** | Convert PaperMind extractions + facts.db to canonical `extraction_record` JSONL | Claude 100% | Will write `outputs/migrate/migrate_week3.py` and generate output |
| 1b | **Week 5 Migration Script** | Convert Veritas Stream events to canonical `event_record` JSONL | Claude 100% | Will write `outputs/migrate/migrate_week5.py` and generate output |
| 1c | **Week 4 Migration Script** | Convert Cartographer lineage graphs to canonical `lineage_snapshot` JSONL | Claude 100% | Will write `outputs/migrate/migrate_week4.py` and generate output |
| 1d | **Week 1 Migration Script** | Convert Roo-Code traces to canonical `intent_record` JSONL | Claude 100% | Will write `outputs/migrate/migrate_week1.py` and generate output |
| 1e | **Week 2 Migration Script** | Convert Auditor evidence to canonical `verdict_record` JSONL | Claude 100% | Will write `outputs/migrate/migrate_week2.py` and generate output |
| 1f | **LangSmith Trace Export** | Export traces to `outputs/traces/runs.jsonl` | **You** | Requires your LangSmith credentials and project selection |
| 2 | **DOMAIN_NOTES.md** | Write all 5 Phase 0 answers with examples | Claude ~80% | Claude drafts; **you review** for accuracy against your real experience with these systems |
| 3 | **ContractGenerator** | Build `contracts/generator.py` (4 stages) | Claude 100% | Code implementation |
| 4 | **ValidationRunner** | Build `contracts/runner.py` (structural + statistical checks) | Claude 100% | Code implementation |
| 5 | **Run Validation & Capture** | Execute generator + runner, capture reports | **You** | Must run locally to produce real `validation_reports/` output. Claude provides exact commands. |
| 6a | **PDF: Data Flow Diagram** | Create the diagram of 5 systems | **You** | Claude provides a text description; you draw it (Miro/Excalidraw/hand-drawn) |
| 6b | **PDF: Contract Coverage Table** | Table of all inter-system interfaces | Claude 100% | Claude generates the markdown table |
| 6c | **PDF: Validation Results** | Summarize runner output | Claude ~90% | Claude drafts from validation output; **you verify** numbers match |
| 6d | **PDF: Reflection** | What you discovered about your systems | **You** | Personal reflection - must be authentic |
| 6e | **PDF: Compile & Upload** | Convert markdown to PDF, upload to Google Drive | **You** | Claude provides the markdown draft |
| 7 | **Final Review & Push** | Verify checklist, push to GitHub | **You** | Claude runs through checklist; you push |

### Summary

| Category | Claude 100% | Needs Your Involvement |
|----------|-------------|----------------------|
| Code (scripts, contracts, runner) | 7 tasks | 0 |
| Data (migration, generation) | 5 tasks | 1 (LangSmith export) |
| Documentation | 1 task | 3 (DOMAIN_NOTES review, Reflection, Diagram) |
| Execution & Delivery | 0 | 3 (Run validation, Compile PDF, Push to GitHub) |
| **Total** | **13** | **7** |
