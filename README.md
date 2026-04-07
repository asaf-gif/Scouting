# Systematic Problem Scouting

A structured intelligence tool for identifying and evaluating technology-driven disruption opportunities. The system tracks how emerging technologies alter the economic conditions that make current business models viable — and surfaces where those conditions are already shifting.

---

## Table of Contents

1. [Installation](#installation)
2. [Running the app](#running-the-app)
3. [What this tool does](#what-this-tool-does)
4. [The logic: how disruption hypotheses are built](#the-logic-how-disruption-hypotheses-are-built)
5. [Data model](#data-model)
6. [Pages and how to use them](#pages-and-how-to-use-them)
7. [Day-to-day workflow](#day-to-day-workflow)
8. [Editing prompts and logic](#editing-prompts-and-logic)
9. [Architecture and tech stack](#architecture-and-tech-stack)

---

## Installation

### Prerequisites
- **Python 3.11+** — check with `python3 --version`
- **Git** — check with `git --version`
- **Google Drive desktop app** — [download here](https://drive.google.com/drive/download)

---

### Step 1 — Install Google Drive desktop app

Download and install the [Google Drive desktop app](https://drive.google.com/drive/download). Sign in with your Google account. Wait for it to finish syncing before continuing.

---

### Step 2 — Connect to the shared Drive folder

The team shares a Google Drive folder called **`scouting-data`** that holds all the audit logs. You need to add it to your Drive and confirm it syncs locally.

**If using a Shared Drive (Google Workspace / company account):**
1. Open [drive.google.com](https://drive.google.com)
2. In the left sidebar, click **Shared drives**
3. You should see **scouting-data** — double-click it to open it
4. It will sync automatically via the desktop app

**If using My Drive (personal Gmail):**
1. Open [drive.google.com](https://drive.google.com)
2. In **Shared with me**, find the **scouting-data** folder shared by the team lead
3. Right-click it → **Add shortcut to Drive** → place it in **My Drive**
4. It will now sync via the desktop app

**Find the local path on your machine** — you'll need this in the next step:
```bash
ls ~/Library/CloudStorage/
# Shows something like: GoogleDrive-you@gmail.com
```

Your full path will be one of these (replace `YOURNAME` and `YOUR@EMAIL`):
```bash
# Shared Drive (Google Workspace):
/Users/YOURNAME/Library/CloudStorage/GoogleDrive-YOUR@EMAIL/Shared drives/scouting-data

# My Drive (personal Gmail):
/Users/YOURNAME/Library/CloudStorage/GoogleDrive-YOUR@EMAIL/My Drive/scouting-data
```

Confirm it works:
```bash
ls "/Users/YOURNAME/Library/CloudStorage/GoogleDrive-YOUR@EMAIL/Shared drives/scouting-data"
# Should list: bm_changelog.jsonl  editorial_changelog.jsonl  error_log.jsonl  ...
```

---

### Step 3 — Clone the repository

```bash
git clone https://github.com/asaf-gif/Scouting.git
cd Scouting
```

---

### Step 4 — Create a virtual environment and install dependencies

```bash
python3 -m venv venv
source venv/bin/activate        # Mac / Linux
# venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

---

### Step 5 — Set up your credentials

A pre-filled credentials file called **`.env.team`** is waiting for you inside the shared Google Drive folder (`scouting-data/`). It has all the shared API keys already filled in — you only need to update one line.

```bash
# Copy it from Drive into the project root:
cp "/Users/YOURNAME/Library/CloudStorage/GoogleDrive-YOUR@EMAIL/Shared drives/scouting-data/.env.team" .env
```

Then open `.env` in any text editor and update the last line to match your machine:

```bash
# Change this line:
SHARED_DATA_PATH=/Users/YOURNAME/Library/CloudStorage/GoogleDrive-YOUR@EMAIL/Shared drives/scouting-data

# Example (replace with your actual username and email):
SHARED_DATA_PATH=/Users/alice/Library/CloudStorage/GoogleDrive-alice@company.com/Shared drives/scouting-data
```

Everything else in the file is already correct — do not change the other values.

---

### Step 6 — Verify the connection

```bash
source venv/bin/activate
python3 -c "
import os
from dotenv import load_dotenv
load_dotenv()
from neo4j import GraphDatabase
d = GraphDatabase.driver(os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASSWORD')))
with d.session() as s:
    r = s.run('MATCH (b:BusinessModel) RETURN count(b) AS n').data()
    print(f'Connected. {r[0][\"n\"]} business models in database.')
d.close()
"
```

Expected output: `Connected. 37 business models in database.`

---

## Running the app

```bash
source venv/bin/activate
./start.sh
```

Opens at **http://localhost:8501**. Each team member runs their own local instance — everyone connects to the same shared Neo4j database and reads/writes to the same Google Drive log folder.

To stop: `Ctrl+C` in the terminal.

**To get the latest code changes** (run this before starting each session):
```bash
git pull
```

---

## What this tool does

Most disruption analysis asks the wrong question: *"which companies will this technology kill?"* That question is unanswerable at the frontier. The better question is: *"which structural conditions currently make certain business models work — and is this technology eroding any of them?"*

This tool operationalises that question. It:

- Maintains a taxonomy of **37 business models** with precise descriptions of what makes each one work economically
- Maps **802 transformation vectors** — documented pathways through which one business model evolves into another, each grounded in real case studies
- Tracks **26 scalars** — named structural conditions (e.g. *marginal cost of serving an additional user*, *network density*, *customer switching cost*) that, when they shift past a threshold, make a business model transition viable or necessary
- Monitors **technologies** and automatically generates **disruption hypotheses** by reasoning through how each technology moves scalars, which vectors that activates, and what transition becomes compelling

The output is not a list of companies to watch. It is a set of falsifiable, causally-grounded hypotheses: *"Technology X is moving scalar Y past threshold Z, which makes transformation from Business Model A to Business Model B structurally attractive — here is the evidence."*

---

## The logic: how disruption hypotheses are built

Every hypothesis follows the same causal chain:

```
Technology  →  Scalars  →  Transformation Vector  →  Disruption Hypothesis
```

### Step 1 — Technology intake
A technology (e.g. *Graph Neural Networks*, *Synthetic Audiences*) is submitted with a description. The system enriches it: what does it do, which industries does it affect, which companies are building it.

### Step 2 — Scalar classification
The system asks: which of the 26 structural scalars does this technology move, and in which direction? For example, GNNs *increase* network density amplification (SCL_B4) and *decrease* the cost of fraud detection. Each movement is scored for magnitude and direction.

### Step 3 — Vector activation
A transformation vector (e.g. *E-commerce/Retail → Marketplace/Platform*) has a set of scalars that must shift before the transition becomes viable. If the technology moves enough of those scalars past threshold, the vector is *activated* — the transition is now structurally compelling where it previously was not.

Activation requires:
- **Coverage**: enough of the vector's key scalars are being moved (threshold: 35% minimum)
- **Magnitude**: the movements are material, not marginal

### Step 4 — Hypothesis generation
For each activated vector, the system generates a structured hypothesis containing:
- **Thesis**: the causal argument (2–3 paragraphs)
- **Primary scalar driver**: the single most important scalar being moved
- **Supporting scalars**: secondary structural shifts reinforcing the thesis
- **Counter-argument**: the strongest case against the hypothesis
- **Time horizon**: 1–2 years / 2–5 years / 5+ years
- **Conviction score**: 0–1, based on evidence quality and scalar coverage
- **Companies exposed**: real companies currently in the disrupted business model

### Step 5 — Human review
Hypotheses land in the review queue. The team approves, rejects, or flags for more research. Approved hypotheses persist in the graph. Rejected ones are archived but kept in the Notebook.

### Scoring formula

Signal strength for each hypothesis is computed as:

```
signal = (evidence_weight × evidence_score)
       + (scalar_coverage_weight × scalar_coverage)
       + (scalar_magnitude_weight × scalar_magnitude)
       + (conviction_weight × conviction_score)
```

Default weights: evidence 40%, scalar coverage 30%, scalar magnitude 20%, conviction 10%. All weights and thresholds are editable in the Editorial page without touching code.

---

## Data model

The system is a **property graph** stored in Neo4j. Key node types:

| Node | What it represents | Count |
|---|---|---|
| `BusinessModel` | A named economic model with revenue logic, dependencies, and examples | 37 |
| `TransformationVector` | A documented pathway from one BM to another, with case study and signal score | 802 |
| `Scalar` | A named structural condition that can be measured and tracked over time | 26 |
| `Technology` | A technology being evaluated for disruptive potential | 3 (growing) |
| `DisruptionHypothesis` | A generated hypothesis linking tech → scalars → vector → disruption | 24 |
| `Company` | A real company, associated with the business model it currently operates | 54 |
| `ResearchNote` | A team member's note, idea, or conversation attached to a hypothesis | — |

Key relationships:

```
(Technology)-[:ACTIVATES]->(TransformationVector)
(TransformationVector)-[:MOVES_SCALAR]->(Scalar)
(DisruptionHypothesis)-[:TARGETS]->(BusinessModel)   ← the disrupted BM
(DisruptionHypothesis)-[:PROPOSES]->(BusinessModel)  ← the proposed new BM
(Company)-[:OPERATES_AS]->(BusinessModel)
(ResearchNote)-[:ATTACHED_TO]->(DisruptionHypothesis)
```

---

## Pages and how to use them

### 📚 BM Library
The full taxonomy of 37 business models. Each entry includes revenue logic, key dependencies, typical margins, scalars most affected, and real company examples.

**Use this when** you want to understand what a business model actually is before evaluating whether a technology disrupts it. The precision matters — "marketplace" and "e-commerce/retail" are different models with different vulnerabilities.

---

### 🔀 Transition Case Studies
Documented real-world examples of companies that have made a business model transition. Each case study explains what triggered the transition, when to make the move, and what the critical risk is.

**Use this when** you want grounding for whether a theoretical transition has actually happened in practice.

---

### 📐 Transformations
The 802 transformation vectors — every documented pathway from one business model to another. Each vector has a case study example, signal strength score, and the scalars that drive it.

Sort by signal strength to see which transitions are most active. Vectors with signal > 0.6 are worth close attention.

---

### ⚡ Scalars
The 26 structural conditions. Each scalar shows which business models it affects, which vectors it drives, and its direction of movement.

**Use this when** you want to understand *why* a hypothesis was generated, or find all hypotheses driven by a particular structural shift.

---

### 🔬 Technologies
Technologies currently being tracked. Each page shows the enriched description, which scalars it moves, all activated vectors, and all generated hypotheses.

To add a new technology: click **Add Technology**, enter a name and description, and the pipeline runs automatically.

---

### 🏢 Companies
Companies in the database, each tagged with their current business model. Used to populate the *companies exposed* field in hypotheses.

---

### 🧠 Hypotheses
The main working surface. All generated disruption hypotheses, filterable by technology, business model, status, and conviction score.

Each hypothesis card shows the full causal chain, thesis and counter-argument, conviction and activation scores, and exposed companies.

**Actions:**
- **Approve** — marks the hypothesis as validated
- **Reject** — archives it (still accessible in Notebook)
- **🤖 Discuss with Agent** — opens the AI agent pre-loaded with this hypothesis
- **📓 Research Notes** — attach ideas, writeups, or observations

---

### 📝 Editorial
Full visibility and control over the system's AI prompts and scoring logic — without touching code.

- **Prompts tab**: All 10 AI prompts. Read, edit, and track changes with required rationale.
- **Logic & Thresholds tab**: All 12 scoring constants. Edit any value; change takes effect immediately.
- **Change History tab**: Full audit log showing whether each change was made via UI or directly in code.

---

### 🤖 Agent
A conversational AI that knows the full system — the business model taxonomy, how hypotheses are built, what the scalars mean, and the current state of the graph.

It can explain hypotheses, probe weaknesses in the thesis, look up graph data, and update prompts and logic constants with your confirmation. Any conversation can be saved directly to the Notebook.

---

### 📓 Notebook
Persistent research notes attached to each hypothesis. Notes survive even if a hypothesis is rejected. Types: 💡 Idea, 📝 Writeup, 🤖 Agent conversation, 👁 Observation, ❓ Question.

If two hypotheses share the same transformation vector, the Notebook surfaces relevant notes from both — so prior thinking on a transformation is never lost.

---

### 📊 Graph Overview
Live node and relationship counts. Quick database health check.

---

### 🔄 Pipeline Monitor
Status of the last pipeline run — stages completed, errors, processing times.

---

## Day-to-day workflow

### Adding a new technology
1. Go to **🔬 Technologies** → **Add Technology**
2. Enter the name and a clear description (2–3 paragraphs explaining what it does mechanically)
3. Click **Enrich & Analyse** — the pipeline enriches, classifies scalars, activates vectors, generates hypotheses
4. New hypotheses appear in **🧠 Hypotheses** with status *Pending Review*

### Reviewing hypotheses
1. Open **🧠 Hypotheses**
2. Read the thesis and counter-argument
3. Check the scalar reasoning — does the causal chain hold?
4. Use **🤖 Discuss with Agent** to pressure-test the logic
5. Approve or reject with a note

### Adding research notes
- From any hypothesis card: **📓 Research Notes** → **Add Note**
- From the Agent page: after a useful conversation, click **Save conversation to Notebook**
- From the Notebook page directly

### Keeping code in sync
When someone edits a prompt or logic constant through the Editorial UI, the change is saved to a file in the repository. That person should commit and push:
```bash
git add prompts/ config/
git commit -m "Update hypothesis_generation prompt — tighten scalar reasoning"
git push
```

Others pull to get the update:
```bash
git pull
```

---

## Editing prompts and logic

The system uses 10 AI prompts at different pipeline stages:

| Prompt | Stage | What it does |
|---|---|---|
| `bm_scanner.txt` | Input | Classifies a raw input into a business model |
| `bm_enrichment.txt` | Input | Enriches a business model with description and examples |
| `tech_enrichment.txt` | Input | Enriches a technology description |
| `company_enrichment.txt` | Input | Enriches a company with industry and risk profile |
| `vector_extraction.txt` | Extraction | Extracts transformation vectors from case study text |
| `scalar_classification.txt` | Extraction | Classifies which scalars a technology moves |
| `tech_scalar_classification.txt` | Extraction | Deeper scalar-to-technology linkage |
| `hypothesis_generation.txt` | Analysis | Writes the full disruption hypothesis thesis |
| `deep_research.txt` | Research | Gathers supporting evidence |
| `counter_research.txt` | Research | Generates the strongest counter-argument |

All editable in **📝 Editorial → Prompts** without touching code. Every edit is logged with rationale.

---

## Architecture and tech stack

```
┌─────────────────────────────────────────────────────────┐
│                     Streamlit UI                        │
│              ui/app.py  (~3800 lines)                   │
└───────────────┬────────────────┬───────────────────────┘
                │                │
    ┌───────────▼──────┐  ┌──────▼──────────────┐
    │   core/          │  │   Pipeline modules   │
    │  editorial.py    │  │  input_layer/        │
    │  agent.py        │  │  extraction/         │
    │  notebook.py     │  │  analysis/           │
    └───────────┬──────┘  │  orchestrator/       │
                │          └──────────────────────┘
    ┌───────────▼──────────────────────────────┐
    │              Neo4j AuraDB                │
    │      (cloud-hosted, shared by team)      │
    └──────────────────────────────────────────┘

    External APIs:
    • Anthropic Claude (claude-sonnet-4-5) — hypothesis generation, agent, enrichment
    • Tavily — web search for evidence gathering
```

**Where things live:**

| What | Where | How it's shared |
|---|---|---|
| Graph data (hypotheses, BMs, companies) | Neo4j AuraDB | Live, real-time for everyone |
| Audit logs and changelogs | Google Drive `scouting-data/` | Synced automatically |
| Code, prompts, config | GitHub | `git pull` to update |
| API credentials | `.env` on each machine | Via `.env.team` in Drive folder |

**Key files:**

| File | Purpose |
|---|---|
| `ui/app.py` | Entire Streamlit frontend |
| `core/editorial.py` | Prompt and logic config R/W, changelog, drift detection |
| `core/agent.py` | Conversational agent with 8 tools |
| `core/notebook.py` | Research notes CRUD against Neo4j |
| `config/logic_config.json` | All scoring thresholds and formula weights |
| `prompts/*.txt` | All 10 AI prompts, plain text, editable |
| `extraction/vector_activator.py` | Determines which vectors a technology activates |
| `extraction/scalar_classifier.py` | Maps technology capabilities to scalar movements |
| `analysis/signal_aggregator.py` | Computes composite signal strength |
| `analysis/hypothesis_generator.py` | Writes hypotheses via Claude |
| `orchestrator/pipeline.py` | Coordinates the full enrichment pipeline |
