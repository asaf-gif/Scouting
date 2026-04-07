# Systematic Problem Scouting

A structured intelligence tool for identifying and evaluating technology-driven disruption opportunities. The system tracks how emerging technologies alter the economic conditions that make current business models viable — and surfaces where those conditions are already shifting.

---

## Table of Contents

1. [What this tool does](#what-this-tool-does)
2. [The logic: how disruption hypotheses are built](#the-logic-how-disruption-hypotheses-are-built)
3. [Data model](#data-model)
4. [Installation](#installation)
5. [Running the app](#running-the-app)
6. [Pages and how to use them](#pages-and-how-to-use-them)
7. [Day-to-day workflow](#day-to-day-workflow)
8. [Editing prompts and logic](#editing-prompts-and-logic)
9. [Architecture and tech stack](#architecture-and-tech-stack)

---

## What this tool does

Most disruption analysis asks the wrong question: *"which companies will this technology kill?"* That question is unanswerable at the frontier. The better question is: *"which structural conditions currently make certain business models work — and is this technology eroding any of them?"*

This tool operationalises that question. It:

- Maintains a taxonomy of **37 business models** with precise descriptions of what makes each one work economically
- Maps **802 transformation vectors** — documented pathways through which one business model evolves into another, each grounded in real case studies
- Tracks **26 scalars** — named structural conditions (e.g. *marginal cost of serving an additional user*, *network density*, *customer switching cost*) that, when they shift past a threshold, make a business model transition viable or necessary
- Monitors **technologies** being fed into the system, and automatically generates **disruption hypotheses** by reasoning through how each technology moves scalars, which vectors that activates, and what transition becomes compelling

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
The system asks: which of the 26 structural scalars does this technology move, and in which direction? For example, GNNs *increase* network density amplification (SCL_B4) and *decrease* the cost of fraud detection (reduces switching cost barriers). Each movement is scored for magnitude and direction.

### Step 3 — Vector activation
A transformation vector (e.g. *E-commerce/Retail → Marketplace/Platform*) has a set of scalars that must shift before the transition becomes viable. If the technology moves enough of those scalars past threshold, the vector is *activated* — the transition is now structurally compelling where it previously was not.

Activation requires:
- **Coverage**: enough of the vector's key scalars are being moved (threshold: 35% minimum)
- **Magnitude**: the movements are material, not marginal

### Step 4 — Hypothesis generation
For each activated vector, the system generates a structured hypothesis containing:
- **Thesis**: the causal argument (2-3 paragraphs)
- **Primary scalar driver**: the single most important scalar being moved
- **Supporting scalars**: secondary structural shifts reinforcing the thesis
- **Counter-argument**: the strongest case against the hypothesis
- **Time horizon**: 1-2 years / 2-5 years / 5+ years
- **Conviction score**: 0–1, based on evidence quality and scalar coverage
- **Companies exposed**: real companies currently in the disrupted business model

### Step 5 — Human review
Hypotheses land in the review queue. The team approves, rejects, or flags for more research. Approved hypotheses persist in the graph. Rejected ones are archived but kept for reference.

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
| `TransformationVector` | A documented pathway from one BM to another, with case study example and signal score | 802 |
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

## Installation

### Prerequisites
- **Python 3.11+** — check with `python3 --version`
- **Git** — check with `git --version`
- **Google Drive desktop app** — [download here](https://drive.google.com/drive/download) (for shared logs)

### 1 — Clone the repository

```bash
git clone https://github.com/asaf-gif/Scouting.git
cd Scouting
```

### 2 — Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate        # Mac / Linux
# venv\Scripts\activate         # Windows
```

### 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### 4 — Configure credentials

```bash
cp .env.example .env
```

Open `.env` and fill in the values provided by the team lead:

```bash
# Database (Neo4j AuraDB — shared by the whole team)
NEO4J_URI=neo4j+s://000bc885.databases.neo4j.io
NEO4J_USER=000bc885
NEO4J_PASSWORD=<provided by team lead>

# AI APIs
ANTHROPIC_API_KEY=<provided by team lead>
TAVILY_API_KEY=<provided by team lead>

# Shared logs folder — point this to your locally-synced Google Drive folder
# To find your exact path, run: ls ~/Library/CloudStorage/
SHARED_DATA_PATH=/Users/YOURNAME/Library/CloudStorage/GoogleDrive-YOUR@EMAIL.com/My Drive/scouting-data
```

**Finding your Google Drive path on Mac:**
```bash
ls ~/Library/CloudStorage/
# Will show something like: GoogleDrive-you@gmail.com
# Your full path is then:
# /Users/YOURNAME/Library/CloudStorage/GoogleDrive-you@gmail.com/My Drive/scouting-data
```

### 5 — Verify the connection

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

The app opens at **http://localhost:8501** in your browser. Each team member runs their own local instance — they all connect to the same Neo4j database and the same Google Drive log folder.

To stop: `Ctrl+C` in the terminal.

---

## Pages and how to use them

### 📚 BM Library
The full taxonomy of 37 business models. Each entry includes:
- Revenue logic (how money is made, what the unit of value is)
- Key dependencies (what must be true for the model to work)
- Typical margins
- Scalars most affected
- Real company examples

**Use this when** you want to understand what a business model actually is before evaluating whether a technology disrupts it. The precision matters — "marketplace" and "e-commerce/retail" are different models with different vulnerabilities.

Click any scalar or transformation link to jump directly to that node.

---

### 🔀 Transition Case Studies
Documented real-world examples of companies that have made a business model transition. Each case study is attached to a transformation vector and explains:
- What triggered the transition
- When to make the move
- What the critical risk is

**Use this when** you want grounding for whether a theoretical transition has actually happened in practice.

---

### 📐 Transformations
The 802 transformation vectors — every documented pathway from one business model to another. Each vector has:
- A case study example
- Signal strength (how actively the market is moving along this vector right now)
- The scalars that drive it

Sort by signal strength to see which transitions are most active. Vectors with signal > 0.6 are worth paying close attention to.

---

### ⚡ Scalars
The 26 structural conditions. Each scalar shows:
- Which business models it affects most
- Which transformation vectors it's a primary driver of
- Direction of movement (increases / decreases / threshold)

**Use this when** you're trying to understand *why* a hypothesis was generated, or when you want to find all the hypotheses driven by a particular structural shift.

---

### 🔬 Technologies
Technologies currently being tracked. Each technology page shows:
- Enriched description and capabilities
- Which scalars it moves and in which direction
- All activated transformation vectors
- All generated hypotheses

To add a new technology: click **Add Technology**, enter a name and description, and the system will automatically enrich it, classify its scalar impacts, and generate hypotheses.

---

### 🏢 Companies
Companies in the database, each tagged with their current business model. The system uses these to populate the *companies exposed* field in hypotheses — so when a hypothesis says "these companies are at risk," you can click through to see them.

---

### 🧠 Hypotheses
The main working surface. All generated disruption hypotheses, filterable by technology, business model, status, and conviction score.

Each hypothesis card shows:
- The full causal chain (Technology → Scalars → Transition)
- Thesis and counter-argument
- Conviction and activation scores
- Companies currently in the disrupted model

**Actions:**
- **Approve** — marks the hypothesis as validated, keeps it in the active set
- **Reject** — archives it (still accessible in Notebook)
- **🤖 Discuss with Agent** — opens the AI agent pre-loaded with this hypothesis for deeper discussion
- **📓 Research Notes** — attach ideas, writeups, or observations directly to this hypothesis

---

### 📝 Editorial
Full visibility and control over the system's AI prompts and scoring logic — without touching code.

**Prompts tab**: All 10 AI prompts used at each pipeline stage. Click any prompt to read the full text, see its last 5 edits, and edit it with a required rationale. Changes are logged with timestamp and source.

**Logic & Thresholds tab**: All 12 scoring constants (activation threshold, signal weights, similarity thresholds, etc.) with descriptions of what they do and which formula they appear in. Edit any value with a rationale — the change takes effect immediately.

**Change History tab**: Full audit log of every prompt and logic change, showing whether it was made through this UI or directly in the code (drift detection runs automatically on startup).

---

### 🤖 Agent
A conversational AI that knows the full system — the business model taxonomy, how hypotheses are built, what the scalars mean, and the current state of the graph.

**What it can do:**
- Explain any hypothesis in plain language
- Look up specific hypotheses, scalars, or transformation vectors
- Discuss the strength of a thesis and probe its weaknesses
- Read and update prompts and logic constants (with your confirmation)
- Search across all hypotheses for patterns

**Quick starters** (sidebar): pre-built prompts for common questions like "what are the strongest hypotheses right now?" or "explain how the signal score is calculated."

**Save to Notebook**: any agent conversation can be saved as a research note attached to a hypothesis.

---

### 📓 Notebook
A persistent research layer attached to each hypothesis. Notes survive even if a hypothesis is rejected. Types of notes:
- 💡 Idea
- 📝 Writeup
- 🤖 Agent conversation (auto-saved from the Agent page)
- 👁 Observation
- ❓ Question

**Cross-linking**: if two hypotheses share the same transformation vector, the Notebook shows you relevant notes from both — so prior thinking on a transformation isn't lost when it surfaces in a new hypothesis.

---

### 📊 Graph Overview
Live counts of every node and relationship type in the database. A quick health check.

---

### 🔄 Pipeline Monitor
Shows the status of the last pipeline run — which stages completed, any errors, processing times.

---

## Day-to-day workflow

### Adding a new technology to evaluate
1. Go to **🔬 Technologies** → **Add Technology**
2. Enter the name and a clear description (2-3 paragraphs, explain what it does mechanically)
3. Click **Enrich & Analyse** — the pipeline runs automatically:
   - Enriches the technology with web research
   - Classifies which scalars it moves
   - Activates relevant transformation vectors
   - Generates hypotheses for all activated vectors
4. New hypotheses appear in **🧠 Hypotheses** with status *Pending Review*

### Reviewing hypotheses
1. Open **🧠 Hypotheses**
2. Read the thesis and counter-argument carefully
3. Check the scalar reasoning — does the causal chain hold?
4. Use **🤖 Discuss with Agent** if you want to pressure-test the logic
5. Approve or reject with a note explaining your reasoning

### Adding research notes
- From any hypothesis card: click **📓 Research Notes** → **Add Note**
- From the Agent page: after a useful conversation, click **Save conversation to Notebook**
- From the Notebook page directly: write standalone notes attached to any hypothesis

### Keeping code in sync
When someone edits a prompt or logic constant through the Editorial UI, the change is saved to the file in the repository. That person should commit and push:
```bash
git add prompts/ config/
git commit -m "Update hypothesis_generation prompt — tighten scalar reasoning requirement"
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
| `tech_enrichment.txt` | Input | Enriches a technology description with capabilities and context |
| `company_enrichment.txt` | Input | Enriches a company with industry, BM, and risk profile |
| `vector_extraction.txt` | Extraction | Extracts transformation vectors from case study text |
| `scalar_classification.txt` | Extraction | Classifies which scalars a technology moves and how |
| `tech_scalar_classification.txt` | Extraction | Deeper scalar-to-technology linkage |
| `hypothesis_generation.txt` | Analysis | Writes the full disruption hypothesis thesis |
| `deep_research.txt` | Research | Gathers supporting evidence for a hypothesis |
| `counter_research.txt` | Research | Generates the strongest counter-argument |

All of these are editable in **📝 Editorial → Prompts** without touching any code. Every edit is logged with your rationale and is reversible.

The scoring constants (weights, thresholds) are similarly editable in **📝 Editorial → Logic & Thresholds**.

---

## Architecture and tech stack

```
┌─────────────────────────────────────────────────────────┐
│                     Streamlit UI                        │
│  ui/app.py  (single-file, ~3800 lines)                  │
└───────────────┬────────────────┬───────────────────────┘
                │                │
    ┌───────────▼──────┐  ┌──────▼──────────────┐
    │   core/          │  │   Pipeline modules   │
    │  editorial.py    │  │                      │
    │  agent.py        │  │  input_layer/        │
    │  notebook.py     │  │  extraction/         │
    └───────────┬──────┘  │  analysis/           │
                │          │  orchestrator/       │
    ┌───────────▼──────────▼────────────┐
    │         Neo4j AuraDB              │
    │   (cloud-hosted, shared by team)  │
    └───────────────────────────────────┘

    External APIs:
    - Anthropic Claude (claude-sonnet-4-5) — hypothesis generation, agent, enrichment
    - Tavily — web search for evidence gathering
```

**Key files:**

| File | Purpose |
|---|---|
| `ui/app.py` | Entire Streamlit frontend |
| `core/editorial.py` | Prompt and logic config read/write, changelog, drift detection |
| `core/agent.py` | Conversational agent with 8 tools for querying and editing the system |
| `core/notebook.py` | Research notes CRUD against Neo4j |
| `config/logic_config.json` | All scoring thresholds and formula weights |
| `prompts/*.txt` | All 10 AI prompts, plain text, editable |
| `extraction/vector_activator.py` | Determines which transformation vectors a technology activates |
| `extraction/scalar_classifier.py` | Maps technology capabilities to scalar movements |
| `analysis/signal_aggregator.py` | Computes composite signal strength for each vector |
| `analysis/hypothesis_generator.py` | Writes hypotheses via Claude |
| `orchestrator/pipeline.py` | Coordinates the full enrichment pipeline |

**Shared state:**
- **Graph data** (hypotheses, BMs, companies, scalars) → Neo4j AuraDB, shared by all users in real time
- **Changelogs and audit logs** → Google Drive synced folder (`SHARED_DATA_PATH`), shared by all users
- **Code, prompts, and config** → GitHub, updated via `git push`/`git pull`
- **Credentials** → `.env` file on each person's machine, never committed to git
