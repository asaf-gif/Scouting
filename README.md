# Systematic Problem Scouting — Installation Guide

---

## Prerequisites
- **Python 3.11+** — check with `python3 --version`
- **Git** — check with `git --version`
- **Google Drive desktop app** — [download here](https://drive.google.com/drive/download)

---

## Step 1 — Install Google Drive desktop app

Download and install the [Google Drive desktop app](https://drive.google.com/drive/download). Sign in with your Google account. Wait for it to finish syncing before continuing.

---

## Step 2 — Connect to the shared Drive folder

The team shares a Google Drive folder called **`scouting-data`** that holds all the shared logs.

1. Open [drive.google.com](https://drive.google.com)
2. In **Shared with me**, find the **scouting-data** folder shared by the team lead
3. Right-click it → **Add shortcut to Drive** → place it in **My Drive**
4. It will now sync automatically via the desktop app

**Find the local path on your machine** — you'll need this in Step 5:
```bash
ls ~/Library/CloudStorage/
# Shows something like: GoogleDrive-you@gmail.com
```

Your full path will be (replace `YOURNAME`):
```bash
/Users/YOURNAME/Library/CloudStorage/GoogleDrive-asaf@aleph.vc/My Drive/scouting-data
```

Confirm it works:
```bash
ls "/Users/YOURNAME/Library/CloudStorage/GoogleDrive-asaf@aleph.vc/My Drive/scouting-data"
# Should list: bm_changelog.jsonl  editorial_changelog.jsonl  error_log.jsonl  ...
```

---

## Step 3 — Clone the repository

```bash
git clone https://github.com/asaf-gif/Scouting.git
cd Scouting
```

---

## Step 4 — Create a virtual environment and install dependencies

```bash
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

---

## Step 5 — Set up your credentials

A pre-filled credentials file called **`.env.team`** is inside the shared Google Drive folder. It has all the shared API keys already filled in — you only need to update one line.

```bash
# Copy it from Drive into the project root:
cp "/Users/YOURNAME/Library/CloudStorage/GoogleDrive-asaf@aleph.vc/My Drive/scouting-data/.env.team" .env
```

Open `.env` in any text editor and update the last line with your Mac username:

```bash
# Change this:
SHARED_DATA_PATH=/Users/YOURNAME/Library/CloudStorage/GoogleDrive-asaf@aleph.vc/My Drive/scouting-data

# Example:
SHARED_DATA_PATH=/Users/alice/Library/CloudStorage/GoogleDrive-asaf@aleph.vc/My Drive/scouting-data
```

Do not change any other values in the file.

---

## Step 6 — Verify the connection

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

## Step 7 — Run the app

```bash
source venv/bin/activate
./start.sh
```

Opens at **http://localhost:8501**.

To stop: `Ctrl+C` in the terminal.

**Before each session, pull the latest changes:**
```bash
git pull
```
