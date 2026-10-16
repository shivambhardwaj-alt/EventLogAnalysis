# Event Funnel Analytics

A data engineering pipeline built with **Python + SQL**. It takes raw app event logs, cleans them, groups them into user sessions and measures the checkout funnel. It also catches problems automatically and explains the biggest drop.

No libraries to install: only Python 3.11 or newer.

## What it does

1. **Ingests** event log files (JSONL and CSV). Each file is loaded only once.
2. **Cleans** the data: fixes types and timestamps, and moves bad rows to a quarantine table with a reason.
3. **Removes duplicates**: repeated events and double clicks.
4. **Builds sessions**: a new session starts after 30 minutes of inactivity.
5. **Calculates the funnel**: view product → add to cart → checkout → payment → purchase.
6. **Finds problems**: the biggest drop, the user segment causing it, daily anomalies and unexpected new fields.
7. **Explains** the biggest drop with AI (optional) and sends alerts.
8. **Shows results** in an HTML report and a live dashboard.

## How to run


python run_all.py         
python -m funnel serve     


Then open **http://127.0.0.1:8050** in your browser.

## Optional: AI explanations

1. Copy `.env.example` to `.env`.
2. Add a free key from https://console.groq.com/keys:
```
   GROQ_API_KEY=your_key_here
```

Without a key, the project still works and uses a built-in offline explanation.

## Useful commands

| Command | What it does |
|---|---|
| `python run_all.py` | Run the full demo |
| `python -m funnel serve` | Start the dashboard |
| `python -m funnel run` | Process new files |
| `python -m funnel status` | Show table counts |
| `python -m funnel demo-edge-cases` | Show how bad data is handled |
| `python -m unittest discover -s tests -t .` | Run the tests |

## Project structure

```
event-funnel-analytics/
├── run_all.py          # one command to run everything
├── config/             # settings
├── funnel/             # Python code
│   ├── sql/            # SQL transformations
│   ├── ai/             # AI explanation
│   └── web/            # dashboard page
├── tests/              # tests
└── output/             # database, report and CSV files (created when you run)
```

## Tech used

Python, SQL (SQLite), HTML/JavaScript for the dashboard, and an optional LLM (Groq or Ollama).

All data is synthetic (generated), so no real user data is used.