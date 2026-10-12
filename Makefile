PY ?= python3

.PHONY: all demo-run serve data run rerun full test demo status benchmark clean setup

all:                           ## the whole story in one command (clean start -> 7 daily loads -> tests)
	$(PY) run_all.py

serve:                         ## live dashboard on http://127.0.0.1:8050
	$(PY) -m funnel serve

setup:                         ## optional: venv + pytest (the pipeline itself needs no installs)
	$(PY) -m venv .venv && .venv/bin/pip install -q pytest
	@test -f .env || cp .env.example .env

data:                          ## generate 7 days of synthetic logs straight into data/incoming
	$(PY) -m funnel generate --clean

run:                           ## incremental run: ingest new files, refresh, alert, report
	$(PY) -m funnel run

rerun: run                     ## run again: proves idempotency (status=no_new_data)

full:                          ## rebuild every derived table from the raw layer
	$(PY) -m funnel run --full-refresh

test:
	@if $(PY) -c "import pytest" 2>/dev/null; then $(PY) -m pytest; \
	else $(PY) -m unittest discover -s tests -t . ; fi

demo:                          ## failure / edge-case demonstration
	$(PY) -m funnel demo-edge-cases

status:
	$(PY) -m funnel status

benchmark:
	$(PY) -m funnel benchmark --users 20000

clean:
	$(PY) -m funnel reset --yes
	rm -rf data/incoming data/landing data/generator_truth.json
