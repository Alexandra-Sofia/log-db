# ===============================
# LogDB Ingestion Makefile
# ===============================

PYTHON := python3
LOGDIR := /input-logfiles
PARSED := ./parsed

# Default target
all: parse load

# --------------------------------
# 1. Parse raw log files → CSVs
# --------------------------------
parse:
	@echo "== Parsing logs =="
	$(PYTHON) parse_logs.py --logdir $(LOGDIR) --outdir $(PARSED)

# --------------------------------
# 2. Load CSVs into PostgreSQL
# --------------------------------
load:
	@echo "== Loading into PostgreSQL =="
	$(PYTHON) load_into_db.py

# --------------------------------
# 3. Clean generated CSVs
# --------------------------------
clean:
	@echo "== Cleaning output directory =="
	rm -f $(PARSED)/*.csv

# --------------------------------
# 4. Full reset: clean + parse + load
# --------------------------------
reset: clean parse load

# --------------------------------
# Utility
# --------------------------------
show:
	@echo "Logdir: $(LOGDIR)"
	@echo "Outdir: $(PARSED)"
