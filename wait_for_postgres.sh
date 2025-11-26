#!/usr/bin/env bash
set -e

# Colors (optional but helpful)
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
NC="\033[0m"

# Timestamp helper
ts() { date "+%Y-%m-%d %H:%M:%S.%3N"; }

echo -e "$(ts) ${YELLOW}Waiting for PostgreSQL to become ready...${NC}"
echo "$(ts) Host:     ${PGHOST}"
echo "$(ts) Port:     ${PGPORT}"
echo "$(ts) User:     ${PGUSER}"
echo "$(ts) Database: ${PGDATABASE}"
echo

# Try until success
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
    echo -e "$(ts) ${YELLOW}Postgres is not ready yet, sleeping...${NC}"
    sleep 2
done

echo -e "$(ts) ${GREEN}PostgreSQL is ready!${NC}"
