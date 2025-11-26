#!/usr/bin/env bash
set -e

# Colors (optional but helpful)
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
NC="\033[0m"

echo -e "${YELLOW}Waiting for PostgreSQL to become ready...${NC}"
echo "Host:     ${PGHOST}"
echo "Port:     ${PGPORT}"
echo "User:     ${PGUSER}"
echo "Database: ${PGDATABASE}"
echo

# Try until success
until pg_isready -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" >/dev/null 2>&1; do
    echo -e "${YELLOW}Postgres is not ready yet, sleeping...${NC}"
    sleep 2
done

echo -e "${GREEN}PostgreSQL is ready!${NC}"
