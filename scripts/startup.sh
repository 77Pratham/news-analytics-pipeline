#!/usr/bin/env bash
# One-shot startup for the native (non-Docker) news analytics pipeline.
# Run this from inside WSL2 (Ubuntu-24.04) each time you reopen the project
# after a reboot or laptop shutdown. It brings up Postgres, the FastAPI
# backend, the Streamlit dashboard, and Airflow. Ollama runs on Windows and
# is checked but not auto-started (it's a separate GUI/service on the
# Windows side, not something WSL2 should launch).
#
# Usage:  bash scripts/startup.sh
# Stop the background API/dashboard later with:
#   pkill -f "uvicorn api.main"
#   pkill -f "streamlit run dashboard/app.py"

set -e

PROJECT_ROOT="/mnt/d/Placement_Materials/PROJECTS/NEWS_ANALYTICS_PIPELINE_PROJECT/news-pipeline-v2/news-pipeline-v2"
PG_PORT=5433
NEWS_VENV="$HOME/news-pipeline-venv"
AIRFLOW_VENV="$HOME/airflow-venv"

cd "${PROJECT_ROOT}"

echo "== 1/5: Starting Postgres (port ${PG_PORT}) =="
if pg_lsclusters | grep -q "online"; then
    echo "Postgres already online, skipping."
else
    sudo pg_ctlcluster 16 main start --skip-systemctl-redirect
    sleep 2
fi
pg_lsclusters

echo ""
echo "== 2/5: Checking Postgres is reachable on port ${PG_PORT} =="
if sudo -u postgres psql -p "${PG_PORT}" -c "SELECT 1;" > /dev/null 2>&1; then
    echo "OK: Postgres responding on port ${PG_PORT}."
else
    echo "FAILED: could not connect on port ${PG_PORT}."
    echo "Remember: this machine also has a native Windows PostgreSQL"
    echo "service on 5432 -- our cluster deliberately runs on 5433."
    exit 1
fi

echo ""
echo "== 3/5: Checking Ollama is reachable from WSL2 =="
if curl -s -m 3 http://localhost:11434/api/tags > /dev/null 2>&1; then
    echo "OK: Ollama reachable."
else
    echo "WARNING: Ollama not reachable from WSL2."
    echo "On Windows: check the tray for the Ollama icon, or run 'ollama"
    echo "serve' in a Command Prompt. Confirm OLLAMA_HOST=0.0.0.0 is set"
    echo "and Ollama has been FULLY restarted since it was set."
    echo "Continuing anyway -- the digest step just won't produce summaries."
fi

echo ""
echo "== 4/5: Starting the API and dashboard in the background =="
mkdir -p /tmp/pipeline-logs
nohup "${NEWS_VENV}/bin/uvicorn" api.main:app --port 8000 \
    > /tmp/pipeline-logs/api.log 2>&1 &
echo "API starting (PID $!), logs: /tmp/pipeline-logs/api.log"

nohup "${NEWS_VENV}/bin/streamlit" run dashboard/app.py \
    --server.headless true \
    > /tmp/pipeline-logs/dashboard.log 2>&1 &
echo "Dashboard starting (PID $!), logs: /tmp/pipeline-logs/dashboard.log"

sleep 3
# Best-effort: open both in your default Windows browser via WSL2 interop.
# Harmless if this fails -- just open the URLs manually if so.
explorer.exe "http://localhost:8000/docs" > /dev/null 2>&1 || true
explorer.exe "http://localhost:8501" > /dev/null 2>&1 || true

echo ""
echo "API docs:  http://localhost:8000/docs"
echo "Dashboard: http://localhost:8501"
echo "(If the browser didn't open automatically, open those URLs by hand.)"
echo ""
echo "To stop them later: pkill -f 'uvicorn api.main' && pkill -f 'streamlit run dashboard/app.py'"

echo ""
echo "== 5/5: Starting Airflow standalone =="
echo "This takes over this terminal from here on, streaming logs"
echo "continuously. Open a NEW terminal for any other commands."
echo ""
sleep 2

source "${AIRFLOW_VENV}/bin/activate"
export PGPORT="${PG_PORT}"
airflow standalone
