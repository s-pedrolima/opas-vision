#!/bin/bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/deploy.log"
MODE="${1:-}"
EXTRA_FLAG="${2:-}"
PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python3"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_docker() {
    if ! docker info >/dev/null 2>&1; then
        log_message "❌ Docker is not running"
        exit 1
    fi
}

run_python_script() {
    local rel_path="$1"
    local title="$2"
    local extra_args="${3:-}"
    local full_path="$SCRIPT_DIR/$rel_path"

    if [ ! -f "$full_path" ]; then
        log_message "⚠️ $rel_path not found"
        return 1
    fi

    local command="cd \"$SCRIPT_DIR\" && \"$PYTHON_BIN\" \"$full_path\" $extra_args"

    if command -v gnome-terminal >/dev/null 2>&1; then
        gnome-terminal --title="$title" -- bash -c "$command; exec bash" >/dev/null 2>&1 &
    else
        bash -lc "$command" >>"$LOG_FILE" 2>&1 &
        log_message "⚠️ gnome-terminal not found; running $rel_path in background"
    fi
}

case $MODE in
    start)
        log_message "🟢 Starting O-PAS + ACP environment..."
        check_docker

        # 1. Camada OCF: Servidor OPC UA sobe primeiro para o Node-RED conectar sem erro
        log_message "📡 Starting OPC UA Server..."
        run_python_script "OCF/opcua_server.py" "OCF - OPC UA Server"
        sleep 2

        # 2. Camada DCN: OpenPLC + Node-RED no Docker Swarm
        if [ -d "$SCRIPT_DIR/DCN/Infra" ]; then
            log_message "📦 Deploying Docker stack (dcn-ws)..."
            (
                cd "$SCRIPT_DIR/DCN/Infra" || exit 1
                docker stack deploy -c WeldingStation.yml dcn-ws
            ) || exit 1
        else
            log_message "❌ DCN/Infra directory not found"
            exit 1
        fi

        # 3. Camada de Campo: Welding Station simulada (Modbus TCP <-> OpenPLC)
        log_message "🏭 Starting Welding Station I/O Simulation..."
        run_python_script "Field/IO_Simulation.py" "Field - IO Simulation"

        # 4. Camada OCF: Historian (OPC UA -> SQLite)
        log_message "🗄️ Starting Historian..."
        run_python_script "OCF/historian.py" "OCF - Historian"

        # 5. Camada ACP: Watcher determinístico -> AI Agent
        #    Dica: rode `./Simulation.sh start --dry-run` para vigiar sem chamar a LLM
        WATCHER_ARGS=""
        if [ "$EXTRA_FLAG" = "--dry-run" ]; then
            WATCHER_ARGS="--dry-run"
            log_message "👀 Starting ACP Watcher (DRY-RUN mode)..."
        else
            log_message "🤖 Starting ACP Watcher (AI Agent active)..."
        fi
        run_python_script "ACP/watcher.py" "ACP - Watcher" "$WATCHER_ARGS"

        log_message "✅ All layers started! Dashboard: http://localhost:1881/dashboard/page1"
        ;;

    stop)
        log_message "🔴 Stopping O-PAS + ACP environment..."

        # Envia SIGINT primeiro para o Historian fechar o estado atual como 'shutdown'
        log_message "🐍 Stopping Python services gracefully..."
        for script in "ACP/watcher.py" "OCF/historian.py" "Field/IO_Simulation.py" "OCF/opcua_server.py"; do
            pkill -INT -f "$script" 2>/dev/null || true
        done
        sleep 2

        # Garante encerramento caso algum processo ainda esteja vivo
        for script in "ACP/watcher.py" "OCF/historian.py" "Field/IO_Simulation.py" "OCF/opcua_server.py"; do
            pkill -f "$script" 2>/dev/null || true
        done

        log_message "📦 Removing Docker stack (dcn-ws)..."
        docker stack rm dcn-ws 2>/dev/null || true

        sleep 2
        log_message "✅ Done!"
        ;;

    status)
        echo "📊 O-PAS + ACP Status - $(date)"
        echo "=========================================="

        echo -e "\n📦 Docker Stack:"
        docker stack ls | grep -E "dcn-ws" || echo "  No active stack"
        docker service ls --filter name=dcn-ws 2>/dev/null || true

        echo -e "\n🐍 Python Services:"
        pgrep -af "opcua_server.py|IO_Simulation.py|historian.py|watcher.py" || echo "  No active scripts"

        echo -e "\n🧪 Active Faults:"
        if [ -f "$SCRIPT_DIR/Field/faults.json" ]; then
            cat "$SCRIPT_DIR/Field/faults.json"
            echo ""
        else
            echo "  None (normal operation)"
        fi

        echo -e "\n📝 Last lines from log:"
        tail -5 "$LOG_FILE" 2>/dev/null || echo "  No log found"
        ;;

    *)
        echo "Usage: $0 {start [--dry-run]|stop|status}"
        echo ""
        echo "Commands:"
        echo "  start            - Start OPC UA, Docker stack, Simulation, Historian, and Watcher"
        echo "  start --dry-run  - Same as start, but Watcher runs without calling the LLM API"
        echo "  stop             - Stop all Python services and remove Docker stack"
        echo "  status           - Show Docker stack, Python services, and active faults"
        exit 1
        ;;
esac
