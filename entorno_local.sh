#!/usr/bin/env bash
# Entorno de pruebas local: RAG + Phoenix + Agente + ApiUsuario con un solo comando.
#
# Cada servicio corre en su panel de una sesión de tmux (logs en directo) y queda un panel
# libre para lanzar consultas. El modelo y el resto de la configuración del agente salen
# siempre de Agente/.env; el script solo añade RAG_URL y PHOENIX_ENDPOINT.
#
#   ./entorno_local.sh                      levantar todo y entrar en la sesión
#   ./entorno_local.sh parar                parar todo (servicios y Phoenix)
#   ./entorno_local.sh estado               qué servicios responden
#   ./entorno_local.sh preguntar "texto" [session_id] [--crudo]
#                                           pregunta por ApiUsuario (/chat/stream)
#
# Guía de uso: sección "Entorno de pruebas local" del README.
# ApiUsuario arranca sin base de datos (/estaciones fallará). Para darle una:
#   DATABASE_URL=postgresql://... ./entorno_local.sh

set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESION=jupiter
PY_ML="$RAIZ/env-pontia-ml/bin"
ENV_AGENTE="$RAIZ/Agente/.env"
UMBRAL_RAG=0.1754
PUERTO_RAG=8010
PUERTO_AGENTE=8200
PUERTO_API=8000
URL_PHOENIX=http://localhost:6006/v1/traces

# Valor de una variable de Agente/.env, sin cargar el fichero entero en este shell.
leer_env() {
    grep -E "^$1=" "$ENV_AGENTE" | tail -1 | cut -d= -f2- | tr -d "\r\"'" || true
}

puerto_ocupado() {
    (: <"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

responde() {
    curl -sf --max-time 2 "$1" >/dev/null 2>&1
}

fallo() {
    echo "ERROR: $*" >&2
    exit 1
}

comprobar_requisitos() {
    command -v tmux >/dev/null || fallo "falta tmux (sudo apt install tmux)"
    [[ -x "$PY_ML/python" ]] || fallo "falta el entorno env-pontia-ml (RAG y ApiUsuario)"
    [[ -x "$RAIZ/Agente/.venv/bin/uvicorn" ]] || fallo "falta Agente/.venv (ver Agente/README.md)"
    [[ -f "$ENV_AGENTE" ]] || fallo "falta Agente/.env (copiar Agente/.env.example y rellenarlo)"

    local ocupados=()
    for puerto in "$PUERTO_RAG" "$PUERTO_AGENTE" "$PUERTO_API"; do
        if puerto_ocupado "$puerto"; then ocupados+=("$puerto"); fi
    done
    if ((${#ocupados[@]})); then
        fallo "puertos ocupados: ${ocupados[*]}. ¿Queda algo de otra sesión? Ver quién los usa: ss -ltnp"
    fi

    # Las claves del perfil SSO caducan: mejor saberlo ahora que en el primer turno.
    if [[ "$(leer_env LLM_PROVEEDOR)" == "bedrock" ]]; then
        local perfil
        perfil="$(leer_env AWS_PROFILE)"
        perfil="${perfil:-default}"
        aws sts get-caller-identity --profile "$perfil" >/dev/null 2>&1 \
            || fallo "las credenciales del perfil AWS '$perfil' no valen (¿caducadas?). Renuévalas con las claves del portal: aws configure set ... --profile $perfil"
    fi
}

arrancar() {
    if tmux has-session -t "$SESION" 2>/dev/null; then
        echo "La sesión '$SESION' ya existe: entrando (para empezar de cero: ./entorno_local.sh parar)."
        entrar
        return
    fi
    comprobar_requisitos

    # Phoenix es opcional: sin Docker, las trazas siguen yendo al JSONL de Agente/trazas.
    local phoenix=""
    if docker info >/dev/null 2>&1; then
        docker compose -f "$RAIZ/docker-compose.yml" --profile observabilidad up -d phoenix
        phoenix="$URL_PHOENIX"
    else
        echo "AVISO: Docker no responde (¿Docker Desktop parado?). Sigo sin Phoenix."
    fi

    local p_rag p_agente p_api p_consultas
    p_rag=$(tmux new-session -d -s "$SESION" -n servicios -c "$RAIZ" -x 200 -y 50 -P -F '#{pane_id}')
    tmux set -t "$SESION" mouse on
    tmux set -t "$SESION" pane-border-status top
    tmux set -t "$SESION" pane-border-format ' #{pane_title} '
    # Se pasa por el entorno de tmux para que la contraseña no quede escrita en el panel.
    if [[ -n "${DATABASE_URL:-}" ]]; then
        tmux set-environment -t "$SESION" DATABASE_URL "$DATABASE_URL"
    fi

    p_agente=$(tmux split-window -h -t "$p_rag" -c "$RAIZ/Agente" -P -F '#{pane_id}')
    p_api=$(tmux split-window -v -t "$p_rag" -c "$RAIZ/ApiUsuario" -P -F '#{pane_id}')
    p_consultas=$(tmux split-window -v -t "$p_agente" -c "$RAIZ" -P -F '#{pane_id}')

    tmux select-pane -t "$p_rag" -T "RAG :$PUERTO_RAG"
    tmux select-pane -t "$p_agente" -T "Agente :$PUERTO_AGENTE"
    tmux select-pane -t "$p_api" -T "ApiUsuario :$PUERTO_API"
    tmux select-pane -t "$p_consultas" -T "Consultas"

    # Cada servicio espera a que responda aquel del que depende. Si uno se cae, su panel
    # sigue abierto: flecha arriba + Enter lo vuelve a lanzar.
    tmux send-keys -t "$p_rag" \
        "RAG_UMBRAL_DISTANCIA=$UMBRAL_RAG $PY_ML/python -m rag.api" C-m
    tmux send-keys -t "$p_agente" \
        "echo 'Esperando al RAG (~1 min)...'; until curl -sf localhost:$PUERTO_RAG/salud >/dev/null; do sleep 2; done; RAG_URL=http://localhost:$PUERTO_RAG PHOENIX_ENDPOINT=$phoenix .venv/bin/uvicorn agente.main:app --app-dir src --port $PUERTO_AGENTE --env-file .env" C-m
    tmux send-keys -t "$p_api" \
        "echo 'Esperando al agente...'; until curl -sf localhost:$PUERTO_AGENTE/salud >/dev/null; do sleep 2; done; AGENTE_URL=http://localhost:$PUERTO_AGENTE $PY_ML/uvicorn api_usuario.main:app --app-dir src --port $PUERTO_API" C-m
    tmux send-keys -t "$p_consultas" \
        "until curl -sf localhost:$PUERTO_API/health >/dev/null; do sleep 2; done; clear; ./entorno_local.sh estado" C-m

    tmux select-pane -t "$p_consultas"
    entrar
}

# Desde dentro de otro tmux, attach falla (sesiones anidadas): se cambia de sesión.
entrar() {
    if [[ -n "${TMUX:-}" ]]; then
        tmux switch-client -t "$SESION"
    else
        tmux attach -t "$SESION"
    fi
}

parar() {
    if tmux has-session -t "$SESION" 2>/dev/null; then
        tmux kill-session -t "$SESION"
        echo "Sesión '$SESION' cerrada (RAG, agente y ApiUsuario parados)."
    else
        echo "No había sesión '$SESION'."
    fi
    if docker info >/dev/null 2>&1; then
        docker compose -f "$RAIZ/docker-compose.yml" --profile observabilidad stop phoenix
    fi
}

estado() {
    local nombre url
    while read -r nombre url; do
        if responde "$url"; then
            printf '  %-11s OK      %s\n' "$nombre" "$url"
        else
            printf '  %-11s CAÍDO   %s\n' "$nombre" "$url"
        fi
    done <<EOF
RAG http://localhost:$PUERTO_RAG/salud
Agente http://localhost:$PUERTO_AGENTE/salud
ApiUsuario http://localhost:$PUERTO_API/health
Phoenix http://localhost:6006
EOF
    cat <<EOF

Modelo: $(leer_env LLM_PROVEEDOR) / $(leer_env LLM_MODELO)   (Agente/.env)
Phoenix: http://localhost:6006   Docs de la API: http://localhost:$PUERTO_API/docs

Preguntar:   ./entorno_local.sh preguntar "¿Qué efectos tiene el NO2 en la salud?"
Seguir la conversación: añadir el session_id que sale al final
Ver el SSE tal cual: añadir --crudo
Moverse entre paneles: clic con el ratón, o Ctrl+b y una flecha
Salir sin parar nada: Ctrl+b y luego d    Parar todo: ./entorno_local.sh parar
EOF
}

# Convierte el SSE de /chat/stream en texto legible: fases entre corchetes (solo cuando
# cambian, el heartbeat las repite), tokens seguidos, y session_id y traza al final.
FORMATO_SSE=$(cat <<'EOF'
import json, sys

evento, ultima_fase = None, None
for linea in sys.stdin:
    linea = linea.rstrip("\n")
    if linea.startswith("event: "):
        evento = linea[7:]
        continue
    if not linea.startswith("data: "):
        if linea and not linea.startswith(":"):
            print(linea, flush=True)  # p. ej. el JSON de un 503 de ApiUsuario
        continue
    datos = json.loads(linea[6:])
    if evento == "status":
        fase = datos["fase"] + (f" {datos['herramienta']}" if datos.get("herramienta") else "")
        if fase != ultima_fase:
            print(f"[{fase}]", flush=True)
            ultima_fase = fase
    elif evento == "token":
        print(datos["texto"], end="", flush=True)
    elif evento == "passthrough":
        print(datos["texto"], flush=True)
    elif evento == "error":
        print(f"\n[error] {datos.get('detalle')}", flush=True)
    elif evento == "done":
        print(f"\n\nsession_id: {datos.get('session_id')}   traza: {datos.get('traza_id')}", flush=True)
EOF
)

preguntar() {
    local crudo=false args=()
    for arg in "$@"; do
        if [[ "$arg" == "--crudo" ]]; then crudo=true; else args+=("$arg"); fi
    done
    ((${#args[@]} >= 1)) || fallo 'uso: ./entorno_local.sh preguntar "texto" [session_id] [--crudo]'
    responde "http://localhost:$PUERTO_API/health" \
        || fallo "ApiUsuario no responde en el puerto $PUERTO_API (./entorno_local.sh estado)"

    local cuerpo
    cuerpo=$("$PY_ML/python" -c '
import json, sys
datos = {"pregunta": sys.argv[1]}
if len(sys.argv) > 2:
    datos["session_id"] = sys.argv[2]
print(json.dumps(datos, ensure_ascii=False))' "${args[@]}")

    local peticion=(curl -sN -X POST "http://localhost:$PUERTO_API/chat/stream"
        -H 'Content-Type: application/json' -d "$cuerpo")
    if $crudo; then
        "${peticion[@]}"
    else
        "${peticion[@]}" | "$PY_ML/python" -u -c "$FORMATO_SSE"
    fi
}

case "${1:-arrancar}" in
    arrancar) arrancar ;;
    parar) parar ;;
    estado) estado ;;
    preguntar) shift; preguntar "$@" ;;
    *) fallo "orden desconocida '$1' (arrancar | parar | estado | preguntar)" ;;
esac
