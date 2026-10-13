#!/usr/bin/env bash
# Utilidades de los workflows para la EC2 de la API. Se ejecuta en el runner de GitHub, con las
# credenciales OIDC ya cargadas por aws-actions/configure-aws-credentials.
#
#   ec2.sh instancia            imprime "<id> <estado>" de la EC2 con tag Name=jupiter-api (o nada)
#   ec2.sh ejecutar <comando>   lo ejecuta en la EC2 con SSM Run Command (sin SSH), espera a que
#                               termine, muestra su salida y falla si el comando falla
set -euo pipefail
NOMBRE="${EC2_NOMBRE:-jupiter-api}"

instancia() {
    aws ec2 describe-instances \
        --filters "Name=tag:Name,Values=$NOMBRE" \
                  "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query "Reservations[].Instances[].[InstanceId,State.Name]" --output text | head -n 1
}

ejecutar() {
    local comando="$1" salida id estado parametros orden="" st="Pending"
    salida=$(instancia)   # en dos pasos: si la consulta falla, falla aquí y no parece "no existe"
    read -r id estado <<< "$salida"
    if [ "${estado:-}" != "running" ]; then
        echo "::error::La EC2 '$NOMBRE' no está encendida (estado: ${estado:-no existe})"
        return 1
    fi
    parametros=$(jq -nc --arg c "$comando" '{commands: [$c]}')

    # Recién encendida, el agente de SSM tarda alrededor de un minuto en estar disponible.
    for _ in $(seq 30); do
        orden=$(aws ssm send-command --instance-ids "$id" --document-name AWS-RunShellScript \
                    --parameters "$parametros" --comment "GitHub Actions ${GITHUB_RUN_ID:-local}" \
                    --query Command.CommandId --output text 2> /dev/null) && break
        sleep 10
    done
    if [ -z "$orden" ]; then
        echo "::error::No se pudo enviar el comando a la EC2: ¿el agente de SSM no está disponible?"
        return 1
    fi

    # Hasta 15 min: la primera descarga de la imagen del RAG son ~3 GB.
    for _ in $(seq 180); do
        sleep 5
        st=$(aws ssm get-command-invocation --command-id "$orden" --instance-id "$id" \
                 --query Status --output text 2> /dev/null || echo Pending)
        case "$st" in Success|Failed|Cancelled|TimedOut) break ;; esac
    done

    aws ssm get-command-invocation --command-id "$orden" --instance-id "$id" \
        --query "[StandardOutputContent, StandardErrorContent]" --output text || true
    if [ "$st" != "Success" ]; then
        echo "::error::El comando en la EC2 terminó en estado $st"
        return 1
    fi
}

case "${1:-}" in
    instancia) instancia ;;
    ejecutar)  shift; ejecutar "$*" ;;
    *) echo "uso: ec2.sh instancia | ejecutar <comando>" >&2; exit 2 ;;
esac
