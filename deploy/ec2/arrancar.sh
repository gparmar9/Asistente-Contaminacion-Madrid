#!/usr/bin/env bash
# Arranca o actualiza la aplicación en la EC2.
#
#   arrancar.sh                 todos los servicios (lo lanza systemd en cada arranque)
#   arrancar.sh rag             solo ese servicio (lo lanzará el despliegue por SSM)
#
# Todo sale de AWS con el rol de la máquina, sin claves en disco más allá de este script:
#   s3://jupiter-calidad-aire-madrid/ec2/compose.yml   compose de producción (del repositorio)
#   SSM /jupiter/versiones/<servicio>                 qué imagen está activa (tag = commit)
#   SSM /jupiter/database_url                         cadena de conexión a RDS de la API (cifrada)
#   SSM /jupiter/agente/<variable>                    configuración del agente (opcional): cada
#                                                     parámetro es una variable de entorno, p. ej.
#                                                     /jupiter/agente/llm_modelo -> LLM_MODELO
set -euo pipefail

REGION=eu-west-1
BUCKET=jupiter-calidad-aire-madrid
DIR="${JUPITER_DIR:-/opt/jupiter}"   # variable solo para probarlo fuera de la EC2
SERVICIOS=(web api-usuario agente rag)
export AWS_DEFAULT_REGION="$REGION"

# A stderr: varios pasos redirigen stdout a ficheros .env y el mensaje acabaría dentro.
log() { echo "[jupiter] $*" >&2; }

# Valor de un parámetro de SSM, o vacío si no existe.
parametro() {
    aws ssm get-parameter --name "$1" --with-decryption \
        --query Parameter.Value --output text 2>/dev/null || true
}

# Línea VAR='valor' para un fichero .env de Docker Compose. Entre comillas simples el valor es
# literal: una contraseña con $ o # no se interpreta.
linea_env() {
    if [[ "$2" == *"'"* ]]; then
        log "El valor de $1 contiene una comilla simple: no se puede escribir de forma segura"
        exit 1
    fi
    printf "%s='%s'\n" "$1" "$2"
}

# Servicios a actualizar: los indicados (validados) o todos.
OBJETIVO=()
for s in "$@"; do
    if [[ " ${SERVICIOS[*]} " != *" $s "* ]]; then
        log "Servicio desconocido: '$s' (válidos: ${SERVICIOS[*]})"
        exit 1
    fi
    OBJETIVO+=("$s")
done

cd "$DIR"

log "Descargando compose.yml de s3://$BUCKET/ec2/"
aws s3 cp "s3://$BUCKET/ec2/compose.yml" compose.yml --only-show-errors

log "Leyendo versiones de SSM"
CUENTA=$(aws sts get-caller-identity --query Account --output text)
REGISTRO="$CUENTA.dkr.ecr.$REGION.amazonaws.com"
{
    echo "REGISTRO=$REGISTRO"
    for s in "${SERVICIOS[@]}"; do
        VERSION=$(parametro "/jupiter/versiones/$s")
        if [ -z "$VERSION" ]; then
            log "Falta /jupiter/versiones/$s: despliega '$s' con el workflow antes de arrancar"
            exit 1
        fi
        echo "VERSION_$(echo "$s" | tr 'a-z-' 'A-Z_')=$VERSION"
    done
} > versiones.env

log "Leyendo secretos de SSM"
umask 077   # los ficheros de secretos solo los lee root
DATABASE_URL=$(parametro /jupiter/database_url)
if [ -z "$DATABASE_URL" ]; then
    log "Falta /jupiter/database_url en SSM"
    exit 1
fi
linea_env DATABASE_URL "$DATABASE_URL" > secretos-bd.env

# Todo /jupiter/agente/ a variables de entorno: una variable nueva del agente se da de alta
# creando el parámetro, sin tocar este script. Salida en texto: nombre<TAB>valor por línea.
: > config-agente.env
# Ruta sin barra final: es la que autoriza el rol (parameter/jupiter/agente).
aws ssm get-parameters-by-path --path /jupiter/agente --with-decryption \
    --query "Parameters[].[Name,Value]" --output text > parametros-agente.tmp
while IFS=$'\t' read -r NOMBRE VALOR; do
    [ -n "$NOMBRE" ] || continue
    linea_env "$(basename "$NOMBRE" | tr 'a-z' 'A-Z')" "$VALOR" >> config-agente.env
done < parametros-agente.tmp
rm -f parametros-agente.tmp
if ! grep -q '^LLM_MODELO=' config-agente.env; then
    log "Falta /jupiter/agente/llm_modelo en SSM: el chat responderá 503 hasta configurarlo"
fi
if ! grep -q '^DATABASE_URL=' config-agente.env; then
    log "Falta /jupiter/agente/database_url en SSM: el agente no responderá preguntas de datos"
fi

log "Login en ECR ($REGISTRO)"
aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRO" > /dev/null

COMPOSE=(docker compose --env-file versiones.env -f compose.yml)
log "Descargando imágenes: ${OBJETIVO[*]:-todas}"
"${COMPOSE[@]}" pull "${OBJETIVO[@]}"
# --wait: no termina hasta que los contenedores pasan su healthcheck (o falla si no lo pasan),
# así un despliegue que deja un servicio roto falla en GitHub en vez de parecer correcto.
if [ ${#OBJETIVO[@]} -eq 0 ]; then
    "${COMPOSE[@]}" up -d --remove-orphans --wait --wait-timeout 600
else
    "${COMPOSE[@]}" up -d --wait --wait-timeout 600 "${OBJETIVO[@]}"
    # nginx resuelve api-usuario al arrancar y guarda su IP: si el contenedor se recrea con otra,
    # el proxy de /api/* quedaría apuntando a la antigua.
    if [[ " ${OBJETIVO[*]} " == *" api-usuario "* && " ${OBJETIVO[*]} " != *" web "* ]]; then
        log "Reiniciando web para que vuelva a resolver api-usuario"
        "${COMPOSE[@]}" restart web
    fi
fi

# Imágenes que ya no usa ningún contenedor (versiones anteriores): el disco es de 30 GB y la
# del RAG ocupa ~3 GB. Siguen en ECR, así que el rollback no depende de ellas.
docker image prune -af > /dev/null
log "Listo"
"${COMPOSE[@]}" ps
