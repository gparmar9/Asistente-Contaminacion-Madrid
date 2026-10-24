#!/bin/bash
# Primer arranque de la EC2 (cloud-init, Amazon Linux 2023). Se pasa como "user data" al
# crear la instancia y solo se ejecuta esa vez. Log en /var/log/cloud-init-output.log.
#
# Instala Docker y Docker Compose, copia de S3 el script de arranque y el servicio de systemd,
# y deja el servicio activado para que la API se levante en cada arranque.
set -euxo pipefail
export AWS_DEFAULT_REGION=eu-west-1
BUCKET=jupiter-calidad-aire-madrid
COMPOSE_VERSION=v2.29.7

# El agente de CloudWatch lo configura arrancar.sh (y lo instala si falta).
dnf install -y docker amazon-cloudwatch-agent
systemctl enable --now docker

# Amazon Linux 2023 no trae el plugin de Compose en sus repositorios.
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
docker compose version

mkdir -p /opt/jupiter
aws s3 cp "s3://$BUCKET/ec2/arrancar.sh" /opt/jupiter/arrancar.sh
aws s3 cp "s3://$BUCKET/ec2/jupiter.service" /etc/systemd/system/jupiter.service
chmod 700 /opt/jupiter/arrancar.sh

systemctl daemon-reload
systemctl enable jupiter.service
# Si aún no se ha desplegado ninguna imagen, el arranque falla con un mensaje claro y se
# reintenta en el siguiente encendido; no debe abortar el resto del user data.
systemctl start jupiter.service || echo "jupiter.service no arrancó todavía (¿faltan imágenes?)"
