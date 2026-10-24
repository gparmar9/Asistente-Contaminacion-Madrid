# Imagen del frontend: build multi-stage (node -> nginx).
#
# Etapa 1 compila el proyecto (tsc + vite build). Etapa 2 solo contiene nginx y
# los ficheros estáticos: ~50 MB, sin Node ni dependencias de desarrollo.
# El proxy de /api/* y /phoenix/* se configura en nginx.conf.

FROM node:22-alpine AS build
WORKDIR /app

# Primero las dependencias (capa cacheable), luego el código
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY . .
# `npm run build` = tsc -b && vite build: un error de tipos rompe la imagen a propósito.
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
# Se copia fuera de conf.d para incluirlo solo en location /phoenix/.
COPY phoenix-acceso.conf /etc/nginx/phoenix-acceso.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD wget -qO- http://127.0.0.1/ > /dev/null || exit 1
