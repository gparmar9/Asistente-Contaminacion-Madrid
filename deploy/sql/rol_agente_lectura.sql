-- Rol `agente_lectura`: el agente solo puede leer la vista `mediciones_bloques`.
--
-- Es la barrera real del SQL libre: aunque el validador del agente dejara pasar una sentencia,
-- este rol no puede escribir ni leer otras tablas. Lo usa el agente siempre, también en local.
--
-- Requisitos: vista_mediciones_bloques.sql ya ejecutado; usuario con permiso para crear roles
-- (en local, postgres; en RDS, el usuario maestro); PostgreSQL 15 o posterior (el esquema public
-- ya no deja crear objetos a PUBLIC).
--
-- La contraseña no va en el repositorio: se pasa como variable de psql desde el entorno.
--
--   psql "$DATABASE_URL" -v clave="$AGENTE_LECTURA_CLAVE" -f deploy/sql/rol_agente_lectura.sql
--
-- Idempotente: si el rol ya existe, actualiza la contraseña y vuelve a conceder los permisos.

\set ON_ERROR_STOP on

SELECT 'CREATE ROLE agente_lectura LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agente_lectura') \gexec

ALTER ROLE agente_lectura WITH LOGIN PASSWORD :'clave';

-- Valores por defecto de cada sesión del rol. El cliente puede cambiarlos con SET: son una capa
-- más, no la barrera (la barrera son los permisos de abajo). El agente fija además los suyos al
-- conectar (DB_TIMEOUT_S).
ALTER ROLE agente_lectura SET default_transaction_read_only = on;
ALTER ROLE agente_lectura SET statement_timeout = '5s';

GRANT CONNECT ON DATABASE :"DBNAME" TO agente_lectura;
GRANT USAGE ON SCHEMA public TO agente_lectura;
GRANT SELECT ON mediciones_bloques TO agente_lectura;
