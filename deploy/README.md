# Despliegue en AWS — Fase 1

Todo lo necesario para que el pipeline de tiempo real se ejecute **solo en la nube**, sin depender
de que nadie tenga el ordenador encendido. Esta carpeta contiene únicamente infraestructura; la
lógica del proyecto sigue en [`src/`](../src/).

```
deploy/
├── Dockerfile.lambda          # imagen que ejecuta Lambda
├── Dockerfile.lambda.dockerignore   # qué NO viaja al construirla (Docker lo asocia por el nombre)
├── Dockerfile.rag             # imagen del servicio RAG (modelo e índice dentro)
├── Dockerfile.rag.dockerignore
├── lambda_handler.py          # punto de entrada de la función
├── requirements-lambda.txt    # dependencias de esa imagen (solo las que usa el pipeline)
└── iam/                       # permisos, versionados para poder revisarlos en un PR
    ├── confianza-lambda.json
    ├── permisos-lambda.json
    ├── confianza-scheduler.json
    ├── permisos-scheduler.json
    ├── confianza-github-actions.json
    ├── permisos-github-actions.json
    ├── permisos-github-entorno.json
    ├── confianza-automation-rds.json
    └── permisos-automation-rds.json
```

El despliegue automático vive en [`desplegar.yml`](../.github/workflows/desplegar.yml) y
[`rollback.yml`](../.github/workflows/rollback.yml) (en `.github/workflows/`), y el encendido y
apagado a demanda en [`encender_entorno.yml`](../.github/workflows/encender_entorno.yml) y
[`apagar_entorno.yml`](../.github/workflows/apagar_entorno.yml).

## Qué hay desplegado

| Servicio | Nombre | Para qué |
|---|---|---|
| **S3** | `jupiter-calidad-aire-madrid` | Modelo entrenado, histórico y parquet |
| **RDS PostgreSQL** | `jupiter-postgres` | La base de datos del proyecto (`db.t4g.micro`). Encendida solo de noche y a demanda |
| **ECR** | `jupiter-pipeline` | Registro de la imagen de la función |
| **Lambda** | `jupiter-pipeline` | Ejecuta el pipeline (imagen, x86_64, 1024 MB, timeout 300 s) |
| **EventBridge Scheduler** | `jupiter-pipeline-diario` | Lo dispara a las 23:45 (`Europe/Madrid`) |
| **EventBridge Scheduler** | `jupiter-rds-encender` / `jupiter-rds-apagar` | Enciende la RDS a las 22:00 y la apaga a la 01:30 (`Europe/Madrid`) |
| **Systems Manager Automation** | `AWS-StartRdsInstance` / `AWS-StopRdsInstance` | Runbooks de AWS que encienden o apagan la RDS solo si hace falta |
| **SSM Parameter Store** | `/jupiter/database_url` | La cadena de conexión, cifrada |
| **CloudWatch + SNS** | `jupiter-alertas` | Logs y avisos por correo |

Flujo de una ejecución:

```
Scheduler (23:45) ──▶ Lambda ──▶ API de Madrid
                        │  ├─ lee el secreto de SSM
                        │  └─ descarga el modelo de S3 (lo cachea en /tmp)
                        └──▶ RDS: calidad_aire_horas_live + resumen_datos_ml
```

## Por qué una imagen de contenedor y no un .zip

Lambda admite los dos formatos, pero un `.zip` está limitado a **250 MB descomprimidos** y
pandas + NumPy + SciPy + scikit-learn los superan. Con imagen el límite son 10 GB (la nuestra ocupa
1,26 GB, ~300 MB comprimida en ECR).

Ventaja añadida: la imagen base de AWS trae un emulador del entorno de Lambda, así que **lo mismo
que se despliega se puede probar en local** antes de subirlo.

## El handler

[`lambda_handler.py`](lambda_handler.py) **no duplica lógica**: importa `pipeline_tiempo_real` y solo
resuelve de dónde salen en la nube las tres cosas que en local vienen del disco y del `.env`:

| En local | En la nube |
|---|---|
| `models/isolation_forest.joblib` | S3 → `/tmp` (único directorio escribible) |
| `DATABASE_URL` del `.env` | SSM Parameter Store, descifrado con el rol |
| `data/raw/estaciones-de-control.csv` | Dentro de la propia imagen (son 5 KB) |

Todo eso está **fuera** de la función `handler`, así que se ejecuta una vez por contenedor y no en
cada invocación: Lambda reutiliza el contenedor mientras está caliente.

**Variables de entorno de la función:** `S3_BUCKET` (obligatoria), `MODEL_KEY` y `SSM_PARAM`
(opcionales, con valores por defecto). La contraseña no aparece en ninguna.

## Los roles IAM

Hay **cinco roles**, y cada uno se define con **dos ficheros**, que responden a preguntas distintas:

- **Política de confianza** (`confianza-*.json`): *¿quién puede ponerse este rol?*
- **Política de permisos** (`permisos-*.json`): *¿qué puede hacer quien lo lleva puesto?*

### `jupiter-lambda-pipeline` — rol de ejecución de la función

| Permiso | Alcance |
|---|---|
| `AWSLambdaBasicExecutionRole` (política gestionada por AWS) | Escribir sus logs en CloudWatch |
| `s3:GetObject` | **Solo** `models/*` del bucket del proyecto |
| `ssm:GetParameter` + `kms:Decrypt` | **Solo** los parámetros `/jupiter/*`, y solo a través de SSM |

Fíjate en lo estrecho que es: la función **no puede leer el histórico** del bucket, ni escribir en
S3, ni tocar otros parámetros. Si alguien colara código malicioso en la imagen, el daño posible
sería mínimo. Eso es el **mínimo privilegio**.

### `jupiter-scheduler-pipeline` — rol del disparador

Lo usan las tres programaciones. Puede:

- `lambda:InvokeFunction` sobre **esta** función, y ninguna otra.
- Lanzar **solo** los runbooks `AWS-StartRdsInstance` y `AWS-StopRdsInstance`.
- Entregarles el rol `jupiter-automation-rds` (`iam:PassRole`, solo hacia Systems Manager).

No puede tocar la RDS directamente: lo hace el runbook con su propio rol.

### `jupiter-automation-rds` — rol de los runbooks

Lo asume Systems Manager (solo desde esta cuenta) mientras ejecuta el runbook. Permisos:
consultar, encender y apagar **solo** `jupiter-postgres`. No puede borrarla ni tocar otras bases
de datos.

### `jupiter-github-actions-deploy` — rol del despliegue automático

Lo asumen los workflows [`desplegar.yml`](../.github/workflows/desplegar.yml) y
[`rollback.yml`](../.github/workflows/rollback.yml) mediante
**OIDC**: GitHub entrega al workflow un token firmado que dice de qué repositorio y rama viene, y
AWS lo cambia por credenciales temporales de este rol. No hay ninguna clave de AWS guardada en
GitHub.

| Permiso | Alcance |
|---|---|
| `ecr:GetAuthorizationToken` | Login en ECR (esta acción no admite restringirse a un repositorio) |
| Subida de imágenes (`ecr:PutImage`, capas…) | **Solo** el repositorio `jupiter-pipeline` |
| `lambda:UpdateFunctionCode`, `PublishVersion`, `ListVersionsByFunction` + lecturas | **Solo** la función `jupiter-pipeline` (no puede borrarla ni cambiar su configuración) |

**Quién puede asumirlo** ([`confianza-github-actions.json`](iam/confianza-github-actions.json)):
tokens con audiencia `sts.amazonaws.com` y `sub` de **este** repositorio, desde cualquier rama.
IAM solo sabe evaluar las claves `aud` y `sub` del token, así que no se puede restringir a un
fichero de workflow concreto. Consecuencia práctica: cualquier workflow del repositorio con
`id-token: write` podría asumir el rol, así que **los cambios en `.github/workflows/` se revisan
en el PR** como cualquier otro código con acceso a producción. El `<ID_CUENTA>` del fichero se
sustituye al aplicarlo; no se versiona porque el repositorio es público.

### `jupiter-github-actions-entorno` — rol del encendido y apagado a demanda

Lo asumen los workflows [`encender_entorno.yml`](../.github/workflows/encender_entorno.yml) y
[`apagar_entorno.yml`](../.github/workflows/apagar_entorno.yml) con la **misma** política de
confianza OIDC que el anterior. Permisos
([`permisos-github-entorno.json`](iam/permisos-github-entorno.json)): consultar, encender y apagar
**solo** `jupiter-postgres`. No puede borrarla ni modificarla. Se separa del rol de despliegue para
que cada workflow tenga solo lo que necesita.

## Desplegar un cambio

### Opción recomendada: desde GitHub Actions

Pestaña **Actions → Desplegar → Run workflow**, eligiendo:

- **La rama** en *Use workflow from*: se despliega el código de esa rama tal como está (lo normal
  es `main`; con otra rama el workflow avisa). No hay que indicar ninguna imagen: el workflow la
  construye. Elegir una versión anterior es cosa del rollback.
- **El componente** en el desplegable. Hoy solo existe `lambda`; la API, el orquestador, el RAG y
  la web se añadirán como nuevas opciones cuando se desplieguen en AWS.

Dos despliegues del **mismo** componente nunca coinciden (el segundo espera); de componentes
distintos, sí. Para la Lambda, el workflow:

1. Ejecuta los tests unitarios y de integración ([`tests.yml`](../.github/workflows/tests.yml)).
   Si fallan, no despliega.
2. Construye la imagen y la sube a ECR con dos tags: el **SHA corto del commit** (para saber qué
   código corre y poder volver atrás) y `latest`.
3. Actualiza la función, espera a que termine y publica una **versión numerada** con la
   descripción `deploy <commit> desde <rama> (@quien)`.
4. **Verifica** que el `CodeSha256` de la Lambda coincide con el digest recién subido; si no,
   falla.
5. Deja un resumen en la ejecución: rama, commit, tag, digest y versión publicada.

Requisitos (una sola vez):

- El workflow tiene que estar en `main`: GitHub solo muestra el botón *Run workflow* de los
  workflows que existen en la rama por defecto.
- Un secreto del repositorio `AWS_ACCOUNT_ID` (*Settings → Secrets and variables → Actions*) con
  el ID de la cuenta. Es un secreto para que GitHub lo oculte en los logs, que son públicos.

### Opción manual (sin GitHub Actions)

Desde la **raíz del repositorio** (el contexto de construcción incluye `src/` y el CSV de
estaciones):

```powershell
docker build --provenance=false --sbom=false -f deploy/Dockerfile.lambda -t jupiter-pipeline .

$cuenta = aws sts get-caller-identity --query Account --output text
$registro = "$cuenta.dkr.ecr.eu-west-1.amazonaws.com"

aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin $registro
docker tag jupiter-pipeline:latest "$registro/jupiter-pipeline:latest"
docker push "$registro/jupiter-pipeline:latest"

aws lambda update-function-code --function-name jupiter-pipeline --image-uri "$registro/jupiter-pipeline:latest" --publish
aws lambda wait function-updated --function-name jupiter-pipeline
```

### Tres cosas que se olvidan y cuestan una tarde

1. **`update-function-code` no es opcional.** Subir la imagen a ECR con el mismo tag **no**
   actualiza la función: se queda con la copia que tenía. Y como el tag es `latest`, nada te avisa.
2. **`--provenance=false`.** Sin esa opción, Docker sube además un manifiesto de atestación y la
   imagen queda publicada como *lista de manifiestos*, un formato que Lambda **no sabe leer**. El
   error que da no apunta a la causa.
3. **La arquitectura tiene que coincidir.** La imagen se construye `x86_64` en un PC normal y la
   función está creada como `x86_64`. Si no cuadran, falla al arrancar sin explicar por qué.

Para verificar que la función tiene la imagen nueva, compara el digest del `push` con:

```powershell
aws lambda get-function-configuration --function-name jupiter-pipeline --query "{estado:State,sha:CodeSha256}" --output table
```

## Volver a una versión anterior (rollback)

Si un despliegue rompe algo: **Actions → Rollback → Run workflow**, eligiendo el componente en el
desplegable (hoy solo `lambda`), en dos pasos.

1. **Consultar** (campos vacíos). No cambia nada: muestra en el resumen de la ejecución una tabla
   con las versiones publicadas (número, fecha, imagen, descripción) y cuál está en producción, y
   sugiere la anterior.
2. **Volver atrás**: relanzar con la **versión** elegida y el **motivo**. El workflow apunta la
   función a la imagen de esa versión por su digest (no reconstruye nada: segundos), publica una
   versión nueva con la descripción `rollback a vN: <motivo> (@quien)` y verifica que la Lambda
   ejecuta exactamente esa imagen.

No hay un "volver a la anterior" automático a propósito: después de un rollback, la versión
anterior es justo la que estaba mal. Si la versión activa ya es un rollback, la tabla lo avisa en
vez de sugerir.

El rollback funciona porque **ECR conserva las imágenes antiguas** aunque pierdan su tag cuando se
sube una nueva. Si algún día se añade una regla de ciclo de vida que borre imágenes sin tag, las
versiones que apunten a ellas dejarán de poder recuperarse.

Deshacer un rollback es otro rollback (a la versión que se retiró) o un despliegue nuevo.

## Encendido y apagado de la base de datos

La RDS **solo está encendida cuando hace falta**. Parada solo se paga el disco, no la instancia.

| Hora (Madrid) | Qué pasa |
|---|---|
| 22:00 | `jupiter-rds-encender` enciende la RDS |
| 23:45 | `jupiter-pipeline-diario` lanza la carga (la Lambda) |
| 01:30 | `jupiter-rds-apagar` la apaga, **también si alguien la encendió a mano y se olvidó** |

**Para trabajar o hacer una demo de día:** **Actions → Encender entorno → Run workflow**. Si ya está
encendida, termina bien sin hacer nada; si está parada, la enciende y espera a que esté lista
(unos minutos).

**Al terminar:** **Actions → Apagar entorno → Run workflow** para no pagar horas de más. Es
opcional, porque se apaga sola a la 01:30. Si ya está apagada, no hace nada; si se está
encendiendo, espera a que esté lista y la apaga. **Entre las 22:00 y las 00:00 se niega a
apagarla**, porque la carga de las 23:45 la necesita encendida.

Cómo está montado:

- Las programaciones no llaman a RDS directamente: lanzan los runbooks de AWS
  `AWS-StartRdsInstance` / `AWS-StopRdsInstance`, que **primero consultan el estado** y no hacen
  nada si la base ya está como se pide. Así no hay errores si alguien la encendió o apagó antes.
- Pocos reintentos (3, como mucho durante 30 min): un fallo de noche no debe acabar encendiéndola
  por la mañana.
- Las ventanas de RDS están dentro de la franja encendida, en verano y en invierno: copias
  **21:10–21:40 UTC** y mantenimiento **domingo 22:00–22:30 UTC**. RDS solo admite UTC; por eso la
  franja encendida es de 3,5 h y no de una (ver la §8.8 de las notas de la memoria).

Ver qué ha hecho cada noche:

```powershell
# Últimas ejecuciones de los runbooks (encender/apagar) y su resultado
aws ssm describe-automation-executions --filters Key=DocumentNamePrefix,Values=AWS-StartRdsInstance,AWS-StopRdsInstance --max-results 6 --query "AutomationExecutionMetadataList[].[DocumentName,AutomationExecutionStatus,ExecutionStartTime]" --output table

# Estado actual de la base de datos
aws rds describe-db-instances --db-instance-identifier jupiter-postgres --query "DBInstances[0].DBInstanceStatus" --output text
```

## Probar en local antes de desplegar

```powershell
docker run --rm -p 9000:8080 `
  -e S3_BUCKET=jupiter-calidad-aire-madrid `
  -e AWS_DEFAULT_REGION=eu-west-1 `
  -e AWS_ACCESS_KEY_ID=$Env:AWS_ACCESS_KEY_ID `
  -e AWS_SECRET_ACCESS_KEY=$Env:AWS_SECRET_ACCESS_KEY `
  -e AWS_SESSION_TOKEN=$Env:AWS_SESSION_TOKEN `
  jupiter-pipeline
```

Y desde otra terminal:

```powershell
curl.exe -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

En local se le pasan tus credenciales porque no hay rol; en AWS eso desaparece.

## Operación del día a día

```powershell
# ¿Se ha ejecutado?
aws logs tail /aws/lambda/jupiter-pipeline --since 1d --format short

# Invocarla a mano
aws lambda invoke --function-name jupiter-pipeline --cli-binary-format raw-in-base64-out --payload '{}' "$env:TEMP\respuesta.json"
Get-Content "$env:TEMP\respuesta.json"

# Estado de la programación
aws scheduler get-schedule --name jupiter-pipeline-diario --query "{estado:State,expresion:ScheduleExpression,zona:ScheduleExpressionTimezone}"

# Parar / arrancar la base de datos a mano (lo normal es el workflow "Encender entorno"
# y dejar que la programación la apague a la 01:30)
aws rds stop-db-instance  --db-instance-identifier jupiter-postgres
aws rds start-db-instance --db-instance-identifier jupiter-postgres
```

> Si la base de datos está parada a las 23:45, **la ejecución de esa noche falla**. La
> programación de las 22:00 lo evita; no desactives `jupiter-rds-encender` sin desactivar también
> la carga.

## Alarmas

| Alarma | Salta cuando | Por qué importa |
|---|---|---|
| `jupiter-pipeline-errores` | La función lanza una excepción | El fallo evidente |
| `jupiter-pipeline-sin-ejecuciones` | No hay ninguna invocación en 24 h | **El fallo silencioso**: si el disparador deja de funcionar no hay ningún error que contar, y el hueco en los datos se descubriría semanas después |

Ambas avisan por correo a través del tema SNS `jupiter-alertas`.

## Coste

Prácticamente todo el coste es la instancia de RDS. Encendida 24 h eran unos **15,5 $/mes** sin
capa gratuita; con el encendido programado (unas 3,5 h por noche más el uso a demanda) la
estimación baja a **4–5 $/mes**, pendiente de confirmar con la factura. Lambda, S3, ECR,
EventBridge, SSM, CloudWatch y SNS caen dentro de capas gratuitas a este volumen. Detalle en [`docs/notas_memoria.md`](../docs/notas_memoria.md).

**El coste a evitar** es un NAT Gateway (~32 $/mes): por eso la Lambda vive **fuera de la VPC** y se
conecta a RDS por su endpoint público, con TLS obligatorio (`rds.force_ssl = 1`) y la contraseña
cifrada en SSM. Es una decisión consciente, explicada en las notas.
