# Despliegue en AWS — Fase 1

Todo lo necesario para que el pipeline de tiempo real se ejecute **solo en la nube**, sin depender
de que nadie tenga el ordenador encendido. Esta carpeta contiene únicamente infraestructura; la
lógica del proyecto sigue en [`src/`](../src/).

```
deploy/
├── Dockerfile.lambda          # imagen que ejecuta Lambda
├── lambda_handler.py          # punto de entrada de la función
├── requirements-lambda.txt    # dependencias de esa imagen (solo las que usa el pipeline)
└── iam/                       # permisos, versionados para poder revisarlos en un PR
    ├── confianza-lambda.json
    ├── permisos-lambda.json
    ├── confianza-scheduler.json
    └── permisos-scheduler.json
```

## Qué hay desplegado

| Servicio | Nombre | Para qué |
|---|---|---|
| **S3** | `jupiter-calidad-aire-madrid` | Modelo entrenado, histórico y parquet |
| **RDS PostgreSQL** | `jupiter-postgres` | La base de datos del proyecto (`db.t4g.micro`) |
| **ECR** | `jupiter-pipeline` | Registro de la imagen de la función |
| **Lambda** | `jupiter-pipeline` | Ejecuta el pipeline (imagen, x86_64, 1024 MB, timeout 300 s) |
| **EventBridge Scheduler** | `jupiter-pipeline-diario` | Lo dispara a las 23:45 (`Europe/Madrid`) |
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

Hay **dos roles**, y cada uno se define con **dos ficheros**, que responden a preguntas distintas:

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

Un único permiso: `lambda:InvokeFunction` sobre **esta** función. No puede invocar ninguna otra.

## Desplegar un cambio

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

# Parar / arrancar la base de datos (máximo 7 días parada: AWS la arranca sola)
aws rds stop-db-instance  --db-instance-identifier jupiter-postgres
aws rds start-db-instance --db-instance-identifier jupiter-postgres
```

> Si la base de datos está parada a las 23:45, **la ejecución de esa noche falla**.

## Alarmas

| Alarma | Salta cuando | Por qué importa |
|---|---|---|
| `jupiter-pipeline-errores` | La función lanza una excepción | El fallo evidente |
| `jupiter-pipeline-sin-ejecuciones` | No hay ninguna invocación en 24 h | **El fallo silencioso**: si el disparador deja de funcionar no hay ningún error que contar, y el hueco en los datos se descubriría semanas después |

Ambas avisan por correo a través del tema SNS `jupiter-alertas`.

## Coste

Unos **15,5 $/mes** si la cuenta no tiene capa gratuita, y prácticamente todo es la instancia de
RDS. Lambda, S3, ECR, EventBridge, SSM, CloudWatch y SNS caen dentro de capas gratuitas a este
volumen. Detalle en [`docs/notas_memoria.md`](../docs/notas_memoria.md).

**El coste a evitar** es un NAT Gateway (~32 $/mes): por eso la Lambda vive **fuera de la VPC** y se
conecta a RDS por su endpoint público, con TLS obligatorio (`rds.force_ssl = 1`) y la contraseña
cifrada en SSM. Es una decisión consciente, explicada en las notas.
