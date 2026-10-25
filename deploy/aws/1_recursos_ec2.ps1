# Parte B1 de la EC2: todo lo que necesita la máquina, menos la máquina.
#
#   pwsh deploy/aws/1_recursos_ec2.ps1                  # detecta tu IP pública
#   pwsh deploy/aws/1_recursos_ec2.ps1 -MiIp 1.2.3.4    # o se la pasas
#
# Requisitos: credenciales de AWS en la sesión ($Env:AWS_ACCESS_KEY_ID...) y ejecutarlo desde la
# raíz del repositorio. Se puede repetir: lo que ya existe se salta o se actualiza.
#
# Crea o actualiza:
#   1. 5 repositorios de ECR (jupiter-web, -api-usuario, -agente, -rag, -prediccion), con limpieza automática
#   2. El rol de la EC2 (jupiter-ec2-api) y su perfil de instancia
#   3. Los permisos nuevos de los roles de GitHub Actions (despliegue y entorno)
#   4. El grupo de seguridad jupiter-api-sg: puerto 80 abierto solo a tu IP
#   5. Los parámetros del agente en SSM (/jupiter/agente/llm_modelo, llm_modelo_clasificador y
#      comprobaciones_bloquean)
#   6. Los ficheros de arranque en s3://jupiter-calidad-aire-madrid/ec2/
# No crea la EC2: eso es 2_crear_ec2.ps1, después de desplegar las 5 imágenes.
param(
    [string]$MiIp = ""
)
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true   # un comando aws que falla detiene el script
$Env:AWS_DEFAULT_REGION = "eu-west-1"

$Bucket = "jupiter-calidad-aire-madrid"
$Servicios = "web", "api-usuario", "agente", "rag", "prediccion"
# Los mismos que deja 5_modelo_agente.ps1: repetir este script no devuelve el agente al modelo anterior.
$ModeloAgente = "nvidia.nemotron-super-3-120b"
$ModeloClasificador = "openai.gpt-oss-120b-1:0"
$ComprobacionesBloquean = "fuga"

function Paso($texto) { Write-Host "`n== $texto" -ForegroundColor Cyan }

# Comprobar si algo existe sin que el error detenga el script.
function Existe([scriptblock]$consulta) {
    $PSNativeCommandUseErrorActionPreference = $false
    & $consulta 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

if (-not (Test-Path deploy/iam/permisos-ec2-api.json)) { throw "Ejecútalo desde la raíz del repositorio" }
aws sts get-caller-identity --query Arn --output text

if (-not $MiIp) { $MiIp = (Invoke-RestMethod https://checkip.amazonaws.com).Trim() }
Write-Host "Tu IP pública: $MiIp (solo ella podrá abrir la web)"

# --- 1. ECR -----------------------------------------------------------------------------------
Paso "1. Repositorios de ECR"
# Solo se guardan las 5 últimas imágenes de cada servicio: la del RAG ocupa ~3 GB y ECR cobra
# por GB almacenado. 5 versiones bastan para el rollback.
$Limpieza = '{"rules":[{"rulePriority":1,"description":"Conservar las 5 ultimas imagenes","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":5},"action":{"type":"expire"}}]}'
foreach ($s in $Servicios) {
    $repo = "jupiter-$s"
    if (Existe { aws ecr describe-repositories --repository-names $repo }) {
        Write-Host "$repo ya existe"
    } else {
        aws ecr create-repository --repository-name $repo --query repository.repositoryName --output text
    }
    aws ecr put-lifecycle-policy --repository-name $repo --lifecycle-policy-text $Limpieza --query repositoryName --output text | Out-Null
}

# --- 2. Rol de la EC2 -------------------------------------------------------------------------
Paso "2. Rol de la máquina (jupiter-ec2-api)"
if (Existe { aws iam get-role --role-name jupiter-ec2-api }) {
    Write-Host "El rol ya existe"
} else {
    aws iam create-role --role-name jupiter-ec2-api `
        --assume-role-policy-document file://deploy/iam/confianza-ec2.json `
        --description "EC2 de la aplicacion: imagenes de ECR, parametros de SSM y Bedrock" `
        --query Role.RoleName --output text
}
# Lo que puede hacer la máquina (se sobrescribe: así se aplican los cambios del fichero).
aws iam put-role-policy --role-name jupiter-ec2-api --policy-name ec2-api `
    --policy-document file://deploy/iam/permisos-ec2-api.json
# Session Manager: entrar en la máquina y que GitHub le mande comandos, sin SSH ni puerto 22.
aws iam attach-role-policy --role-name jupiter-ec2-api `
    --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
# Una EC2 no recibe un rol directamente, sino un "perfil de instancia" que lo contiene.
if (Existe { aws iam get-instance-profile --instance-profile-name jupiter-ec2-api }) {
    Write-Host "El perfil de instancia ya existe"
} else {
    aws iam create-instance-profile --instance-profile-name jupiter-ec2-api --query InstanceProfile.InstanceProfileName --output text
    aws iam add-role-to-instance-profile --instance-profile-name jupiter-ec2-api --role-name jupiter-ec2-api
}

# --- 3. Roles de GitHub Actions ---------------------------------------------------------------
Paso "3. Permisos nuevos de GitHub Actions"
# Mismos nombres de política que ya tienen: se sustituyen por la versión del repositorio.
aws iam put-role-policy --role-name jupiter-github-actions-deploy --policy-name despliegue-ecr-lambda `
    --policy-document file://deploy/iam/permisos-github-actions.json
aws iam put-role-policy --role-name jupiter-github-actions-entorno --policy-name encender-entorno `
    --policy-document file://deploy/iam/permisos-github-entorno.json
Write-Host "Despliegue: ECR de los 5 servicios, versiones en SSM, ficheros en S3 y comandos en la EC2"
Write-Host "Entorno: encender y apagar también la EC2"

# --- 4. Grupo de seguridad --------------------------------------------------------------------
Paso "4. Grupo de seguridad (jupiter-api-sg)"
$Vpc = aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query "Vpcs[0].VpcId" --output text
$Sg = aws ec2 describe-security-groups --filters "Name=group-name,Values=jupiter-api-sg" "Name=vpc-id,Values=$Vpc" `
        --query "SecurityGroups[0].GroupId" --output text
if ($Sg -eq "None") {
    $Sg = aws ec2 create-security-group --group-name jupiter-api-sg --vpc-id $Vpc `
            --description "EC2 de la aplicacion: web (80) solo desde las IP del equipo" `
            --query GroupId --output text
    Write-Host "Creado $Sg"
} else {
    Write-Host "Ya existe: $Sg"
}
# Sin regla para el 22: no hay SSH. La salida a internet (ECR, SSM, Bedrock) viene permitida.
if (Existe { aws ec2 authorize-security-group-ingress --group-id $Sg --ip-permissions "IpProtocol=tcp,FromPort=80,ToPort=80,IpRanges=[{CidrIp=$MiIp/32,Description=web-equipo}]" }) {
    Write-Host "Puerto 80 abierto para $MiIp"
} else {
    Write-Host "El puerto 80 ya estaba abierto para $MiIp"
}

# --- 5. Parámetros del agente -----------------------------------------------------------------
Paso "5. Configuración del agente en SSM"
# No son secretos (String). Cambiar de modelo = cambiar el valor y reiniciar el agente.
aws ssm put-parameter --name /jupiter/agente/llm_modelo --type String --overwrite `
    --value $ModeloAgente --description "Modelo de Bedrock del agente" --query Version --output text | Out-Null
aws ssm put-parameter --name /jupiter/agente/llm_modelo_clasificador --type String --overwrite `
    --value $ModeloClasificador --description "Modelo de Bedrock del clasificador de intencion" --query Version --output text | Out-Null
aws ssm put-parameter --name /jupiter/agente/comprobaciones_bloquean --type String --overwrite `
    --value $ComprobacionesBloquean --description "Reglas que sustituyen la respuesta (cifras, fuga, internos)" --query Version --output text | Out-Null
Write-Host "llm_modelo = $ModeloAgente"
Write-Host "llm_modelo_clasificador = $ModeloClasificador"
Write-Host "comprobaciones_bloquean = $ComprobacionesBloquean"

# --- 6. Ficheros de arranque ------------------------------------------------------------------
Paso "6. Ficheros de arranque en S3"
# El user data de la EC2 los descarga al crearla. Después los mantiene al día cada despliegue.
aws s3 cp deploy/ec2/ "s3://$Bucket/ec2/" --recursive --only-show-errors
aws s3 ls "s3://$Bucket/ec2/"

Write-Host "`nListo. Siguiente: mergear a main y desplegar web, api-usuario, agente y rag desde GitHub." -ForegroundColor Green
