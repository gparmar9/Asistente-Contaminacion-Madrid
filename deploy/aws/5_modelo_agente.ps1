# Parte B5 de la EC2: cambia el modelo de Bedrock del agente (por defecto, Nemotron 3 Super).
#
#   pwsh deploy/aws/5_modelo_agente.ps1                                   # Nemotron + fuga bloquea
#   pwsh deploy/aws/5_modelo_agente.ps1 -Modelo mistral.ministral-3-14b-instruct -Bloquean ""   # volver atrás
#
# Requisitos: credenciales de AWS en la sesión y ejecutarlo desde la raíz del repositorio. Se puede
# repetir: sobrescribe los valores.
#
# Hace:
#   1. Comprueba que el modelo existe en Bedrock (eu-west-1) y se puede usar bajo demanda.
#   2. Escribe /jupiter/agente/llm_modelo (conversación y SQL; el clasificador no cambia: sigue en
#      /jupiter/agente/llm_modelo_clasificador).
#   3. Escribe /jupiter/agente/comprobaciones_bloquean -> COMPROBACIONES_BLOQUEAN. Nemotron copia el
#      prompt de sistema si se lo piden (docs/notas_memoria.md, §7.3.1): con «fuga» esa respuesta se
#      sustituye por una frase fija. Vacío = se borra el parámetro y todas las reglas solo observan.
# No reinicia nada: el agente lee SSM al arrancar. Después: «Desplegar» → agente (desde main).
param(
    [string]$Modelo = "nvidia.nemotron-super-3-120b",
    [string]$Bloquean = "fuga"
)
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$Env:AWS_DEFAULT_REGION = "eu-west-1"

function Paso($texto) { Write-Host "`n== $texto" -ForegroundColor Cyan }
function Existe([scriptblock]$consulta) {
    $PSNativeCommandUseErrorActionPreference = $false
    & $consulta 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

# Comprueba la cuenta antes de tocar nada: la del proyecto es la que tiene la EC2 jupiter-api.
aws sts get-caller-identity --query Arn --output text
$Instancia = aws ec2 describe-instances --filters "Name=tag:Name,Values=jupiter-api" `
    --query "Reservations[0].Instances[0].InstanceId" --output text
if ($Instancia -eq "None") { throw "En esta cuenta no está la EC2 jupiter-api: revisa las credenciales" }
Write-Host "Cuenta del proyecto (EC2 $Instancia)"

Paso "1. Modelo en Bedrock ($Modelo)"
# Solo modelos de la región, sin perfil de inferencia (eu., global.): el rol de la EC2 solo puede
# invocar arn:aws:bedrock:eu-west-1::foundation-model/* (deploy/iam/permisos-ec2-api.json).
$Estado = aws bedrock get-foundation-model --model-identifier $Modelo `
    --query "[modelDetails.modelLifecycle.status, join(',', modelDetails.inferenceTypesSupported)]" --output text
$Ciclo, $Tipos = $Estado -split "`t"
if ($Ciclo -ne "ACTIVE" -or $Tipos -notmatch "ON_DEMAND") {
    throw "$Modelo no se puede usar bajo demanda en eu-west-1 (estado $Ciclo, tipos $Tipos)"
}
Write-Host "Disponible: $Ciclo, $Tipos"

Paso "2. Modelo del agente en SSM"
$Antes = aws ssm get-parameter --name /jupiter/agente/llm_modelo --query Parameter.Value --output text
aws ssm put-parameter --name /jupiter/agente/llm_modelo --type String --overwrite `
    --value $Modelo --description "Modelo de Bedrock del agente" --query Version --output text | Out-Null
Write-Host "llm_modelo: $Antes -> $Modelo"
$Clasificador = aws ssm get-parameter --name /jupiter/agente/llm_modelo_clasificador --query Parameter.Value --output text
Write-Host "llm_modelo_clasificador (sin cambios): $Clasificador"

Paso "3. Comprobaciones que bloquean"
if ($Bloquean) {
    aws ssm put-parameter --name /jupiter/agente/comprobaciones_bloquean --type String --overwrite `
        --value $Bloquean --description "Reglas que sustituyen la respuesta (cifras, fuga, internos)" `
        --query Version --output text | Out-Null
    Write-Host "comprobaciones_bloquean = $Bloquean"
} elseif (Existe { aws ssm get-parameter --name /jupiter/agente/comprobaciones_bloquean }) {
    aws ssm delete-parameter --name /jupiter/agente/comprobaciones_bloquean
    Write-Host "comprobaciones_bloquean borrado: todas las reglas solo observan"
} else {
    Write-Host "Sin comprobaciones_bloquean: todas las reglas solo observan"
}

Write-Host "`nListo. Siguiente: GitHub → Actions → Desplegar → agente (desde main)." -ForegroundColor Green
