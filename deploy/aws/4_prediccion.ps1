# Parte B4 de la EC2: lo que necesita en AWS el servicio de predicción antes de desplegarlo.
#
#   pwsh deploy/aws/4_prediccion.ps1
#
# Requisitos: credenciales de AWS en la sesión y ejecutarlo desde la raíz del repositorio. Se puede
# repetir: lo que ya existe se salta o se actualiza.
#
# Hace:
#   1. Repositorio de ECR jupiter-prediccion, con la misma limpieza que el resto (5 imágenes).
#   2. Permisos: GitHub Actions puede subir imágenes a ese repositorio y la EC2 descargarlas
#      (sobrescribe las políticas con la versión de deploy/iam/, que solo añade ese repositorio).
# No necesita parámetros nuevos en SSM: la predicción usa /jupiter/agente/database_url (solo lectura).
# Después, desde main y en este orden: «Desplegar» → prediccion, api-usuario, agente y web.
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$Env:AWS_DEFAULT_REGION = "eu-west-1"

function Paso($texto) { Write-Host "`n== $texto" -ForegroundColor Cyan }
function Existe([scriptblock]$consulta) {
    $PSNativeCommandUseErrorActionPreference = $false
    & $consulta 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

if (-not (Test-Path deploy/iam/permisos-ec2-api.json)) { throw "Ejecútalo desde la raíz del repositorio" }
# Comprueba la cuenta antes de tocar nada: la del proyecto es la que tiene la EC2 jupiter-api.
aws sts get-caller-identity --query Arn --output text
$Instancia = aws ec2 describe-instances --filters "Name=tag:Name,Values=jupiter-api" `
    --query "Reservations[0].Instances[0].InstanceId" --output text
if ($Instancia -eq "None") { throw "En esta cuenta no está la EC2 jupiter-api: revisa las credenciales" }
Write-Host "Cuenta del proyecto (EC2 $Instancia)"

Paso "1. Repositorio de ECR (jupiter-prediccion)"
$Repo = "jupiter-prediccion"
$Limpieza = '{"rules":[{"rulePriority":1,"description":"Conservar las 5 ultimas imagenes","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":5},"action":{"type":"expire"}}]}'
if (Existe { aws ecr describe-repositories --repository-names $Repo }) {
    Write-Host "$Repo ya existe"
} else {
    aws ecr create-repository --repository-name $Repo --query repository.repositoryName --output text
}
aws ecr put-lifecycle-policy --repository-name $Repo --lifecycle-policy-text $Limpieza --query repositoryName --output text | Out-Null

Paso "2. Permisos de GitHub Actions y de la EC2"
# Mismos nombres de política que ya tienen: se sustituyen por la versión del repositorio.
aws iam put-role-policy --role-name jupiter-github-actions-deploy --policy-name despliegue-ecr-lambda `
    --policy-document file://deploy/iam/permisos-github-actions.json
aws iam put-role-policy --role-name jupiter-ec2-api --policy-name ec2-api `
    --policy-document file://deploy/iam/permisos-ec2-api.json
Write-Host "GitHub Actions puede subir a $Repo y la EC2 descargar de él"

Write-Host "`nListo. Siguiente: GitHub → Actions → Desplegar → prediccion (desde main)." -ForegroundColor Green
