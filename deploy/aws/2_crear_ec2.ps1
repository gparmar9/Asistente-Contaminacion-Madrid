# Parte B2 de la EC2: crea la máquina y su apagado nocturno.
#
#   pwsh deploy/aws/2_crear_ec2.ps1
#
# Antes: 1_recursos_ec2.ps1 y desplegar desde GitHub web, api-usuario, agente y rag. Al crearse,
# la máquina instala Docker (user-data.sh) y levanta los 4 contenedores con las versiones de SSM.
# Desde ese momento cuesta ~0,045 $/hora mientras esté encendida [estimación]; se apaga sola a la
# 01:30 o con el workflow «Apagar entorno».
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$Env:AWS_DEFAULT_REGION = "eu-west-1"

function Paso($texto) { Write-Host "`n== $texto" -ForegroundColor Cyan }
function Existe([scriptblock]$consulta) {
    $PSNativeCommandUseErrorActionPreference = $false
    & $consulta 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}

if (-not (Test-Path deploy/ec2/user-data.sh)) { throw "Ejecútalo desde la raíz del repositorio" }
aws sts get-caller-identity --query Arn --output text

Paso "0. Comprobaciones"
foreach ($s in "web", "api-usuario", "agente", "rag") {
    if (-not (Existe { aws ssm get-parameter --name "/jupiter/versiones/$s" })) {
        throw "Falta desplegar '$s' desde GitHub (no hay /jupiter/versiones/$s): la máquina no podría arrancar"
    }
}
$Existente = aws ec2 describe-instances --filters "Name=tag:Name,Values=jupiter-api" `
    "Name=instance-state-name,Values=pending,running,stopping,stopped" `
    --query "Reservations[].Instances[].InstanceId" --output text
if ($Existente) { throw "Ya existe una EC2 jupiter-api ($Existente): no se crea otra" }
Write-Host "Las 4 versiones están en SSM y no hay ninguna EC2 jupiter-api"

Paso "1. Crear la EC2"
$Vpc = aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query "Vpcs[0].VpcId" --output text
$Sg = aws ec2 describe-security-groups --filters "Name=group-name,Values=jupiter-api-sg" "Name=vpc-id,Values=$Vpc" `
        --query "SecurityGroups[0].GroupId" --output text
$Subred = aws ec2 describe-subnets --filters "Name=vpc-id,Values=$Vpc" "Name=availability-zone,Values=eu-west-1a" `
            "Name=default-for-az,Values=true" --query "Subnets[0].SubnetId" --output text
# - La imagen: la última Amazon Linux 2023 x86 (AWS publica su ID en un parámetro público).
# - Disco gp3 de 30 GB cifrado, que se borra con la máquina.
# - Metadatos solo con IMDSv2 (token) y 2 saltos: el contenedor del agente está un salto más allá
#   que la máquina y necesita llegar a ellos para obtener las credenciales del rol (Bedrock).
# - Etiqueta Name=jupiter-api: es por la que la encuentran los workflows y la que limita sus permisos.
$Instancia = aws ec2 run-instances `
    --image-id resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 `
    --instance-type t3.medium `
    --subnet-id $Subred --security-group-ids $Sg --associate-public-ip-address `
    --iam-instance-profile Name=jupiter-ec2-api `
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3,Encrypted=true,DeleteOnTermination=true}" `
    --metadata-options "HttpTokens=required,HttpPutResponseHopLimit=2,HttpEndpoint=enabled" `
    --user-data file://deploy/ec2/user-data.sh `
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=jupiter-api},{Key=Proyecto,Value=jupiter}]" `
                         "ResourceType=volume,Tags=[{Key=Name,Value=jupiter-api},{Key=Proyecto,Value=jupiter}]" `
    --query "Instances[0].InstanceId" --output text
Write-Host "Creada: $Instancia. Esperando a que esté encendida..."
aws ec2 wait instance-running --instance-ids $Instancia
$Ip = aws ec2 describe-instances --instance-ids $Instancia --query "Reservations[0].Instances[0].PublicIpAddress" --output text
Write-Host "Encendida. IP pública: $Ip"

Paso "2. Apagado nocturno (01:30, hora de Madrid)"
if (Existe { aws iam get-role --role-name jupiter-scheduler-ec2 }) {
    Write-Host "El rol jupiter-scheduler-ec2 ya existe"
} else {
    aws iam create-role --role-name jupiter-scheduler-ec2 `
        --assume-role-policy-document file://deploy/iam/confianza-scheduler.json `
        --description "EventBridge Scheduler: apagar la EC2 de la aplicacion" --query Role.RoleName --output text
    Start-Sleep 10   # un rol recién creado tarda unos segundos en poder usarse
}
aws iam put-role-policy --role-name jupiter-scheduler-ec2 --policy-name apagar-ec2 `
    --policy-document file://deploy/iam/permisos-scheduler-ec2.json
$RolScheduler = aws iam get-role --role-name jupiter-scheduler-ec2 --query Role.Arn --output text
# Llama directamente a ec2:StopInstances (destino "universal" de Scheduler), sin runbook.
$Destino = @{
    Arn     = "arn:aws:scheduler:::aws-sdk:ec2:stopInstances"
    RoleArn = $RolScheduler
    Input   = (@{ InstanceIds = @($Instancia) } | ConvertTo-Json -Compress)
} | ConvertTo-Json -Compress
$Fichero = New-TemporaryFile
Set-Content $Fichero $Destino -Encoding utf8NoBOM
$Accion = if (Existe { aws scheduler get-schedule --name jupiter-ec2-apagar }) { "update-schedule" } else { "create-schedule" }
aws scheduler $Accion --name jupiter-ec2-apagar `
    --schedule-expression "cron(30 1 * * ? *)" --schedule-expression-timezone Europe/Madrid `
    --flexible-time-window Mode=OFF --target "file://$Fichero" `
    --description "Apaga la EC2 de la aplicacion cada noche, como la RDS" --query ScheduleArn --output text | Out-Null
Remove-Item $Fichero
Write-Host "Programado jupiter-ec2-apagar"

Paso "3. Primer arranque"
Write-Host "La máquina está instalando Docker y descargando las imágenes (~5-10 min la primera vez)."
Write-Host "Sigue el progreso con el workflow «Encender entorno» (espera a que todo esté sano) o con:"
Write-Host "  aws ssm start-session --target $Instancia     (y dentro: sudo journalctl -u jupiter -f)"
Write-Host "`nCuando esté lista: http://$Ip" -ForegroundColor Green
