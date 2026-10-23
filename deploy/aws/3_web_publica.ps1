# Parte B3 de la EC2: la web pública para la corrección del TFM, con IP fija y presupuesto de Bedrock.
#
#   pwsh deploy/aws/3_web_publica.ps1 -Correo equipo@ejemplo.com
#
# Requisitos: credenciales de AWS en la sesión y ejecutarlo desde la raíz del repositorio, con la
# EC2 jupiter-api ya creada (2_crear_ec2.ps1). Se puede repetir: lo que ya está hecho se salta.
#
# Hace:
#   1. IP elástica (fija) para la EC2: la crea con la etiqueta Name=jupiter-api y la asocia.
#      Sustituye a la IP pública automática, que cambiaba en cada encendido.
#   2. Quita la contraseña de la web (/jupiter/web/contrasena): la web queda abierta a todo internet
#      y /phoenix/ responde 403 a todo el mundo (guarda las preguntas de los usuarios).
#   3. Presupuesto diario de Bedrock (jupiter-bedrock-diario): avisa por correo al pasar el 80 % y
#      el 100 % de -LimiteDia. Solo avisa, no corta nada.
# Después: «Desplegar → web» desde GitHub para que nginx deje de pedir la contraseña.
param(
    [Parameter(Mandatory = $true)][string]$Correo,
    [decimal]$LimiteDia = 2
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

if (-not (Test-Path deploy/ec2/arrancar.sh)) { throw "Ejecútalo desde la raíz del repositorio" }
$Cuenta = aws sts get-caller-identity --query Account --output text
aws sts get-caller-identity --query Arn --output text

$Instancia = aws ec2 describe-instances --filters "Name=tag:Name,Values=jupiter-api" `
    "Name=instance-state-name,Values=pending,running,stopping,stopped" `
    --query "Reservations[0].Instances[0].InstanceId" --output text
if ($Instancia -eq "None") { throw "No existe la EC2 jupiter-api: créala antes con 2_crear_ec2.ps1" }
Write-Host "EC2: $Instancia"

# --- 1. IP elástica ---------------------------------------------------------------------------
Paso "1. IP fija (IP elástica)"
# Cuesta lo mismo que la IP automática a la que sustituye (0,005 $/h), también con la EC2 apagada.
$Eip = aws ec2 describe-addresses --filters "Name=tag:Name,Values=jupiter-api" `
    --query "Addresses[0].[AllocationId,PublicIp,InstanceId]" --output text
if (-not $Eip -or $Eip -eq "None") {
    $Otras = aws ec2 describe-addresses --query "Addresses[].[PublicIp,InstanceId,Tags[?Key=='Name']|[0].Value]" --output text
    if ($Otras) { Write-Host "IP elásticas que ya hay en la cuenta (no se tocan):`n$Otras" }
    $Asignacion = aws ec2 allocate-address --domain vpc `
        --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=jupiter-api},{Key=Proyecto,Value=jupiter}]" `
        --query AllocationId --output text
    $Eip = aws ec2 describe-addresses --allocation-ids $Asignacion `
        --query "Addresses[0].[AllocationId,PublicIp,InstanceId]" --output text
    Write-Host "Creada"
}
$Asignacion, $IpFija, $Asociada = $Eip -split "\s+"
if ($Asociada -eq $Instancia) {
    Write-Host "$IpFija ya está asociada a la EC2"
} else {
    # Al asociarla, la EC2 pierde su IP automática: la web pasa a la fija en unos segundos.
    aws ec2 associate-address --allocation-id $Asignacion --instance-id $Instancia --query AssociationId --output text | Out-Null
    Write-Host "$IpFija asociada a la EC2"
}

# --- 2. Web sin contraseña --------------------------------------------------------------------
Paso "2. Quitar la contraseña de la web"
if (Existe { aws ssm get-parameter --name /jupiter/web/contrasena }) {
    aws ssm delete-parameter --name /jupiter/web/contrasena
    Write-Host "Borrado /jupiter/web/contrasena"
} else {
    Write-Host "No había contraseña"
}

# --- 3. Presupuesto de Bedrock ----------------------------------------------------------------
Paso "3. Presupuesto diario de Bedrock ($LimiteDia USD)"
# Nombre del servicio tal como lo factura AWS (Cost Explorer; cada consulta cuesta 0,01 $).
$Hoy = Get-Date
$Servicios = aws ce get-dimension-values --dimension SERVICE --search-string Bedrock `
    --time-period "Start=$($Hoy.AddDays(-30).ToString('yyyy-MM-dd')),End=$($Hoy.ToString('yyyy-MM-dd'))" `
    --query "DimensionValues[].Value" --output json | ConvertFrom-Json
if (-not $Servicios) { $Servicios = @("Amazon Bedrock") }
Write-Host "Servicios incluidos: $($Servicios -join ', ')"

$Presupuesto = @{
    BudgetName  = "jupiter-bedrock-diario"
    BudgetType  = "COST"
    TimeUnit    = "DAILY"
    BudgetLimit = @{ Amount = "$LimiteDia"; Unit = "USD" }
    CostFilters = @{ Service = @($Servicios) }
} | ConvertTo-Json -Depth 5 -Compress
$Avisos = @(80, 100) | ForEach-Object {
    @{
        Notification = @{ NotificationType = "ACTUAL"; ComparisonOperator = "GREATER_THAN"; Threshold = $_; ThresholdType = "PERCENTAGE" }
        Subscribers  = @(@{ SubscriptionType = "EMAIL"; Address = $Correo })
    }
} | ConvertTo-Json -Depth 5 -Compress -AsArray
$FPresupuesto = New-TemporaryFile
$FAvisos = New-TemporaryFile
Set-Content $FPresupuesto $Presupuesto -Encoding utf8NoBOM
Set-Content $FAvisos $Avisos -Encoding utf8NoBOM
if (Existe { aws budgets describe-budget --account-id $Cuenta --budget-name jupiter-bedrock-diario }) {
    # Actualiza límite y servicios; los avisos se quedan como se crearon.
    aws budgets update-budget --account-id $Cuenta --new-budget "file://$FPresupuesto"
    Write-Host "Presupuesto actualizado (los avisos no cambian)"
} else {
    aws budgets create-budget --account-id $Cuenta --budget "file://$FPresupuesto" `
        --notifications-with-subscribers "file://$FAvisos"
    Write-Host "Presupuesto creado: aviso a $Correo al pasar el 80 % y el 100 %"
}
Remove-Item $FPresupuesto, $FAvisos

Write-Host "`nListo. Falta «Desplegar → web» en GitHub Actions para quitar la contraseña de nginx." -ForegroundColor Green
Write-Host "Dirección fija de la web: http://$IpFija" -ForegroundColor Green
