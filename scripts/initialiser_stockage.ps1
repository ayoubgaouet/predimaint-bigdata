# Initialise le stockage NoSQL : schéma Cassandra, puis seuils et machines dans Redis.
# Relançable sans risque (IF NOT EXISTS côté Cassandra, réécriture des clés côté Redis).
# Usage : & ".\scripts\initialiser_stockage.ps1"          (seuils du fichier de référence)
#         & ".\scripts\initialiser_stockage.ps1" -Odoo    (seuils lus dans Odoo par XML-RPC)
param([switch]$Odoo)

$ErrorActionPreference = "Stop"
$projet = Split-Path $PSScriptRoot -Parent
$compose = Join-Path $projet "docker-compose.yaml"
$python = Join-Path $projet "simulator\.venv\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "py" }

# 1. Cassandra : attendre qu'elle accepte les requêtes (son démarrage prend environ une minute).
Write-Host "Cassandra : attente de la disponibilité..."
$pret = $false
for ($i = 0; $i -lt 40; $i++) {
    docker compose -f "$compose" exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES" 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $pret = $true; break }
    Start-Sleep 5
}
if (-not $pret) { throw "Cassandra ne répond pas : docker compose logs --tail=50 cassandra" }

# 2. Schéma (keyspace predimaint et ses 4 tables).
docker compose -f "$compose" exec -T cassandra cqlsh -f /schema/schema.cql
if ($LASTEXITCODE -ne 0) { throw "Échec du schéma Cassandra" }
$tables = docker compose -f "$compose" exec -T cassandra cqlsh -e "DESCRIBE TABLES" -k predimaint
Write-Host "Cassandra : keyspace predimaint, tables : $((($tables -join ' ') -split '\s+' | Where-Object { $_ }) -join ', ')"

# 3. Redis : seuils et fiche des machines.
$argsSeuils = @("-X", "utf8", (Join-Path $projet "redis\charger_seuils.py"))
if ($Odoo) { $argsSeuils += "--odoo" }
& $python @argsSeuils
if ($LASTEXITCODE -ne 0) { throw "Échec du chargement des seuils dans Redis" }
