# Crée les topics Kafka du projet (réutilisable : --if-not-exists ne recrée pas un topic existant).
# Réplication 1 pour l'instant : un seul broker. Passera à 3 avec le cluster de 3 brokers.
# Usage (PowerShell, depuis n'importe quel dossier) :
#   & "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data\scripts\creer_topics.ps1"

$ErrorActionPreference = "Stop"
$compose = Join-Path $PSScriptRoot "..\docker-compose.yaml"

$topics = @(
    @{ nom = "sensor.readings";    partitions = 6 },  # 20 machines, clé = machine_id
    @{ nom = "maintenance.alerts"; partitions = 3 },
    @{ nom = "odoo.events";        partitions = 3 }
)
$replication = 1

foreach ($t in $topics) {
    Write-Host "Topic $($t.nom) : $($t.partitions) partitions, réplication $replication"
    docker compose -f "$compose" exec -T kafka kafka-topics --bootstrap-server kafka:9092 `
        --create --if-not-exists --topic $t.nom `
        --partitions $t.partitions --replication-factor $replication
    if ($LASTEXITCODE -ne 0) { throw "Échec de création du topic $($t.nom)" }
}

Write-Host "`nÉtat des topics :"
docker compose -f "$compose" exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe `
    --topic sensor.readings
docker compose -f "$compose" exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe `
    --topic maintenance.alerts
docker compose -f "$compose" exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe `
    --topic odoo.events
