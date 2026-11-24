# Démarre la pile de la démo en une commande : Kafka, Kafka UI, Elasticsearch, Kibana, Logstash,
# Cassandra et Redis, puis crée les topics, l'index template, le schéma Cassandra et les seuils Redis
# (opérations idempotentes, sans risque si déjà faites).
# Ajouter -Spark pour démarrer aussi Jupyter/Spark.
param([switch]$Spark)

$ErrorActionPreference = "Stop"
$compose = Join-Path $PSScriptRoot "..\docker-compose.yaml"

# Les conteneurs du cours utilisent les mêmes ports : on les arrête s'ils tournent (sans rien supprimer).
$cours = docker ps --filter "label=com.docker.compose.project=bigdata-neuf-2026" --format "{{.Names}}"
if ($cours) {
    Write-Host "Arrêt des conteneurs du cours : $($cours -join ', ')"
    docker stop $cours | Out-Null
}

$profils = @("--profile", "pipeline")
if ($Spark) { $profils += @("--profile", "spark") }
docker compose -f "$compose" @profils up -d --pull never --wait --wait-timeout 600
if ($LASTEXITCODE -ne 0) { throw "Démarrage incomplet : docker compose -f `"$compose`" ps -a" }

& (Join-Path $PSScriptRoot "creer_index_template.ps1")
& (Join-Path $PSScriptRoot "creer_topics.ps1")
& (Join-Path $PSScriptRoot "initialiser_stockage.ps1")
docker compose -f "$compose" ps --format "table {{.Service}}\t{{.Status}}"
