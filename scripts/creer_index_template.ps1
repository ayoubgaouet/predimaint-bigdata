# Crée (ou met à jour) l'index template predimaint-sensors dans Elasticsearch.
# À exécuter AVANT le premier document indexé : le mapping ne s'applique qu'aux index créés après.
# Équivalent Kibana Dev Tools : PUT _index_template/predimaint-sensors  + contenu du fichier JSON.

$ErrorActionPreference = "Stop"
$fichier = Join-Path $PSScriptRoot "..\elasticsearch\predimaint-sensors-template.json"
$corps = [IO.File]::ReadAllText((Resolve-Path $fichier))

$r = Invoke-RestMethod -Method Put -Uri "http://localhost:9200/_index_template/predimaint-sensors" `
        -ContentType "application/json; charset=utf-8" -Body ([Text.Encoding]::UTF8.GetBytes($corps))
Write-Host "Index template predimaint-sensors : acknowledged = $($r.acknowledged)"
