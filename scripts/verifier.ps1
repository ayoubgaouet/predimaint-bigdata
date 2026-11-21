# Vérifie que chaque service répond. Usage : & ".\scripts\verifier.ps1"
$compose = Join-Path $PSScriptRoot "..\docker-compose.yaml"

function Test-Url($nom, $url) {
    try {
        $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 5
        Write-Host ("[OK]  {0,-14} {1} (HTTP {2})" -f $nom, $url, $r.StatusCode)
    } catch {
        Write-Host ("[KO]  {0,-14} {1} : {2}" -f $nom, $url, $_.Exception.Message)
    }
}

$es = Invoke-RestMethod "http://localhost:9200" -ErrorAction SilentlyContinue
if ($es) { Write-Host "[OK]  Elasticsearch  version $($es.version.number)" }
$h = Invoke-RestMethod "http://localhost:9200/_cluster/health" -ErrorAction SilentlyContinue
if ($h) { Write-Host "      cluster $($h.cluster_name) : statut $($h.status), $($h.number_of_nodes) nœud(s)" }
$kb = Invoke-RestMethod "http://localhost:5601/api/status" -ErrorAction SilentlyContinue
if ($kb) { Write-Host "[OK]  Kibana         version $($kb.version.number), état $($kb.status.overall.level)" }
$ls = Invoke-RestMethod "http://localhost:9600/_node/pipelines" -ErrorAction SilentlyContinue
if ($ls) { Write-Host "[OK]  Logstash       version $($ls.version), pipelines : $(($ls.pipelines.PSObject.Properties.Name) -join ', ')" }
Test-Url "Kafka UI" "http://localhost:8090/actuator/health"

$brokers = docker compose -f "$compose" exec -T kafka kafka-broker-api-versions --bootstrap-server kafka:9092 2>$null | Select-String "id: "
if ($brokers) { Write-Host "[OK]  Kafka          $($brokers.Count) broker(s) : $($brokers -join ' ')" } else { Write-Host "[KO]  Kafka" }
$zk = docker compose -f "$compose" exec -T zookeeper bash -c "echo srvr | nc -w 2 localhost 2181 | head -1" 2>$null
if ($zk) { Write-Host "[OK]  ZooKeeper      $zk" } else { Write-Host "[--]  ZooKeeper      (vérifié indirectement par Kafka)" }
