# PrediMaint : volet Big Data

Mini-projet *Big Data Frameworks & NoSQL* : « Étude et évaluation d'une architecture Big Data
pour la maintenance prédictive des équipements industriels ».
Flux actuel : **simulateur → Kafka → Logstash → Elasticsearch → Kibana**, et Spark Structured
Streaming en lecture de Kafka. Stockage NoSQL en place : **Cassandra** (séries temporelles) et
**Redis** (seuils, dernières valeurs, alertes). Schéma et justification des choix : `docs/architecture.md`.

## Contenu

| Chemin | Rôle |
| --- | --- |
| `docker-compose.yaml` | Copie de travail du compose du cours (édition Lab 2), projet `predimaint` |
| `reference/docker-compose.cours-lab2.yaml` | Copie intacte du fichier du cours, pour comparaison |
| `scripts/demarrer.ps1` | Démarre la pile, crée l'index template, les topics, le schéma Cassandra et les seuils Redis (une commande) |
| `scripts/initialiser_stockage.ps1` | Applique le schéma Cassandra et charge les seuils dans Redis |
| `cassandra/schema.cql` | Keyspace `predimaint` et ses 4 tables, conçues à partir des requêtes |
| `redis/charger_seuils.py` | Charge seuils et machines dans Redis, depuis Odoo (`--odoo`) ou le fichier de référence |
| `redis/STRUCTURES.md` | Description des clés Redis |
| `scripts/creer_topics.ps1` | Crée `sensor.readings` (6), `maintenance.alerts` (3), `odoo.events` (3) |
| `scripts/creer_index_template.ps1` | Crée le template `predimaint-sensors` (mapping explicite) |
| `scripts/verifier.ps1` | Vérifie que chaque service répond |
| `elasticsearch/predimaint-sensors-template.json` | Index template (keyword, float, date) |
| `logstash/pipelines.yml` | Déclaration du pipeline `sensors` (file persistante + DLQ) |
| `logstash/pipeline/sensors.conf` | Pipeline Kafka → Elasticsearch |
| `simulator/simulateur.py` | Simulateur des 20 machines (identifiants Odoo) |
| `data_prep/preparer_donnees.py` | Prépare le jeu Kaggle *Azure Predictive Maintenance* pour nos 20 machines |
| `data/echantillon/` | Extrait de 48 h des données préparées (le jeu complet n'est pas redistribué) |
| `docs/donnees.md` | Description des données : source, licence, contrôle qualité, choix des machines |
| `spark/moyennes_streaming.py` | Job Spark Streaming : moyennes par machine, fenêtres d'une minute |
| `spark/predimaint_streaming_moyennes.ipynb` | Même traitement en notebook |

## Versions (identiques au cours, jamais « latest »)

Elasticsearch, Kibana et Logstash **8.17.0** · Kafka et ZooKeeper Confluent **7.5.0** ·
Kafka UI **v0.7.2** · Jupyter PySpark **spark-3.3.0** · Python **3.12** pour le simulateur ·
connecteur Spark **org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0**.
Ajouts du projet, hors fichier du cours : Cassandra **4.1.7** et Redis **7.4.1** (versions fixes).

## Adresses

| Service | Adresse |
| --- | --- |
| Kafka depuis Windows | `localhost:29092` |
| Kafka depuis un conteneur | `kafka:9092` |
| Kafka UI | http://localhost:8090 |
| Elasticsearch | http://localhost:9200 |
| Kibana | http://localhost:5601 |
| API Logstash | http://localhost:9600 |
| JupyterLab | http://localhost:8890 (token `bigdata-lab-2026`, celui du cours) |
| Spark UI | http://localhost:4040 (quand un job tourne) |
| Cassandra (CQL) | `localhost:9042` ; console : `docker compose exec cassandra cqlsh -k predimaint` |
| Redis | `localhost:6379` ; console : `docker compose exec redis redis-cli` |

Kafka UI et Jupyter ne sont pas sur 8080 et 8888 comme dans le cours : la VM VirtualBox
`mlops-vm` redirige déjà 8080, 8081 et 8888 sur ce PC.

## Déploiement pas à pas (PowerShell)

Prérequis : Docker Desktop démarré (moteur Linux), Python 3.12 installé (`py -3.12`).

### 1. Images (une seule fois)

```powershell
Set-Location "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data"
docker compose --profile complet --parallel 1 pull
```

### 2. Démarrer la pile, créer le template et les topics

Les conteneurs du cours (`bigdata-neuf-2026-*`) utilisent les mêmes ports : le script les
arrête s'ils tournent (sans rien supprimer).

```powershell
powershell -ExecutionPolicy Bypass -File "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data\scripts\demarrer.ps1"
```

Équivalent manuel :

```powershell
Set-Location "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data"
docker compose --profile pipeline up -d --pull never --wait --wait-timeout 600
powershell -ExecutionPolicy Bypass -File ".\scripts\creer_index_template.ps1"
powershell -ExecutionPolicy Bypass -File ".\scripts\creer_topics.ps1"
```

Le template doit exister **avant** le premier document indexé : on le crée avant de lancer
le simulateur.

### 3. Vérifier les services

```powershell
powershell -ExecutionPolicy Bypass -File "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data\scripts\verifier.ps1"
```

### 4. Simulateur (environnement virtuel, une seule fois)

```powershell
Set-Location "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data\simulator"
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Lancer le simulateur (terminal 1, laisser tourner) :

```powershell
Set-Location "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data\simulator"
.\.venv\Scripts\python.exe simulateur.py
```

Options :

| Option | Effet |
| --- | --- |
| `--rate 1000` | Débit total en messages/s (défaut 20 = 1 par machine et par seconde) |
| `--derive POMPE-02 COMP-01` | Usure lente : température et vibration montent de `--derive-pct` % par minute (défaut 1) |
| `--panne COMP-03 --type surchauffe` | Panne `surchauffe`, `vibration` ou `pression` (surpression), montée en `--montee` s (défaut 15) |
| `--reparer COMP-03` | Fin de la panne (`--reparer TOUT` pour toutes) |
| `--machines PRESSE-01,COMP-03` | Simule seulement ces machines |
| `--duree 60` | S'arrête après 60 s |
| `--sec` | Affiche les messages sans les envoyer (test hors Kafka) |

**Panne en direct** : dans un **second** terminal, pendant que le simulateur tourne :

```powershell
Set-Location "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data\simulator"
.\.venv\Scripts\python.exe simulateur.py --panne COMP-03 --type surchauffe
```

Le second processus détecte le simulateur en cours (fichier `.simulateur_actif` mis à jour chaque
seconde), lui transmet l'ordre par `commandes_pannes.json` et s'arrête. La panne apparaît dans les
données en moins d'une seconde.

### 5. Contrôler le flux

```powershell
# Compteur Elasticsearch (relancer : il augmente d'environ 20 par seconde)
curl.exe "http://localhost:9200/predimaint-sensors-*/_count"
# Index du jour
curl.exe "http://localhost:9200/_cat/indices/predimaint-*?v"
# Retard (LAG) du groupe Logstash dans Kafka
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group logstash-predimaint
# Statistiques du pipeline Logstash (événements in / out)
curl.exe "http://localhost:9600/_node/stats/pipelines/sensors?pretty"
```

Dans Kafka UI (http://localhost:8090) : Topics › `sensor.readings` › Messages.

### 6. Spark Structured Streaming (à la demande)

```powershell
Set-Location "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data"
docker compose --profile spark up -d --pull never ed-pyspark-jupyter
docker compose exec ed-pyspark-jupyter spark-submit --conf spark.jars.ivy=/home/jovyan/work/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /home/jovyan/work/moyennes_streaming.py
```

Un tableau par machine s'affiche toutes les 10 secondes (Ctrl+C pour arrêter). Le premier
lancement télécharge le connecteur Kafka (environ 56 Mo) dans `spark/.ivy2` ; les suivants le
réutilisent. Le notebook `predimaint_streaming_moyennes.ipynb` s'ouvre dans JupyterLab
(dossier `work`). Arrêter Jupyter ensuite pour libérer la mémoire :

```powershell
docker compose stop ed-pyspark-jupyter
```

### 7. Tableau de bord Kibana

Voir `docs/kibana_tableau_de_bord.md`.

## Arrêter et reprendre

```powershell
Set-Location "D:\3GL Alt\mini projet big data et erp\Mini Projet Big Data"
docker compose --profile complet stop
docker compose --profile pipeline up -d --pull never --wait --wait-timeout 600
```

Les données sont dans les volumes nommés `predimaint_*`. **Ne jamais utiliser
`docker compose down -v`** : `-v` supprime ces volumes (Kafka, Elasticsearch, Kibana, file
persistante de Logstash).

## Mémoire

Pile `pipeline` : environ 5 Go (Elasticsearch 1,5 Go, Cassandra 1 Go, Logstash 0,9 Go, Kibana 0,7 Go,
Kafka 0,5 Go, Kafka UI 0,3 Go, Redis quelques Mo). Jupyter/Spark ajoute 1 à 2 Go : le démarrer seulement
quand on en a besoin. La VM `mlops-vm` occupe à elle seule environ 8 Go : l'arrêter avant la
démo si la mémoire manque.
