# Journal de bord : Mini Projet Big Data

Matière Big Data Frameworks & NoSQL. Le journal du volet ERP est dans `../Mini Projet ERP/docs/journal_de_bord.md`.

## Séance S10 : 06/11/2026 · Premier flux Big Data de bout en bout

### Objectif de la séance

Montrer en direct : simulateur → Kafka → Logstash → Elasticsearch → Kibana, avec une panne
injectée qui apparaît dans les données. Cassandra et Redis : séance suivante.

### Ce qui est réalisé

| Élément | Détail |
| --- | --- |
| Environnement | Copie du compose du cours (édition Lab 2, avec Logstash) dans `Mini Projet Big Data/`, projet `predimaint`, versions identiques (ES/Kibana/Logstash 8.17.0, Kafka/ZooKeeper 7.5.0, Kafka UI 0.7.2, Spark 3.3.0). Copie intacte du fichier du cours dans `reference/`. |
| Vérification des services | `scripts/verifier.ps1` : Elasticsearch 8.17.0 (green, 1 nœud), Kibana 8.17.0 *available*, Logstash 8.17.0 (pipeline `sensors`), Kafka UI HTTP 200, Kafka 1 broker, ZooKeeper 3.6.4. |
| Topics | `scripts/creer_topics.ps1` : `sensor.readings` 6 partitions, `maintenance.alerts` 3, `odoo.events` 3, réplication 1. Relançable sans effet de bord (`--if-not-exists`). |
| Simulateur | `simulator/simulateur.py`, Python 3.12, venv, `confluent-kafka 2.6.1`. 20 machines PRESSE-01…POMPE-05, 1 message par machine et par seconde, clé = `machine_id`, valeur = nominal + cycle + bruit, dérive lente (`--derive`), débit (`--rate`), panne en direct depuis un 2e terminal (`--panne … --type surchauffe/vibration/pression`, `--reparer`). |
| Index template | `predimaint-sensors-*` : mapping explicite (keyword, float, date), 1 shard, 0 réplica, `dynamic: false`. |
| Logstash | Pipeline `sensors` : input kafka (codec json), `date` (ts → @timestamp), `mutate` (types), catégorie par conditionnelles, niveau normal/surveillance/critique d'après les seuils d'Odoo, `document_id` = topic-partition-offset. Déclaré dans `pipelines.yml` avec file persistante et DLQ. Syntaxe validée (`-t` : *Configuration OK*). |
| Spark | `spark/moyennes_streaming.py` et notebook : fenêtres d'une minute par machine, watermark 2 min, sortie console toutes les 10 s. Connecteur `org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0`. |
| Documentation | `README.md`, `docs/architecture.md`, `docs/kibana_tableau_de_bord.md`. |

### Résultats des vérifications

| Vérification | Résultat |
| --- | --- |
| Simulateur → Kafka | 20 msg/s, 0 erreur ; messages répartis sur les 6 partitions, une machine reste toujours dans la même partition (clé). |
| Kafka UI | Topic `sensor.readings` visible (6 partitions) ; groupe `logstash-predimaint` STABLE, 2 membres. |
| `_count` Elasticsearch | 10 140 → 10 260 → 10 360 en 12 s (environ 20 documents/s). |
| Retard Logstash (LAG) | 6 à 12 messages par partition, soit moins d'une seconde. |
| Rattrapage | Logstash démarré 7 minutes après le simulateur : les 9 880 messages en attente dans Kafka ont été indexés (aucune perte). |
| Panne COMP-03 surchauffe | Température max par tranche de 5 s : 80,0 → 96,3 → 116,2 → 130,4 → 131,0 °C ; niveau `surveillance` puis `critique` ; visible dans Elasticsearch en moins de 5 s. |
| Fausses alertes au régime normal | Aucune : seule COMP-03 dépasse un seuil sur 15 minutes (requête en 4 ms). |
| Spark Streaming | 11 micro-lots en 100 s, 20 lignes par fenêtre (41 mesures par machine), 0 erreur. |

### Difficultés rencontrées

1. **Logstash absent du compose du cours** trouvé dans `3frameworksbigdata` : l'édition Lab 1 ne
   contient pas Logstash (« non inclus dans cette édition »).
2. **Image `logstash:8.17.0` absente** du PC (environ 1,5 Go à télécharger).
3. **Ports occupés** : 8080 (Kafka UI), puis 8081, puis 8888 (Jupyter) refusés par Docker
   (« Only one usage of each socket address »).
4. **Mémoire** : il ne restait qu'environ 1 Go libre sous Windows avec la pile démarrée.
5. **Cache du connecteur Spark** : le premier `spark-submit` a dépassé 100 s pendant le
   téléchargement de `hadoop-client-runtime`. Le volume prévu pour le cache n'était pas utilisé :
   `docker exec` tourne en root (`/root/.ivy2`), le notebook en `jovyan`.
6. **Avertissement Kafka** sur les noms de topics contenant un point.

### Solutions testées

- Difficulté 1 : recherche dans les dossiers de cours ; le compose avec Logstash 8.17.0 est dans
  `big data framework/Lab2_Logstash_complet.zip`.
- Difficulté 3 : passage de Kafka UI sur 8081 → encore refusé ; `netstat` montrait 8081 libre
  et ce n'était pas une plage réservée par Windows (`netsh … excludedportrange`). Les règles de
  redirection de la VM VirtualBox `mlops-vm` (`VBoxManage showvminfo`) ont donné la vraie liste :
  3000, 5000, 8080, 8081, 8333, 8443, 8888, 9090, 9093, 30080, 2222.
- Difficulté 5 : volume nommé monté sur `/home/jovyan/.ivy2` → écarté (droits root/jovyan
  différents sur un volume nommé).

### Corrections retenues

| Problème | Correction | Justification |
| --- | --- | --- |
| 1 | Base de travail = compose du Lab 2, copié dans `Mini Projet Big Data/` (d'abord `bigdata/`) | C'est le fichier du cours qui contient Logstash 8.17.0 ; le fichier d'origine n'est pas modifié. |
| Mélange avec les labs | `name: predimaint` | Volumes séparés : les données des labs ne sont ni lues ni modifiées. |
| 2 | `docker pull docker.elastic.co/logstash/logstash:8.17.0` | Même version que le cours. |
| 3 | Kafka UI sur **8090**, Jupyter sur **8890** | Ports hors de la liste de la VM ; on ne dépend pas de l'arrêt de la VM le jour de la démo. |
| 4 | Profils `pipeline` (démo) et `spark` (à la demande) ; Jupyter arrêté après les tests | Environ 4 Go pour la démo, Spark seulement si nécessaire. |
| 5 | `spark.jars.ivy=/home/jovyan/work/.ivy2` (dossier du projet, exclu de Git) | Cache commun à root et jovyan, conservé si le conteneur est recréé ; 2e lancement sans téléchargement. |
| 6 | Noms conservés (`sensor.readings`…) | Fixés par le cahier des charges ; aucune collision possible, car aucun topic n'utilise `_`. |
| Création automatique de topics | `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` | Un producteur lancé trop tôt ne crée pas un topic à 1 partition par erreur. |
| Doublons après une reprise | `document_id` = topic-partition-offset | Relire un message réécrit le même document (idempotence). |

### Choix à justifier devant l'enseignante

- **Kafka entre le simulateur et les traitements** : découplage, relecture après panne,
  plusieurs consommateurs indépendants (Logstash et Spark ont chacun leur groupe).
- **Clé = machine_id** : ordre garanti des mesures d'une machine, répartition sur 6 partitions.
- **Logstash plutôt qu'un script maison** : configuration déclarative, file persistante, DLQ,
  API de supervision (port 9600), vus au Lab 2.
- **Mapping explicite avant l'indexation** : sans template, `machine_id` serait `text` +
  `keyword` et les dates pourraient être mal détectées (Lab 1).
- **Un index par jour, 1 shard, 0 réplica** : un seul nœud (un réplica serait non attribué,
  statut yellow) ; suppression simple des jours anciens.
- **Seuils recopiés dans Logstash** : provisoire, pour colorer le tableau de bord ; ils
  viendront de Redis, synchronisé depuis Odoo, à la séance suivante.
- **Panne « pression » = surpression** : les seuils d'Odoo sont des maxima. Une chute de pression
  (cavitation d'une pompe) demandera un seuil bas dans le modèle `predimaint.sensor.threshold`.

### Reste à faire

- Cassandra (`readings_by_machine`, `aggregates_by_machine`) et Redis (`last:<machine>`, seuils, alertes actives).
- Détection d'anomalies dans Spark (seuils + 3 écarts-types), publication dans `maintenance.alerts`, checkpoint.
- Connecteur `maintenance.alerts` → Odoo (`predimaint.alert.create_from_api`), pipeline Logstash `alerts`.
- Cluster Kafka de 3 brokers (réplication 3) et tests de performance et de panne (sections 10.2 et 10.3).

---

## Séance S10 (suite) : 07/11/2026 · Jeu de données public

### Ce qui est réalisé

- Recherche et téléchargement sur Kaggle du jeu **Microsoft Azure Predictive Maintenance** (zip de 31 Mo, 5 CSV, 80,4 Mo décompressés), prévu par le cahier des charges (section 8.3).
- Exploration et contrôle qualité : 876 100 mesures horaires, 100 machines, année 2015 complète ; 0 valeur manquante, 0 doublon, 0 trou dans la série ; 761 pannes sur 98 machines.
- Script `data_prep/preparer_donnees.py` (bibliothèque standard Python, sans dépendance) : choix de 20 machines, renommage vers les identifiants Odoo, dates ISO 8601 UTC.
- Résultat : 175 220 mesures, 243 pannes, 671 interventions et 878 erreurs pour nos 20 machines ; extrait de 48 h dans `data/echantillon/`.
- Fiche descriptive `docs/donnees.md`.

### Difficultés et corrections retenues

| Difficulté | Correction | Justification |
| --- | --- | --- |
| Téléchargement Kaggle impossible sans compte | Connexion faite par l'étudiant, puis téléchargement | Kaggle exige un compte pour télécharger. |
| Licence « Unknown » sur Kaggle | Données brutes et préparées exclues du dépôt ; seul un extrait de 48 h et le script sont joints | On ne redistribue pas des données dont la licence n'est pas connue ; le script permet de tout régénérer. |
| 100 machines dans le jeu, 20 dans le projet | 4 modèles = 4 catégories ; pour chaque modèle, les 5 machines qui ont le plus de pannes | Correspondance simple à expliquer ; historique le plus riche pour le MTBF. |
| Grandeurs différentes du simulateur | Grandeurs du jeu conservées (tension, rotation, pression, vibration) | Les convertir en température ou en puissance serait inventer des données. |
| Données synthétiques | Signalé dans `docs/donnees.md` et à dire en soutenance | Elles servent à construire la chaîne, pas à conclure sur une vraie usine. |
