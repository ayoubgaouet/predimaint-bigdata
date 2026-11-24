-- =====================================================================
-- Schéma Cassandra du projet PrediMaint (cahier des charges, section 7.5)
-- Modélisation dirigée par les requêtes : une table = une question.
-- Appliqué par scripts/initialiser_stockage.ps1 (idempotent : IF NOT EXISTS).
-- =====================================================================

-- Réplication 1 : un seul nœud pour l'instant. Avec 3 nœuds, on passera à
-- NetworkTopologyStrategy, réplication 3, et le niveau de cohérence QUORUM.
CREATE KEYSPACE IF NOT EXISTS predimaint
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE predimaint;

-- Q1 : « Toutes les mesures d'une machine pour une journée, les plus récentes d'abord. »
-- Clé de partition (machine_id, day) : une partition = une machine x un jour, soit au plus
-- 86 400 lignes (1 mesure/s). Elle ne grossit donc pas sans fin.
-- Les deux sources partagent la table : le simulateur (temperature, puissance_kw, bruit_db)
-- et l'historique Azure PdM (volt, rotate) ; une colonne vide ne coûte rien dans Cassandra.
CREATE TABLE IF NOT EXISTS readings_by_machine (
  machine_id   text,
  day          date,
  ts           timestamp,
  source       text,      -- 'simulateur' ou 'azure_pdm'
  temperature  float,
  vibration    float,
  pression     float,
  puissance_kw float,
  bruit_db     float,
  volt         float,
  rotate       float,
  PRIMARY KEY ((machine_id, day), ts)
) WITH CLUSTERING ORDER BY (ts DESC);

-- Q2 : « Les agrégats minute par minute d'une machine sur une journée. »
-- Écrite par Spark Structured Streaming (fenêtres d'une minute).
CREATE TABLE IF NOT EXISTS aggregates_by_machine (
  machine_id       text,
  day              date,
  minute           timestamp,
  nb               int,
  temperature_avg  float,
  temperature_max  float,
  vibration_avg    float,
  vibration_max    float,
  pression_avg     float,
  puissance_kw_avg float,
  bruit_db_avg     float,
  PRIMARY KEY ((machine_id, day), minute)
) WITH CLUSTERING ORDER BY (minute DESC);

-- Q3 : « L'état de santé de toutes les machines pour un jour donné. »
-- Écrite chaque nuit par Spark batch (score, MTBF, disponibilité).
CREATE TABLE IF NOT EXISTS health_by_day (
  day             date,
  machine_id      text,
  health_score    float,
  mtbf_hours      float,
  availability    float,
  nb_alerts       int,
  nb_failures     int,
  PRIMARY KEY ((day), machine_id)
);

-- Q4 : « L'historique des pannes, interventions et erreurs d'une machine. »
-- Nécessaire au calcul du MTBF (temps entre deux pannes) par Spark batch.
-- Partition par machine : environ 100 événements par machine et par an, volume modeste.
CREATE TABLE IF NOT EXISTS events_by_machine (
  machine_id   text,
  ts           timestamp,
  event_type   text,      -- 'panne', 'maintenance' ou 'erreur'
  detail       text,      -- composant (comp1..comp4) ou code d'erreur (error1..error5)
  source       text,
  PRIMARY KEY ((machine_id), ts, event_type, detail)
) WITH CLUSTERING ORDER BY (ts DESC, event_type ASC, detail ASC);
