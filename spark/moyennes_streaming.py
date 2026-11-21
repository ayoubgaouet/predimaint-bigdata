# -*- coding: utf-8 -*-
"""Spark Structured Streaming : moyennes par machine sur des fenêtres d'une minute.

Lit le topic Kafka sensor.readings et affiche dans la console, toutes les 10 secondes,
les moyennes de chaque grandeur par machine et par fenêtre d'une minute.

Lancement (PowerShell, dossier Mini Projet Big Data) :
    docker compose --profile spark up -d --pull never ed-pyspark-jupyter
    docker compose exec ed-pyspark-jupyter spark-submit --conf spark.jars.ivy=/home/jovyan/work/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 /home/jovyan/work/moyennes_streaming.py

Le connecteur Kafka n'est pas inclus dans Spark : --packages le télécharge depuis Maven
(une seule fois, il reste ensuite dans le dossier spark/.ivy2 du mini-projet).
La version du paquet doit être celle de Spark (3.3.0) et de Scala (2.12).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA = "kafka:9092"   # adresse interne au réseau Docker
TOPIC = "sensor.readings"

spark = (SparkSession.builder
         .appName("predimaint-moyennes-1min")
         .config("spark.sql.shuffle.partitions", "6")   # = nombre de partitions du topic
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Schéma explicite du message JSON (section 8.1 du cahier des charges).
schema = StructType([
    StructField("machine_id", StringType()),
    StructField("ts", StringType()),
    StructField("temperature", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("pression", DoubleType()),
    StructField("puissance_kw", DoubleType()),
    StructField("bruit_db", DoubleType()),
])

brut = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

mesures = (brut
           .select(F.from_json(F.col("value").cast("string"), schema).alias("m"))
           .select("m.*")
           .withColumn("event_time", F.col("ts").cast("timestamp")))   # temps de l'événement

moyennes = (mesures
            # Watermark : on accepte des mesures en retard jusqu'à 2 minutes,
            # au-delà la fenêtre est close et son état libéré en mémoire.
            .withWatermark("event_time", "2 minutes")
            .groupBy(F.window("event_time", "1 minute").alias("fenetre"), "machine_id")
            .agg(F.count("*").alias("nb"),
                 F.round(F.avg("temperature"), 1).alias("temp_moy"),
                 F.round(F.max("temperature"), 1).alias("temp_max"),
                 F.round(F.avg("vibration"), 2).alias("vib_moy"),
                 F.round(F.avg("pression"), 2).alias("pres_moy"),
                 F.round(F.avg("puissance_kw"), 1).alias("kw_moy"),
                 F.round(F.avg("bruit_db"), 1).alias("db_moy"))
            .select(F.date_format("fenetre.start", "HH:mm").alias("debut"),
                    F.date_format("fenetre.end", "HH:mm").alias("fin"),
                    "machine_id", "nb", "temp_moy", "temp_max", "vib_moy",
                    "pres_moy", "kw_moy", "db_moy"))

requete = (moyennes.writeStream
           # update : seules les fenêtres modifiées par le micro-lot sont affichées (la minute
           # en cours) ; grâce au watermark, l'état des fenêtres closes est libéré.
           .outputMode("update")
           .format("console")
           .option("truncate", "false")
           .option("numRows", 50)
           .trigger(processingTime="10 seconds")
           .start())
requete.awaitTermination()
