#### ---- LIBRAIRIES

from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as func
from pyspark.sql.window import Window
import argparse
from datetime import datetime, timedelta
import pandas as pd
import re

#### ---- PARAMETRES

# Command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--DATE_START', help = "Date de départ", type = str, default = "2022-09-01 15:00:00")
parser.add_argument('--DATE_END', help = "Date de fin", type = str, default = "2022-09-02 23:00:00")
parser.add_argument('--DESTINATION', help = "Adresse de sortie", type = str, default = "hdfs:///user/hdfs/out_table_{}_{}.csv")

parser.add_argument('--SOURCE_TRANSAC', help = "Adresse source pour le fichier des transactions", type = str, default= "hdfs:///user/hdfs/transactions-large.csv")
parser.add_argument('--SOURCE_LAUND', help = "Adresse source pour le fichier des suspicions de fraude", type = str, default= "/tmp/laundering-large.txt")

args = parser.parse_args("")
# args = parser.parse_args() 

DATE_START = datetime.strptime(args.DATE_START,"%Y-%m-%d %H:%M:%S")
DATE_END =  datetime.strptime(args.DATE_END,"%Y-%m-%d %H:%M:%S")
DESTINATION = args.DESTINATION.format(DATE_START.strftime("%Y%m%d"),DATE_END.strftime("%Y%m%d"))

# Initialisation du sparkContext
spark = SparkSession \
    .builder \
    .appName("PySpark") \
    .getOrCreate()

sc = spark.sparkContext

sql_c = SQLContext(spark.sparkContext)

# Pour éviter le bug de conversion de date
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Pour expliciter les colonnes
col_names = ["Timestamp","From Bank","Account2","To Bank","Account4",
             "Amount Received","Receiving Currency","Amount Paid",
             "Payment Currency","Payment Format"
             ]

#### ---- TRAITEMENTS

# Vérifier que DATE_START < DATE_END
if DATE_START >= DATE_END:
    raise ValueError("DATE_END doit être plus ancien que DATE_START.")

# Lire les transactions
data = sql_c.read.option("header", False).csv(args.SOURCE_TRANSAC)
data = data.toDF(*col_names)

# Lire et formater les suspicions de fraude

# ... lire le fichier texte
with open(args.SOURCE_LAUND, 'r') as file:
    file_content = file.read()

# ... séparer les différentes sections de tentatives de blanchiment d'argent
attempts = re.split(r'BEGIN LAUNDERING ATTEMPT - .+\n', file_content)
attempts = [attempt.strip() for attempt in attempts if attempt.strip()]  # Supprimer les chaînes vides

# ... initialiser la liste pour stocker toutes les transactions frauduleuses
frauds = []

# ... lire les transactions frauduleuses de chaque section
for attempt in attempts:
    for line in attempt.split('\n'):
        if not line.startswith('END LAUNDERING ATTEMPT'):
            frauds.append(line.split(','))

# ... formater dans un dataframe
frauds = pd.DataFrame(frauds, columns = [*col_names,"status"])

# ... agréger la table des transactions frauduleuses à la maille (compte x devis)
frauds.drop_duplicates(["Account2","Receiving Currency"])
frauds = frauds[["Account2","Receiving Currency"]]
frauds["has_laundering"] = True
frauds.head()

# ... convertir frauds en spark dataframe
frauds = sql_c.createDataFrame(frauds)

# ... convertir certaines variables
data = data \
    .withColumn("Timestamp", func.to_timestamp("Timestamp","yyyy/MM/dd HH:mm")) \
    .withColumn("Amount Received", func.col("Amount Received").cast("double")) \
    .withColumn("Amount Paid", func.col("Amount Paid").cast("double"))

# Filtrer sur les dates
data = data \
    .filter(
        (func.col("Timestamp") >= func.lit(DATE_START)) & (func.col("Timestamp") <= func.lit(DATE_END))
    )

# Créer une fenêtre temporelle sur le maillage (compte x devise)
windowSpec = Window.partitionBy("Account2","Receiving Currency").orderBy("Timestamp")

# Calculer le délai entre les transactions à la maille (compte x devise)
data = data \
    .withColumn("previous_Timestamp",
                func.lag("Timestamp").over(windowSpec)
                ) \
    .withColumn("delay_transaction",
                func.when(
                    func.col("previous_Timestamp").isNotNull(),
                    func.col("Timestamp").cast("long") - func.col("previous_Timestamp").cast("long")
                    ) \
                    .otherwise(None)
                )

# Calculer le montant total reçu pour chaque maille (compte x devise)
data_receiver = data \
    .groupby("Account4","Payment Currency") \
    .agg(
        func.sum("Amount Paid").alias("received")
    )

# Calculer, à la maille (compte x devise), le nombre de transactions émises, le délai moyen entre celles-ci, et le montant total émis
data_sender = data \
    .groupby("Account2","Receiving Currency") \
    .agg(
        func.count("*").alias("num_transactions"),
        func.round(func.avg("delay_transaction")).cast("int").alias("avg_delay_transactions"),
        func.sum("Amount Received").alias("withdrawals")
    )

# Libérer de la mémoire
data = None

# Joindre toutes les colonnes dans une même table de sortie
out_table = data_sender \
    .join(
        data_receiver,
        (data_sender["Account2"] == data_receiver["Account4"]) & \
            (data_sender["Receiving Currency"] == data_receiver["Payment Currency"]),
        "left"
        ) \
    .join(
        frauds,
        ["Account2","Receiving Currency"],
        "left"
    ) \
    .fillna(False,["has_laundering"]) \
    .drop(*["Account4","Payment Currency"])

# Libérer mémoire
data_receiver, data_sender, frauds= None, None, None

# Exporter la table
out_table.write.csv(DESTINATION,header=True, mode="overwrite")

