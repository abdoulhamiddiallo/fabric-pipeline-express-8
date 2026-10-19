# SJD_Lisboa : trois formats, trois unités, un seul jeu de trajets, puis le verdict par quartier
import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("SJD_Lisboa").getOrCreate()
run_date = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1][:2] == "20" else "2026-10-19"  # marche aussi en notebook
elec = F.avg(F.when(F.col("bike_type") == "electrique", 1).otherwise(0)) * 100

def unify(table, duration, distance, to_min, to_km):  # tout en minutes et en kilomètres
    return spark.read.table(table).select(
        F.col("trip_id").cast("string").alias("trip_id"), F.col("station").cast("string").alias("station"),
        F.col("district").cast("string").alias("district"), F.to_timestamp(F.col("start_time").cast("string")).alias("start_time"),
        (F.col(duration).cast("double") / to_min).alias("duration_min"), (F.col(distance).cast("double") / to_km).alias("distance_km"),
        F.col("bike_type").cast("string").alias("bike_type"))

raw = (unify("trips_csv", "duration_min", "distance_km", 1, 1)
       .unionByName(unify("trips_json", "duration_s", "distance_m", 60, 1000))
       .unionByName(unify("trips_parquet", "duration_min", "distance_km", 1, 1)))
rows_in = raw.count()
trips = raw.dropDuplicates(["trip_id"])
n = trips.count()
trips.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("trips")

stats = (trips.groupBy("district").agg(F.count("*").alias("trips"), F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
         F.round(F.sum("distance_km"), 1).alias("km_total"), F.round(elec, 1).alias("electric_pct")).orderBy(F.desc("trips")))
stats.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("district_stats")

top = stats.first()
tot = trips.agg(F.round(F.sum("distance_km"), 1).alias("km"), F.round(elec, 1).alias("pct")).first()
spark.createDataFrame(
    [(run_date, 3, rows_in, n, rows_in - n, top["district"], float(tot["km"]), float(tot["pct"]))],
    "run_date string, formats int, rows_in int, trips int, duplicates int, top_district string, km_total double, electric_pct double",
).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("run_summary")
print(f"SJD_Lisboa : {rows_in} lignes lues, {n} trajets, {rows_in - n} doublons écartés")
