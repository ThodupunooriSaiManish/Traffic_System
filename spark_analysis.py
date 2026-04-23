from pyspark.sql import SparkSession

# ✅ Create Spark session (safe way)
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Traffic Analysis") \
    .getOrCreate()

# Read JSON file
df = spark.read.json("data/traffic.json")

print("🔥 RAW DATA")
df.show()

print("📊 AVG VEHICLES")
df.groupBy("location").avg("vehicles").show()

print("📈 MAX VEHICLES")
df.groupBy("location").max("vehicles").show()

spark.stop()