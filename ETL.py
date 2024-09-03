from docutils.nodes import header
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, regexp_replace, split, round

spark = SparkSession.builder.appName("ETL").enableHiveSupport().getOrCreate()

# -----------------   write data from local, into HDFS   -----------------------------------

# Define the local file path and the HDFS path
local_athlete = "/home/prasad/Desktop/BigData-Project/Mini-Project/data/athlete_events.csv"
hdfs_athlete = "hdfs://localhost:9000/user/prasad/Mini-project/data/athlete_events"

local_winners = "/home/prasad/Desktop/BigData-Project/Mini-Project/data/Winners.csv"
hdfs_winners = "hdfs://localhost:9000/user/prasad/Mini-project/data/Winners"

local_noc = "/home/prasad/Desktop/BigData-Project/Mini-Project/data/noc_regions.csv"
hdfs_noc = "hdfs://localhost:9000/user/prasad/Mini-project/data/noc"

athlete_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Age", FloatType(), True),
    StructField("Height", FloatType(), True),
    StructField("Weight", FloatType(), True),
    StructField("Team", StringType(), True),
    StructField("Games", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Sport", StringType(), True),
    StructField("Event", StringType(), True),
])

winners_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Team", StringType(), True),
    StructField("Games", StringType (), True),
    StructField("Sport", StringType(), True),
    StructField("Event", StringType(), True),
    StructField("Medal", StringType(), True)
])

noc_schema = StructType([
    StructField("NOC", StringType(), True),
    StructField("region", StringType(), True),
    StructField("notes", StringType(), True)
])


# Reading the CSV file from the local filesystem
localdf_athlete = spark.read.csv(local_athlete, header=True, schema=athlete_schema)
local_df_winners = spark.read.csv(local_winners, header=True, schema=winners_schema)
local_df_noc = spark.read.csv(local_noc, header=True, schema=noc_schema)


# Coalesce to a single file
localdf_athlete = localdf_athlete.coalesce(1)
local_df_winners = local_df_winners.coalesce(1)
local_df_noc = local_df_noc.coalesce(1)


# Write to HDFS
localdf_athlete.write.mode('overwrite').parquet(hdfs_athlete)
local_df_winners.write.mode('overwrite').parquet(hdfs_winners)
local_df_noc.write.mode('overwrite').parquet(hdfs_noc)

# -----------------   reading data from HDFS    --------------------------------

# Read the Parquet files from HDFS
df_athlete = spark.read.schema(athlete_schema).parquet(hdfs_athlete)
df_winners = spark.read.schema(winners_schema).parquet(hdfs_winners)
df_noc = spark.read.schema(noc_schema).parquet(hdfs_noc)

# -----------------   splitting athlete    ---------------------------------

## splitting columns
df_athlete = df_athlete.withColumn('Year', split(df_athlete['Games'], ' ').getItem(0)) \
    .withColumn('Season', split(df_athlete['Games'], ' ').getItem(1))

# -----------------   formatting athlete into km, m, etc    ---------------------------------

# Replacement rules
replacements = {
    'kilometres': 'km',
    'kilometers': 'km',
    'metres': 'm',
    'meters': 'm'
}

for old, new in replacements.items():
    df_athlete = df_athlete.withColumn("Event", regexp_replace(col("Event"), old, new))

# -----------------   Calculate BMI    ---------------------------------

df_athlete = df_athlete.withColumn("BMI", round(col("Weight") / (col("Height") / 100) ** 2, 2))
df_athlete = df_athlete.withColumn("BMI", round(col("Weight") / (col("Height") / 100) ** 2, 2).cast(FloatType()))

# -----------------   Join athlete_events with winners    ---------------------------------

# Perform an inner join on 'ID' and 'Event' to combine athlete and winner data
df_selected = df_athlete.join(df_winners, ["ID"], "inner") \
    .select(
        df_athlete.ID,
        df_athlete.Name,
        df_winners.Team,
        df_winners.Games,
        df_winners.Sport,
        df_winners.Medal
    ).show(truncate=False)

# -----------------   Joining athlete_events with noc    ---------------------------------

df_joined_noc = df_athlete.join(df_noc, df_athlete.Team == df_noc.region, "inner") \
    .select(df_athlete.ID, df_athlete.Name, df_athlete.Sport, df_noc.NOC, df_noc.region).show(truncate=False)


# -----------------  INTO HIVE    ---------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS project")

spark.sql("USE project")

spark.sql("DROP TABLE IF EXISTS athlete_events")
spark.sql("DROP TABLE IF EXISTS winners")
spark.sql("DROP TABLE IF EXISTS noc")

# ORC table
df_athlete.write.mode('overwrite').format("orc").saveAsTable("athlete_events")
df_winners.write.mode('overwrite').format("orc").saveAsTable("winners")
df_noc.write.mode('overwrite').format("orc").saveAsTable("noc")



spark.sql("SELECT * FROM athlete_events").show(truncate=False)
spark.sql("SELECT * FROM winners").show(truncate=False)
spark.sql("SELECT * FROM noc").show(truncate=False)




