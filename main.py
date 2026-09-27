"""Assignment 3: Trip Analytics with Spark.

Usage (on the Docker cluster):
    spark-submit main.py <input directory> <output directory>

Reads trips.csv and zones.csv from the input directory. Writes the cleaned trips as Parquet
to <output>/clean_trips and one CSV result per part to <output>/part1, part2a, part2b, part3.

Fill in the four functions marked TODO. Each returns a DataFrame; a function that still
returns None is skipped, so the file runs after every step.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, sum as ssum, avg, round as sround, to_date,
                                   desc, row_number, expr, when)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType,
                               TimestampType)
from pyspark.sql.window import Window

if len(sys.argv) != 3:
    print(__doc__)
    sys.exit(2)
in_dir, out_dir = sys.argv[1].rstrip("/"), sys.argv[2].rstrip("/")

spark = SparkSession.builder.appName("TripAnalytics").getOrCreate()


def save(df, name, n=20):
    """Print a DataFrame and write it as a single CSV file with a header."""
    if df is None:
        print(f"\n=== {name}: not implemented yet ===")
        return
    print(f"\n=== {name} ===")
    df.show(n, truncate=False)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/{name}")



trips_schema = StructType([
    StructField("trip_id", StringType(), False),
    StructField("driver_id", StringType(), False),
    StructField("pickup_zone", IntegerType(), False),
    StructField("dropoff_zone", IntegerType(), False),
    StructField("pickup_ts", TimestampType(), False),
    StructField("dropoff_ts", TimestampType(), False),
    StructField("distance_km", DoubleType(), False),
    StructField("fare", StringType(), True),
    StructField("payment_type", StringType(), False),
])
zones_schema = "zone_id INT, zone_name STRING, borough STRING"

raw = spark.read.csv(f"{in_dir}/trips.csv", header=True, schema=trips_schema)
zones = spark.read.csv(f"{in_dir}/zones.csv", header=True, schema=zones_schema)
raw.printSchema()


# ---------------------------------------------------------------- Part 1: clean
def part1_clean(raw):
    # Convering fare, Invalid strings become null.
    typed = raw.withColumn(
        "_fare_num",
        expr("try_cast(fare AS DOUBLE)")
    )

    # Rule 1: dropoff must be after pickup.
    bad_time = col("dropoff_ts") <= col("pickup_ts")

    # Rule 2: fare must be present and numeric.
    bad_fare = col("_fare_num").isNull()

    # Count all five required values.
    counts = (
        typed.agg(
            count("*").alias("rows_total"),
            ssum(
                when(bad_time, 1).otherwise(0)
            ).alias("bad_time"),
            ssum(
                when(bad_fare, 1).otherwise(0)
            ).alias("bad_fare"),
            ssum(
                when(bad_time | bad_fare, 1).otherwise(0)
            ).alias("removed"),
        )
        .withColumn(
            "rows_kept",
            col("rows_total") - col("removed")
        )
        .select(
            "rows_total",
            "bad_time",
            "bad_fare",
            "removed",
            "rows_kept"
        )
    )

    # Keep only valid rows.
    clean = (
        typed
        .filter((~bad_time) & (~bad_fare))
        .withColumn("fare", col("_fare_num"))
        .drop("_fare_num")
        .withColumn(
            "day",
            to_date(col("pickup_ts"))
        )
        .withColumn(
            "duration_min",
            (
                col("dropoff_ts").cast("long")
                - col("pickup_ts").cast("long")
            ) / 60.0
        )
    )

    # Required Parquet output.
    clean.write.mode("overwrite").parquet(
        f"{out_dir}/clean_trips"
    )

    return clean, counts
    
    


# ---------------------------------------------------------------- Part 2a: zone revenue per day
def part2a_zone_revenue(clean, zones):
    """Trips and revenue per pickup zone per day."""
    result = (
        clean
        .join(
            zones,
            clean.pickup_zone == zones.zone_id,
            "inner"
        )
        .groupBy(
            clean.day,
            zones.zone_id,
            zones.zone_name
        )
        .agg(
            count("*").alias("trips"),
            sround(
                ssum(clean.fare),
                2
            ).alias("revenue")
        )
        .select(
            "day",
            "zone_id",
            "zone_name",
            "trips",
            "revenue"
        )
        .orderBy(
            col("day"),
            col("zone_id")
        )
    )

    return result
   


# ---------------------------------------------------------------- Part 2b: driver busiest day
def part2b_driver_busiest_day(clean):
        """Each driver's busiest day."""
        daily = (
            clean
            .groupBy("driver_id", "day")
            .agg(
                count("*").alias("trips")
            )
        )

        window_spec = (
            Window
            .partitionBy("driver_id")
            .orderBy(
                desc("trips"),
                col("day").asc()
            )
        )

        result = (
            daily
            .withColumn(
                "_rn",
                row_number().over(window_spec)
            )
            .filter(col("_rn") == 1)
            .select(
                "driver_id",
                "day",
                "trips"
            )
            .orderBy("driver_id")
        )

        return result
        
    


# ---------------------------------------------------------------- Part 3: Spark SQL
def part3_borough_sql(clean, zones):
    """Activity per pickup borough using Spark SQL."""

    clean.createOrReplaceTempView("trips")
    zones.createOrReplaceTempView("zones")

    result = spark.sql("""
        SELECT
            z.borough,
            COUNT(*) AS trips,
            ROUND(SUM(t.fare), 2) AS revenue,
            ROUND(AVG(t.distance_km), 2) AS avg_distance_km
        FROM trips t
        JOIN zones z
          ON t.pickup_zone = z.zone_id
        GROUP BY z.borough
        ORDER BY revenue DESC
    """)

    return result
   


clean, counts = part1_clean(raw)
save(counts, "part1")

zone_rev = part2a_zone_revenue(clean, zones) if clean is not None else None
save(zone_rev, "part2a")
save(part2b_driver_busiest_day(clean) if clean is not None else None, "part2b")

borough = part3_borough_sql(clean, zones) if clean is not None else None
save(borough, "part3")
if borough is not None and zone_rev is not None:
    print("\n=== Physical plan of part 2a (DataFrame API) ===")
    zone_rev.explain()
    print("\n=== Physical plan of part 3 (Spark SQL) ===")
    borough.explain()

spark.stop()
