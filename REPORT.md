# Assignment #3: Report

**Name:Hardi Koradiya**

**Student ID:801484363**

**Email:hkoradiy@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`:801484363

The commands you ran, in order:

```bash
python3 datagen.py 801484363
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

The seed generated 100,000 trips and 25 zones. The generated trips.csv contains 100,001 lines including the header, and zones.csv contains 26 lines including the header.
```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):
- Rule 1 removes trips where the drop-off time is not after the pickup time. A trip is invalid when dropoff_ts <= pickup_ts, so only rows where dropoff_ts > pickup_ts are kept.

- Rule 2 requires the fare to be present and numeric. The raw fare column is read as a string and converted with try_cast(fare AS DOUBLE). Values that cannot be converted become NULL and are treated as invalid fares and removed.

The cleaned data also contains the required day and duration_min derived columns and is written to Parquet.



Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:
- The fare column is read as a string because the input contains invalid fare values. Keeping it as a string preserves the original values so they can be identified and counted explicitly.

- If the column were read directly as DOUBLE, invalid values could become NULL during CSV parsing. That would make it harder to distinguish and count the original bad fare values. Reading the column as a string first and then using try_cast(fare AS DOUBLE) lets valid fares become numeric while invalid fares become NULL.

The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
|  100000    | 1575     | 1488     |  3063   | 96937     |

Two or three bad rows, as they appear in `trips.csv`:

```
BAD ROW:
T000007,D147,19,8,2026-09-29 01:52:26,2026-09-29 02:10:03,4.9,,app

BAD ROW:
T001201,D212,15,12,2026-09-21 08:37:25,2026-09-21 09:06:07,7.57,"12,40",app
BAD ROW:
T000029,D208,15,7,2026-09-12 08:06:16,2026-09-12 07:55:48,2.89,11.70,card

The first row has a missing fare, the second row contains a non-numeric fare (`12,40`), and the third row has a drop-off timestamp earlier than its pickup timestamp. These examples demonstrate both fare-validation rules and the timestamp-validation rule.
```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```
26/09/27 22:29:21 INFO DAGScheduler: Asked to cleanup jobs for query execution 3
+----------+-------+---------------+-----+-------+
|day       |zone_id|zone_name      |trips|revenue|
+----------+-------+---------------+-----+-------+
|2026-09-01|1      |Uptown Center  |362  |6984.71|
|2026-09-01|2      |First Ward     |232  |4438.56|
|2026-09-01|3      |Fourth Ward    |282  |5497.42|
|2026-09-01|4      |South End      |261  |5058.94|
|2026-09-01|5      |Dilworth       |169  |3159.09|
|2026-09-01|6      |Plaza Midwood  |158  |3087.55|
|2026-09-01|7      |NoDa           |147  |2958.75|
|2026-09-01|8      |University City|201  |3874.76|
|2026-09-01|9      |Ballantyne     |136  |2735.58|
|2026-09-01|10     |SouthPark      |177  |3307.18|

--> On September 1, Uptown Center recorded the highest number of trips among the displayed zones, with 362 trips and $6,984.71 in revenue. Fourth Ward followed with 282 trips, while Ballantyne had the lowest trip count among these ten zones, with 136 trips. This shows that trip activity and revenue varied considerably across pickup zones on the same day.

```

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```
26/09/27 22:29:23 INFO DAGScheduler: Asked to cleanup jobs for query execution 5
+---------+----------+-----+
|driver_id|day       |trips|
+---------+----------+-----+
|D001     |2026-09-04|21   |
|D002     |2026-09-21|22   |
|D003     |2026-09-01|18   |
|D004     |2026-09-04|19   |
|D005     |2026-09-10|19   |
|D006     |2026-09-03|21   |
|D007     |2026-09-03|23   |
|D008     |2026-09-25|25   |
|D009     |2026-09-13|20   |
|D010     |2026-09-01|23   |

---> Among the first ten drivers, D008 had the highest number of trips on their busiest day, completing 25 trips on September 25. D003 had the lowest count among these drivers, with 18 trips on September 1. The results show that the busiest day and daily trip volume vary between drivers.
```

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql

```
The SQL query used for the borough analysis was:

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


```
26/09/27 22:31:45 INFO DAGScheduler: Asked to cleanup jobs for query execution 9
+-------+-----+---------+---------------+
|borough|trips|revenue  |avg_distance_km|
+-------+-----+---------+---------------+
|Central|39894|788410.95|5.36           |
|South  |17119|339132.55|5.37           |
|North  |15505|307483.4 |5.38           |
|East   |12851|256529.92|5.41           |
|West   |11568|228833.68|5.36           |
+-------+-----+---------+---------------+

--> Central had the highest activity, accounting for 39,894 trips, or approximately 41.2% of all valid trips, and generating $788,410.95 in revenue. South ranked second with 17,119 trips and $339,132.55 in revenue. West had the lowest activity, with 11,568 trips and $228,833.68 in revenue. Average trip distances were relatively similar across all five boroughs, ranging from 5.36 km to 5.41 km.
```

---

## The two plans

`explain()` output of part 2a (DataFrame API):
```
=== Physical plan of part 2a (DataFrame API) ===
26/09/27 22:31:47 INFO FileSourceStrategy: Pushed Filters: IsNotNull(dropoff_ts),IsNotNull(pickup_ts),IsNotNull(fare),IsNotNull(pickup_zone)
26/09/27 22:31:47 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4), isnotnull(try_cast(fare#7 as double)), isnotnull(pickup_zone#2))
26/09/27 22:31:47 INFO FileSourceStrategy: Pushed Filters: IsNotNull(zone_id)
26/09/27 22:31:47 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(zone_id#9))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day#34 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day#34 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1547]
      +- HashAggregate(keys=[day#34, zone_id#9, zone_name#10], functions=[count(1), sum(fare#33)])
         +- Exchange hashpartitioning(day#34, zone_id#9, zone_name#10, 200), ENSURE_REQUIREMENTS, [plan_id=1544]
            +- HashAggregate(keys=[day#34, zone_id#9, zone_name#10], functions=[partial_count(1), partial_sum(fare#33)])
               +- Project [fare#33, day#34, zone_id#9, zone_name#10]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, try_cast(fare#7 as double) AS fare#33, cast(pickup_ts#4 as date) AS day#34]
                     :  +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1539]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>

```

`explain()` output of part 3 (Spark SQL):

```
=== Physical plan of part 3 (Spark SQL) ===
26/09/27 22:31:47 INFO FileSourceStrategy: Pushed Filters: IsNotNull(dropoff_ts),IsNotNull(pickup_ts),IsNotNull(fare),IsNotNull(pickup_zone)
26/09/27 22:31:47 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4), isnotnull(try_cast(fare#7 as double)), isnotnull(pickup_zone#2))
26/09/27 22:31:47 INFO FileSourceStrategy: Pushed Filters: IsNotNull(zone_id)
26/09/27 22:31:47 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(zone_id#9))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [revenue#184 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue#184 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1597]
      +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#33), avg(distance_km#6)])
         +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1594]
            +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#33), partial_avg(distance_km#6)])
               +- Project [distance_km#6, fare#33, borough#11]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, distance_km#6, try_cast(fare#7 as double) AS fare#33]
                     :  +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,distance_km#6,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,distance_km:double,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1589]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>


```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.


## Comparision:-  
- Both plans scan the trips and zones CSV files using FileScan csv. The trips scan applies the validity filters, including non-null timestamps, dropoff_ts > pickup_ts, a numeric fare from try_cast, and a non-null pickup zone. The zones scan filters out null zone IDs.

- Both plans use a BroadcastHashJoin with an Inner join and BuildRight. The small zones dataset is broadcast through BroadcastExchange.

- For Part 2a, the first exchange is hashpartitioning(day, zone_id, zone_name, 200). This redistributes the data so rows with the same grouping keys can be aggregated together. The second exchange is rangepartitioning(day ASC, zone_id ASC, 200), which prepares the data for the final global sort by day and zone ID.

- For Part 3, the first exchange is hashpartitioning(borough, 200), which redistributes rows by borough before the borough aggregation. The second exchange is rangepartitioning(revenue DESC, 200), which prepares the data for the final global sort by revenue.

- Although Part 2a was written using the DataFrame API and Part 3 was written using Spark SQL, both are optimized by Spark's Catalyst optimizer and produce physical plans using the same types of Spark operators, including FileScan, BroadcastHashJoin, BroadcastExchange, HashAggregate, Exchange, and Sort. The grouping and sorting keys differ because the two queries perform different analyses.

---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

- With 200 million trips instead of 100,000, I would keep the explicit schema, reading the raw fare column as a string, using try_cast for fare validation, filtering invalid trips, and using Spark DataFrame/Spark SQL operations instead of collecting data into Python. I would keep the cleaned data in Parquet for repeated analytical processing. The first changes 

- I would make would be partitioning the cleaned Parquet data by a useful key such as day, avoiding coalesce(1) for large outputs because it forces work through a single partition, and considering caching the cleaned DataFrame because multiple analysis parts reuse it. I would also tune Spark's shuffle partition settings and monitor shuffle stages for the larger workload.

---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

- No errors occurred during the final execution of the completed assignment. The Spark cluster started successfully with one master and two workers, the generated input contained 100,000 trips and 25 zones, and the final Spark job completed successfully and produced the Part 1, Part 2a, Part 2b, Part 3, and physical-plan outputs.





