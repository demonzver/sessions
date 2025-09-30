# Sessions for IDE Events

## The assignment
Let’s imagine there is data about the usage of different IDEs with the following structure:  
**user_id** – a user’s anonymized identifier;  
**event_id** – identifier of an event that happened inside an IDE; each event corresponds to the action either of a user or an IDE itself.  
For the sake of simplicity we assume that an event is a user action if **event_id in (‘a’, ‘b’, ‘c’)**;  
**timestamp;**
**product_code** – shortened name of an IDE  

A new batch of raw data becomes available each day and is related to five past days maximum.  
Suggest a solution for computation of a user session identifier.  
It has **user_id#product_code#timestamp** format where the timestamp corresponds to the beginning of a session.  
The identifier can be assigned to an event regardless of its type.  
A session always starts with a user’s event (not an IDE’s) and breaks if there are no user actions for five minutes.   
Sessions need to be extended if new related data was acquired (for example, today we received data from three days ago).  
Please use Spark to solve the task, it's up to you how detailed the design and implementation should be.  

---

## Output Schema

**Target Delta** (partitioned by `event_date`):

| column              | type      | notes                                           |
|---------------------|-----------|-------------------------------------------------|
| `user_id`           | string    | anonymized user                                 |
| `event_id`          | string    | event code (user: `a,b,c`; IDE: others)         |
| `product_code`      | string    | IDE                                             |
| `timestamp`         | timestamp | UTC                                             |
| `event_date`        | date      | `to_date(timestamp)` — partition column         |
| `session_start_ts`  | timestamp | start of session (UTC)                          |
| `session_end_ts`    | timestamp | last user action + 5 minutes (UTC)              |
| `session_id`        | string    | `user_id#product_code#YYYY-MM-DDThh:mm:ss.SSSZ` |

IDE events outside sessions keep `session_id = NULL`.

---

## Create an empty Delta at `OUTPUT_PATH`:
```python
schema = T.StructType([
    T.StructField("user_id", T.StringType(), True),
    T.StructField("event_id", T.StringType(), True),
    T.StructField("product_code", T.StringType(), True),
    T.StructField("timestamp", T.TimestampType(), True),
    T.StructField("event_date", T.DateType(), True),
    T.StructField("session_start_ts", T.TimestampType(), True),
    T.StructField("session_end_ts", T.TimestampType(), True),
    T.StructField("session_id", T.StringType(), True),
])

(
    spark.createDataFrame([], schema)
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(output_path)
)
```

---

## Notes
- Timestamps must be **UTC**
- **Spark 3.5.1**
- **Delta Lake** runtime (match Scala version of your Spark)
- Input: daily batches under `INPUT_PATH/date=YYYY-MM-DD/` (Parquet)
- **AQE**
- SparkSession conf: `src/event_sessions/utils/spark.py`
- Params: `src/event_sessions/utils/parameters.py`


---

## Quick Start (Ununtu)
- `git clone https://github.com/demonzver/sessions.git`
- `cd sessions`
- poetry:  `curl -sSL https://install.python-poetry.org | python3 -`
- installing dependencies: `poetry install`
- `poetry env info --executable`
- `PYSPARK_DRIVER_PYTHON=/home/.../.cache/pypoetry/virtualenvs/sessions-...-py3..../bin/python`
- `PYSPARK_DRIVER_PYTHON_OPTS=`
- `spark-submit`

---

## spark-submit
```text
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 src/event_sessions/sessions/events_to_sessions.py --run-date 2025-09-21
```
---

## Demo
```text
notebooks/demo.ipynb
```

---

## TODO:  
    1. Orchestration in Airflow
    2. Regular compaction (bin packing) for the affected date range [write_left_date..write_right_date]
    3. Weekly VACUUM with retention ≥7 days to remove old files
    4. Check Skewed users (AQE helps)
    5. Add filtering for users with a huge number of events (bots)
