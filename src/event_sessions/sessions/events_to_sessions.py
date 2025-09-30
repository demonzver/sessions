# Convert events to sessions

from datetime import date, datetime, timedelta
from typing import List, Optional, Union, Dict
from loguru import logger

import click
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

from event_sessions.utils.parameters import INPUT_PATH, OUTPUT_PATH, USER_ACTION_IDS, INACTIVITY_SECONDS, BUCKET_BIN
from event_sessions.utils.spark import create_conf, DEFAULT_CONF


def rewrite_impacted_users(
    spark: SparkSession,
    output_path: str,
    impacted_keys: DataFrame,
    updates: DataFrame,
    write_left_date: Union[str, date],
    write_right_date: Union[str, date],
    run_date_str: str,
) -> Dict[str, int]:
    # Build unified MERGE source
    del_src = (
        impacted_keys.select("user_id","product_code").distinct()
        .withColumn("op", F.lit("del"))
        .withColumn("left_date", F.lit(write_left_date).cast("date"))
        .withColumn("right_date", F.lit(write_right_date).cast("date"))
        .withColumn("del_date", F.explode(F.sequence("left_date","right_date")))
        # add nullable columns to align schemas for union
        .withColumn("event_id", F.lit(None).cast("string"))
        .withColumn("timestamp", F.lit(None).cast("timestamp"))
        .withColumn("event_date", F.lit(None).cast("date"))
        .withColumn("session_start_ts", F.lit(None).cast("timestamp"))
        .withColumn("session_end_ts", F.lit(None).cast("timestamp"))
        .withColumn("session_id", F.lit(None).cast("string"))
    )

    ins_src = (
        updates.select(
            "user_id",
            "event_id",
            "product_code",
            "timestamp",
            "event_date",
            "session_start_ts",
            "session_end_ts",
            "session_id"
        )
        .withColumn("op", F.lit("ins"))
        .withColumn("del_date", F.lit(None).cast("date"))
        .withColumn("left_date", F.lit(None).cast("date"))
        .withColumn("right_date", F.lit(None).cast("date"))
    )

    src = del_src.unionByName(ins_src, allowMissingColumns=True).alias("s")

    tgt = DeltaTable.forPath(spark, output_path)

    on_cond = """
      s.op = 'del' AND
      t.user_id = s.user_id AND
      t.product_code = s.product_code AND
      t.event_date = s.del_date
    """

    (
        tgt.alias("t")
         .merge(src, on_cond)
         .whenMatchedDelete()
         .whenNotMatchedInsert(
             condition="s.op = 'ins'",
             values={
                 "user_id": "s.user_id",
                 "event_id": "s.event_id",
                 "product_code": "s.product_code",
                 "timestamp": "s.timestamp",
                 "event_date": "s.event_date",
                 "session_start_ts": "s.session_start_ts",
                 "session_end_ts": "s.session_end_ts",
                 "session_id": "s.session_id",
             }
         )
         .execute()
    )

    version = DeltaTable.forPath(spark, output_path).history(1).select("version").head()[0]
    logger.info(
        f"Rewrote impacted users for dates [{write_left_date} .. {write_right_date}] "
        f"from batch date={run_date_str}; table_version={version}"
    )
    return {
        "status": "ok",
        "output_path": output_path,
        "left_date": str(write_left_date),
        "right_date": str(write_right_date),
        "table_version": int(version),
    }


def build_job(
    spark: SparkSession,
    run_date_str: str,
    df_raw: DataFrame,
    output_path: str,
    files_per_day: int,
) -> DataFrame:
    # Step 1: Daily raw partition
    df = (
        df_raw.select(
            F.col("user_id").cast("string").alias("user_id"),
            F.col("event_id").cast("string").alias("event_id"),
            F.col("product_code").cast("string").alias("product_code"),
            F.col("timestamp").cast("timestamp").alias("timestamp"),
        )
        .withColumn("event_date", F.to_date("timestamp"))
    )

    # Step 2: Compute all dates / timestamps - dmin..dmax are the actual event_date bounds inside today's batch
    mm = df.agg(
        F.min("event_date").alias("dmin"),
        F.max("event_date").alias("dmax")
    ).first()
    dmin, dmax = mm["dmin"], mm["dmax"]

    # Context window for building sessions: [ctx_left_ts, ctx_right_ts_excl)
    left_ctx_date = dmin - timedelta(days=1)  # look back 1 day
    right_ctx_date = dmax + timedelta(days=1)  # look ahead 1 day
    ctx_left_ts = f"{left_ctx_date} 00:00:00"
    ctx_right_ts_excl = f"{(right_ctx_date + timedelta(days=1))} 00:00:00"  # = dmax+2 00:00

    # Write window: rewrite impacted users for [dmin .. dmax+1]
    write_left_date = dmin
    write_right_date = dmax + timedelta(days=1)
    write_left_ts = f"{dmin} 00:00:00"
    write_right_ts_excl = f"{(dmax + timedelta(days=2))} 00:00:00"  # = dmax+2 00:00

    logger.info(
        f"Dates: run_date_str={run_date_str}, "
        f"ctx=[{ctx_left_ts} .. {ctx_right_ts_excl}), "
        f"write=[{write_left_ts} .. {write_right_ts_excl}), "
        f"new batch dates=[{write_left_date} .. {write_right_date}]"
    )

    # Step 3: Detect impacted keys to limit IO
    impacted_keys = df.select("user_id", "product_code").distinct()

    # Step 4: Read Delta once for impacted keys, then split into ctx/write
    existing_superset = (
        spark.read.format("delta").load(output_path)
        .select("user_id", "event_id", "product_code", "timestamp", "event_date")
        .join(impacted_keys, ["user_id", "product_code"], "left_semi")
        .where(
            (F.col("event_date") >= F.lit(left_ctx_date)) &
            (F.col("event_date") <= F.lit(right_ctx_date)) &
            (F.col("timestamp") >= F.lit(ctx_left_ts)) &
            (F.col("timestamp") < F.lit(ctx_right_ts_excl))
        )
    )

    # optional cache/persist
    # existing_superset = existing_superset.persist(StorageLevel.DISK_ONLY)
    # existing_superset.count()
    existing_ctx = existing_superset
    existing_write = existing_superset.where(
        (F.col("timestamp") >= F.lit(write_left_ts)) &
        (F.col("timestamp") < F.lit(write_right_ts_excl))
    )

    # Step 5: Build sessions over (new + context) only for impacted keys - Basic session detection logic
    all_for_sessions = df.select(
        "user_id", "event_id", "product_code", "timestamp", "event_date"
    ).unionByName(
        existing_ctx.select("user_id", "event_id", "product_code", "timestamp", "event_date")
    )

    df_window = (
        all_for_sessions
        .where(
            (F.col("timestamp") >= F.lit(ctx_left_ts)) &
            (F.col("timestamp") < F.lit(ctx_right_ts_excl))
        )
        .repartition("user_id", "product_code")  # reduce skew
    )

    ua = (
        df_window
        .filter(F.col("event_id").isin([*USER_ACTION_IDS]))
        .select("user_id", "product_code", F.col("timestamp").alias("ts"))
    )

    w = Window.partitionBy("user_id", "product_code").orderBy("ts")
    ua = ua.withColumn("prev_ts", F.lag("ts").over(w))
    ua = ua.withColumn(
        "is_new",
        F.when(
            F.col("prev_ts").isNull() |
            ((F.col("ts").cast("long") - F.col("prev_ts").cast("long")) >= INACTIVITY_SECONDS),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    ua = ua.withColumn(
        "sess_seq",
        F.sum("is_new").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    )

    sessions = (
        ua.groupBy("user_id", "product_code", "sess_seq")
        .agg(
            F.min("ts").alias("session_start_ts"),
            F.max("ts").alias("last_user_action_ts"),
            # F.count("*").alias("events_in_session")
        )
        .withColumn("session_end_ts", F.expr(f"last_user_action_ts + INTERVAL {INACTIVITY_SECONDS} SECONDS"))
        .withColumn(
            "session_id",
            F.concat_ws("#", "user_id", "product_code",
                        F.date_format("session_start_ts", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        )
    )

    # Step 6: Full write set for [dmin .. dmax+1] - keep existing + add missing new
    new_only = (
        df.where(
            (F.col("timestamp") >= F.lit(write_left_ts)) &
            (F.col("timestamp") < F.lit(write_right_ts_excl))
        )
        .join(
            existing_write.select("user_id", "product_code", "timestamp").distinct(),
            on=["user_id", "product_code", "timestamp"],
            how="left_anti"
        )
    )

    to_write_base = existing_write.unionByName(new_only)

    # Step 7: Assign session_id to all events in the write window
    # We want to avoid inefficiencies caused by the non-equi join
    bucket_seconds = F.lit(BUCKET_BIN)

    # 7.1 Events -> bucket per event
    events_bucketed = (
        to_write_base
        .withColumn("ts", F.col("timestamp"))
        .withColumn("time_bucket", F.floor(F.col("ts").cast("long") / bucket_seconds))
    )

    # 7.2 Sessions -> ALL buckets intersecting each session [start, end]
    sessions_exploded = (
        sessions.select(
            F.col("user_id").alias("s_user_id"),
            F.col("product_code").alias("s_product_code"),
            "session_start_ts",
            "session_end_ts",
            "session_id",
        )
        .withColumn("b_start", F.floor(F.col("session_start_ts").cast("long") / bucket_seconds))
        .withColumn("b_end", F.floor(F.col("session_end_ts").cast("long") / bucket_seconds))
        .withColumn("s_time_bucket", F.explode(F.sequence(F.col("b_start"), F.col("b_end"))))
        .drop("b_start", "b_end")
    )

    # 7.3 Left equi-join on (user_id, product_code, bucket)
    # then conditionally null out session fields for out-of-interval candidates
    candidates = (
        events_bucketed
        .join(
            sessions_exploded,
            on=[
                F.col("user_id") == F.col("s_user_id"),
                F.col("product_code") == F.col("s_product_code"),
                F.col("time_bucket") == F.col("s_time_bucket"),
            ],
            how="left",
        )
    )

    in_range = (
            (F.col("ts") >= F.col("session_start_ts")) &
            (F.col("ts") <= F.col("session_end_ts"))
    )

    candidates = (
        candidates
        .withColumn("session_id_eff", F.when(in_range, F.col("session_id")))
        .withColumn("sess_start_eff", F.when(in_range, F.col("session_start_ts")).cast("timestamp"))
        .withColumn("sess_end_eff", F.when(in_range, F.col("session_end_ts")).cast("timestamp"))
    )

    # 7.4 Collapse to a single row per event: take the first non-null, otherwise keep NULL
    updates = (
        candidates
        .groupBy("user_id", "event_id", "product_code", "timestamp")
        .agg(
            F.first("event_date", ignorenulls=True).alias("event_date"),
            F.first("session_id_eff", ignorenulls=True).alias("session_id"),
            F.first("sess_start_eff", ignorenulls=True).alias("session_start_ts"),
            F.first("sess_end_eff", ignorenulls=True).alias("session_end_ts"),
        )
        .withColumn("event_date", F.to_date("timestamp"))
        .select(
            "user_id", "event_id", "product_code", "timestamp",
            "event_date", "session_start_ts", "session_end_ts", "session_id"
        )
    )

    # Step 8: Delete impacted users for [dmin .. dmax+1] and append fresh result (MERGE delete)
    # Repartition by event_date and a hash-based day_bucket to evenly distribute data and control files per day
    updates_b = updates.withColumn("day_bucket", F.pmod(F.xxhash64("user_id"), F.lit(files_per_day)))
    num_days = (write_right_date - write_left_date).days + 1
    updates = (
        updates_b.repartition(num_days * files_per_day, "event_date", "day_bucket")
        .sortWithinPartitions("user_id", "product_code", "timestamp")  # optimize compression
        .drop("day_bucket")
    )

    rewrite_impacted_users(
        spark=spark,
        output_path=output_path,
        impacted_keys=impacted_keys,
        updates=updates,
        write_left_date=write_left_date,
        write_right_date=write_right_date,
        run_date_str=run_date_str,
    )


@click.command()
@click.option("--run-date", type=click.DateTime(formats=["%Y-%m-%d"]), help="Date of run")
@click.option("--input-path", default=INPUT_PATH)
@click.option("--output-path", default=OUTPUT_PATH)
@click.option("--files-per-day", type=int, default=1)
@click.option("--conf", multiple=True, default=None)
def main(
    run_date: datetime,
    input_path: str,
    output_path: str,
    files_per_day: int,
    conf: Optional[List[str]] = None,
):
    spark_config = create_conf(DEFAULT_CONF) if not conf else create_conf(conf)

    spark = (
        SparkSession.builder.appName("sessions")
        .enableHiveSupport()
        .config(conf=spark_config)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")

    run_date_str = run_date.date().strftime("%Y-%m-%d")
    read_partition_path = f"{input_path}/date={run_date_str}"
    logger.debug(f"[DEBUG] read_partition_path={read_partition_path}")

    df_raw = spark.read.parquet(read_partition_path)

    build_job(
        spark=spark,
        run_date_str=run_date_str,
        df_raw=df_raw,
        output_path=output_path,
        files_per_day=files_per_day,
    )


if __name__ == "__main__":
    main()
