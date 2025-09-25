# Convert events to sessions

from datetime import date, datetime, timedelta
from typing import List, Optional, Union, Dict
from loguru import logger

import click
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, types as T
from delta.tables import DeltaTable

from event_sessions.utils.parameters import INPUT_PATH, OUTPUT_PATH, USER_ACTION_IDS, INACTIVITY_SECONDS
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
    keys_for_del = (
        impacted_keys
        .select("user_id", "product_code")
        .distinct()
        .withColumn("left_date",  F.lit(write_left_date))
        .withColumn("right_date", F.lit(write_right_date))
    )

    tgt = DeltaTable.forPath(spark, output_path)

    # MERGE delete
    tgt.alias("t").merge(
        keys_for_del.alias("k"),
        (
            "t.user_id = k.user_id AND "
            "t.product_code = k.product_code AND "
            "t.event_date >= k.left_date AND "
            "t.event_date <= k.right_date"
        ),
    ).whenMatchedDelete().execute()
    updates.write.format("delta").mode("append").partitionBy("event_date").save(output_path)

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
    run_date_str: date,
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
        .join(impacted_keys, ["user_id", "product_code"], "inner")
        .where(
            (F.col("timestamp") >= F.lit(ctx_left_ts)) &
            (F.col("timestamp") < F.lit(ctx_right_ts_excl))
        )
        .cache()  # TODO: not necessary but possible
    )
    existing_superset.count()

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

    w_sess = Window.partitionBy("user_id", "product_code", "sess_seq")
    sessions = (
        ua
        .withColumn("session_start_ts", F.min("ts").over(w_sess))
        .withColumn("last_user_action_ts", F.max("ts").over(w_sess))
        .select("user_id", "product_code", "sess_seq", "session_start_ts", "last_user_action_ts")
        .dropDuplicates(["user_id", "product_code", "sess_seq"])
        .withColumn(
            "session_end_ts",
            F.expr(f"last_user_action_ts + INTERVAL {INACTIVITY_SECONDS} SECONDS")
        )
        .drop("sess_seq")
        .withColumn(
            "session_id",
            F.concat_ws(
                "#",
                F.col("user_id"),
                F.col("product_code"),
                F.date_format(F.col("session_start_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            )
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

    # Step 7: Assign session_id to all events in the write window (broadcast range join)
    s_for_join = sessions.select(
        F.col("user_id").alias("s_user_id"),
        F.col("product_code").alias("s_product_code"),
        "session_start_ts", "session_end_ts", "session_id"
    )

    updates = (
        to_write_base
        .withColumn("ts", F.col("timestamp"))
        .join(
            F.broadcast(s_for_join),
            on=[
                F.col("user_id") == F.col("s_user_id"),
                F.col("product_code") == F.col("s_product_code"),
                F.col("ts") >= F.col("session_start_ts"),
                F.col("ts") <= F.col("session_end_ts"),
            ],
            how="left"
        )
        .drop("s_user_id", "s_product_code", "ts")
        .withColumn(
            "session_start_ts",
            F.when(F.col("session_id").isNotNull(), F.col("session_start_ts"))
            .otherwise(F.lit(None).cast("timestamp"))
        )
        .withColumn(
            "session_end_ts",
            F.when(F.col("session_id").isNotNull(), F.col("session_end_ts"))
            .otherwise(F.lit(None).cast("timestamp"))
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
    updates = updates_b.repartition(num_days * files_per_day, "event_date", "day_bucket").drop("day_bucket")

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
