from typing import Mapping, Iterable, Union
from pyspark import SparkConf


DEFAULT_CONF = {
    "spark.app.name": "sessions",
    "spark.master": "local[*]",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "200",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0",
    "spark.sql.warehouse.dir": "./spark-warehouse",
    "spark.driver.memory": "1g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "268435456",  # 256MB
    "spark.sql.adaptive.skewJoin.enabled": "true",  # 256MB
}


def create_conf(config: Union[Mapping[str, str], Iterable[str]]) -> SparkConf:
    if isinstance(config, Mapping):
        items = [(str(k), str(v)) for k, v in config.items()]
    else:
        items = []
        for item in config:
            k, v = item.split("=", 1)
            items.append((k, v))
    return SparkConf().setAll(items)
