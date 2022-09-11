from typing import List

from dagster import (
    Nothing,
    asset,
    with_resources,
    static_partitioned_config,
    StaticPartitionsDefinition,
    get_dagster_logger,
)
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    group_name="corise",
    description="Stocks List",
)
def get_s3_data(context):
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    description="Find the highest value in the high field",
    group_name="corise",
)
def process_data(get_s3_data):
    highest_stock = max(get_s3_data, key=lambda x: x.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
    description="Put Data into Redis",
)
def put_redis_data(context, process_data):
    log = get_dagster_logger()
    log.info("Put Data into Redis")
    context.resources.redis.put_data(str(process_data.date), str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    resource_config_by_key={
        "resources": {
            "s3": {
                "config": {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://host.docker.internal:4566",
                }
            },
            "redis": {
                "config": {
                    "host": "redis",
                    "port": 6379,
                }
            },
        },
        "ops": {
            "get_s3_data": {
                "config": {
                    "s3_key": "prefix/stock.csv",
                }
            }
        },
    },
)
