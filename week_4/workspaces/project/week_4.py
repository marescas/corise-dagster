from typing import List

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(config_schema={"s3_key": String},
       required_resource_keys={"s3"},
       op_tags={"kind": "s3"}, group_name="corise")
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(stock_list) for stock_list in context.resources.s3.get_data(s3_key)]


@asset(group_name="corise")
def process_data(get_s3_data) -> Aggregation:
    best_stock = None
    high_value = -1 * 10 ** 6
    for stock in get_s3_data:
        if stock.high > high_value:
            high_value = stock.high
            best_stock = Aggregation(date=stock.date, high=stock.high)
    return best_stock


@asset(required_resource_keys={"redis"}, op_tags={"kind": "redis"}, group_name="corise")
def put_redis_data(context, process_data):
    context.resources.redis.put_data(name=str(process_data.date), value=str(process_data.high))


@asset(required_resource_keys={"s3"}, op_tags={"kind": "s3"}, group_name="corise")
def put_s3_data(context, process_data):
    context.resources.s3.put_data(key_name=str(process_data.date), data=process_data)


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}
get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key=docker)
