from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config, String, build_schedule_from_partitioned_job,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(config_schema={"s3_key": String}, required_resource_keys={"s3"}, tags={"kind": "s3"})
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    return [Stock.from_list(stock_list) for stock_list in context.resources.s3.get_data(s3_key)]


@op
def process_data(context, stocks: List[Stock]) -> Aggregation:
    best_stock = None
    high_value = -1 * 10 ** 6
    for stock in stocks:
        if stock.high > high_value:
            high_value = stock.high
            best_stock = Aggregation(date=stock.date, high=stock.high)
    return best_stock


@op(required_resource_keys={"redis"}, tags={"kind": "redis"})
def put_redis_data(context, aggregation: Aggregation):
    context.resources.redis.put_data(name=str(aggregation.date), value=str(aggregation.high))


@op(required_resource_keys={"s3"}, tags={"kind": "s3"})
def put_s3_data(context, aggregation: Aggregation):
    context.resources.s3.put_data(key_name=str(aggregation.date), data=aggregation)


@graph
def week_3_pipeline():
    result = process_data(get_s3_data())
    put_redis_data(result)
    put_s3_data(result)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

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
    "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_9.csv.csv"}}},
}


@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11)])
def docker_config(partition_key: str):
    return {
        **docker,
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()}
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)

week_3_schedule_local = ScheduleDefinition(job=week_3_pipeline_local, cron_schedule="*/15 * * * *")

week_3_schedule_docker = ScheduleDefinition(job=week_3_pipeline_docker, run_config=docker_config,
                                            cron_schedule="0 * * * *")


@sensor(job=week_3_pipeline_docker, minimum_interval_seconds=30)
def week_3_sensor_docker(context):
    s3_keys = get_s3_keys(bucket="dagster", prefix="prefix")
    if len(s3_keys) == 0:
        yield SkipReason("No new s3 files found in bucket.")
    else:
        for s3_key in s3_keys:
            yield RunRequest(run_key=s3_key, run_config={**docker, "ops":
                {"get_s3_data": {
                    "config": {"s3_key": s3_key}}}})
