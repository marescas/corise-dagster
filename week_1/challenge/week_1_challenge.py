import csv
from datetime import datetime
from heapq import nlargest
from random import randint
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    Output,
    String,
    job,
    op,
    usable_as_dagster_type, Int
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={"s3_key": String}, out={"stocks": Out(is_required=False), "empty_stocks": Out(is_required=False)})
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    stock_list = list(csv_helper(s3_key))
    if len(stock_list) == 0:
        yield Output(None, "empty_stocks")
    else:
        yield Output(stock_list, "stocks")


@op(config_schema={"nlargest": Int}, out=DynamicOut())
def process_data(context, stocks: List[Stock]) -> List[Aggregation]:
    nlargest_int = context.op_config["nlargest"]
    for stock in sorted(stocks, key=lambda t: t.high, reverse=True)[:nlargest_int]:
        yield DynamicOutput(Aggregation(date=stock.date, high=stock.high), mapping_key=str(int(stock.date.timestamp())))


@op
def put_redis_data(context, aggregation: Aggregation):
    pass


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    description="Notifiy if stock list is empty",
)
def empty_stock_notify(context, empty_stocks: List[Stock]) -> Nothing:
    context.log.info("No stocks returned")


@job
def week_1_challenge():
    stocks, empty_stocks = get_s3_data()
    empty_stock_notify(empty_stocks)
    data_processed = process_data(stocks)
    data_processed.map(put_redis_data)
