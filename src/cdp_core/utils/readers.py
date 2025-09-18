import requests
from databricks.sdk.runtime import *

from pyspark.sql import SparkSession, DataFrame

from cdp_core.setup.constants import CATALOG

spark = SparkSession.builder.getOrCreate()

class RestClient:
    def __init__(self, base_url: str, headers: dict = None, params: dict = None):
        self.base_url = base_url
        self.headers = headers or {}
        self.params = params or {}

    def get(self) -> dict:
        try:
            response = requests.get(self.base_url, headers=self.headers, params=self.params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"HTTP request failed: {e}")
            return {}

def read_table(schema: str, table: str) -> DataFrame:
    """Function to return a Spark DataFrame from a table in the specified schema"""
    return spark.read.table(f'{CATALOG}.{schema}.{table}')


