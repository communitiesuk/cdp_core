import requests
from databricks.sdk.runtime import *

from pyspark.sql import SparkSession, DataFrame

from cdp_core.setup.constants import CATALOG

spark = SparkSession.builder.getOrCreate()

class RestClient:
    """
    A simple REST client for making GET requests.

    Atributes:
        base_url (str): The base URL for the request.
        headers (dict): Optional HTTP headers.
        params (dict): Optional query parameters.
    """
    def __init__(self, base_url: str, headers: dict = None, params: dict = None):
        self.base_url = base_url
        self.headers = headers or {}
        self.params = params or {}

    def get(self) -> dict:
        """
        Sends a GET request to the specified base URL with optional headers and parameters.

        Returns:
            dict: The JSON  response from the server
        
        Raises:
            requests.exceptions.ReqeustException: If the request fails.
        """
        response = requests.get(
            self.base_url,
            headers=self.headers,
            params=self.params,
            timeout=10  # seconds
)
        response.raise_for_status()
        return response.json()


def read_table(schema: str, table: str) -> DataFrame:
    """Function to return a Spark DataFrame from a table in the specified schema"""
    return spark.read.table(f'{CATALOG}.{schema}.{table}')


