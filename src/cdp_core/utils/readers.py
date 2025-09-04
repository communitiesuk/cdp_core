import requests

from databricks.sdk.runtime import *

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

def read_table(catalog: str, schema: str, table: str):
    return spark.read.table(f'{catalog}.{schema}.{table}')


