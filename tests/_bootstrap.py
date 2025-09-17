import sys

import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
user_email = spark.sql("SELECT current_user()").collect()[0][0]

paths = [
    f"/Workspace/Users/{user_email}/cdp_core/src",
    f"/Workspace/Users/{user_email}/.bundle/cdp_core/files",
    f"/Workspace/Users/{user_email}/.bundle/cdp_core/files/src"
]

for path in paths:
    if path not in sys.path:
        sys.path.append(path)