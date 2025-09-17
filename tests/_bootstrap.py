import sys

import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
user_email = spark.sql("SELECT current_user()").collect()[0][0]

repo_root = f"/Workspace/Users/{user_email}/cdp_core/src"

if repo_root not in sys.path:
    sys.path.append(repo_root)

sp_root = f"/Workspace/Users/{user_email}/.bundle/cdp_core/files"
if sp_root not in sys.path:
    sys.path.append(sp_root)