import os
import sys

# given permissions issues in databricks, skip writing compiled .pyc files in a newly created __pycache__ folder. 
# this is done prior to importing pytest.
sys.dont_write_bytecode = True 

import pytest
# not park of local pyspark so tests need to be run in a databricks environment
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

# get the path to this notebook
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# define the subdirectories to ad to sys.path
subdirs = ["tests", "src"]

# insert each path into sys.path if not already present
for i, subdir in enumerate(subdirs):
    path = f"/Workspace/{repo_root}/{subdir}/"
    if path not in sys.path:
        sys.path.insert(i, path)

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider", "--disable-warnings"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
