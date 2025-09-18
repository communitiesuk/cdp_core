import os, sys
sys.dont_write_bytecode = True # skip writing compiled .pyc files in __pycache__. This is done prior to importing pytest.

import pytest
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# Define the subdirectories to ad to sys.path
subdirs = ["tests", "src"]

# Insert each path into sys.path if not already present
for i, subdir in enumerate(subdirs):
    path = f"/Workspace/{repo_root}/{subdir}/"
    if path not in sys.path:
        sys.path.insert(i, path)

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider", "--disable-warnings"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
