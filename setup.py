from setuptools import setup, find_packages
import os 

version = os.getenv('BUILD_VERSION', '0.0.1')

setup(
  name = "cdp_core",
  version = version,
  packages=find_packages(where="src"),
  package_dir={'': 'src'},
  package_data={'cdp_core': ['configs/*.yml']},
  entry_points={
    "console_scripts": ['run=cdp_core.main:main']
  },
  # to prevent version mismatch, we exclude any packages that are already installed in the cluster
  # to identify which dependencies are already installed in the chosen cluster runtime, visit the following page
  # https://docs.databricks.com/aws/en/release-notes/runtime
  install_requires=["geopandas==1.1.1"] # this is not required but used as a test case
)