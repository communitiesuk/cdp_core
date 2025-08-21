from setuptools import setup, find_packages
import os 

version = os.getenv('BUILD_VERSION', '0.0.1')

setup(
  name = "cdp_core",
  version = version,
  author = "<my-author-name>",
  url = "https://<my-url>",
  author_email = "<my-author-name>@<my-organization>",
  description = "<my-package-description>",
  packages=find_packages(where="src"),
  package_dir={'': 'src'},
  package_data={'cdp_core': ['configs/*.yml']},
  entry_points={
    "console_scripts": ['run=cdp_core.main:main']
  },
  install_requires=[
    "setuptools"
  ]
)