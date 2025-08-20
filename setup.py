from setuptools import setup, find_packages

import src

setup(
  name = "cdp_core",
  version = "0.2.7",
  author = "<my-author-name>",
  url = "https://<my-url>",
  author_email = "<my-author-name>@<my-organization>",
  description = "<my-package-description>",
  packages=find_packages(where="src/cdp_core"),
  package_dir={'': 'src/cdp_core'},
  package_data={'cdp_core': ['configs/*.yml']},
  entry_points={
    "console_scripts": ['run=cdp_core.main:main']
  },
  install_requires=[
    "setuptools"
  ]
)