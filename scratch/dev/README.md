# dev bundle

## Getting Started

This bundle is intended for development and testing purposes. It utilises the shared cluster in your Databricks workspace enabling a must quicker development cycle than the Job compute used in the ci/cd bundles.

Simply add tasks to the resources.yml in this bundle and deploy it to your Databricks workspace. 

You can test using different built wheels by pasting them into the wheels/*.whl folder.


## Documentation

- For information on using **Databricks Asset Bundles in the workspace**, see: [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Databricks Asset Bundles format** used in this asset bundle, see: [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
