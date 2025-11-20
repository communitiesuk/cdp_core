# dev bundle
This bundle is intended for local development and testing.
It uses the shared interactive cluster in your Databricks workspace, providing a much faster feedback loop than the job compute used in CI/CD bundles.

Rather than modifying an existing DAB’s job cluster to use a shared cluster ID — or picking apart full CI/CD bundles just to test small components — this lightweight development bundle offers a cleaner alternative.
You can run bite-sized tasks independently and iterate quickly without impacting production configurations.

## Getting Started

This bundle is intended for development and testing purposes. It utilises the shared cluster in your Databricks workspace enabling a must quicker development cycle than the Job compute used in the ci/cd bundles.

Simply add tasks to the resources.yml in this bundle and deploy it to your Databricks workspace. 

You can test using different built wheels by pasting them into the wheels/*.whl folder.


## Documentation

- For information on using **Databricks Asset Bundles in the workspace**, see: [Databricks Asset Bundles in the workspace](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-bundles)
- For details on the **Databricks Asset Bundles format** used in this asset bundle, see: [Databricks Asset Bundles Configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/reference)
