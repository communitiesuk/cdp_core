# Key Vault Migration Pipeline

This folder contains `kv-ingestion-pipeline.yml`, a sample Azure DevOps
pipeline that copies all **secret** variables from a variable group into an
Azure Key Vault.  The pipeline runs twice: the first run discovers which
variables are secret, then the pipeline queues itself again and the second
run pushes those secrets to the Key Vault.

## Why two runs?
Azure DevOps protects secret values.  A secret variable such as `myPassword`
can only be expanded if its name appears **literally** as `$(myPassword)` in
the YAML.  If the name is built dynamically (for example inside a runtime
loop), Azure DevOps leaves it untouched and the value cannot be retrieved.

To copy every secret from a variable group you must know the names in advance
so that the YAML can emit literal `$(secretName)` references.  Because we do
not want to hard‑code the names, this pipeline performs a discovery pass, then
re‑queues itself with the secret names supplied as a parameter.  In the second
run the YAML can loop over those names at compile time and each secret value is
expanded safely and sent to the Key Vault.

## Parameters
The pipeline accepts three parameters:

| Parameter | Default | Purpose |
| --------- | ------- | ------- |
| `secretNamesJson` | `[]` | Internal parameter used to pass the list of secret names to the second run.  Leave empty when triggering manually. |
| `vgName` | `kv-tst-core-cdp-vars` | Name of the variable group that contains the secrets. |
| `kvName` | `kvcorecdp4143` | Name of the target Azure Key Vault. |

## Stage 1 – Discover
The `Discover` stage runs only when `secretNamesJson` is empty.  It calls the
Azure DevOps REST API using the pipeline's OAuth token to:

1. Obtain the ID of the variable group.
2. List the variables and select only those marked as secret.
3. Queue the same pipeline again, supplying the names as the `secretNamesJson`
   parameter so that the second run knows which secrets to expand.

## Stage 2 – Expand and push to Key Vault
When the pipeline runs with `secretNamesJson` populated, the `Expand` stage
runs.  The variable group is attached to the job so that the secret values are
available.  A compile‑time loop (`each`) generates one Azure CLI task per
secret.  Each task writes its secret to Azure Key Vault (replacing underscores
in the name with hyphens to comply with Key Vault naming rules).

Because the loop is evaluated at compile time, every task contains a literal
`$(secretName)` reference and Azure DevOps expands the secret before executing
Azure CLI.  No secret values ever appear in the pipeline logs.

## Prerequisites and notes
* Enable **Allow scripts to access the OAuth token** in the pipeline settings.
* The service connection `SPN-SP-sub-tst-ctdpslt001-001` needs permissions to read the variable
group and to write secrets to the target Key Vault.
* `secretNamesJson` is for internal use; you normally do not supply it when
manually queuing the pipeline.

This two‑stage design keeps the YAML generic while still allowing Azure DevOps
to expand secret variables, something that cannot be done if the entire process
runs only once.

