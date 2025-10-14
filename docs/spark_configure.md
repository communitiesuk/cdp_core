# Databricks Environment-Aware Notebook Guide

This template shows how to write **Databricks notebooks** that can run **both locally (via Databricks Connect)** and **inside Databricks workspaces** with **no code changes**.

It automatically detects the runtime environment and creates the correct `SparkSession`.

---

## 🧠 Overview

| Mode | Environment | Spark Initialization | Console Output |
|------|--------------|----------------------|----------------|
| Local development | Databricks Connect (v14+) | Connects via your local Databricks CLI config | `Using Databricks Connect session` |
| Workspace | Databricks cluster / job | Uses the active cluster’s SparkSession | `Using cluster SparkSession` |

---

## ⚙️ Prerequisites

### 1. Install Databricks Connect (v14+ unified CLI)

```bash
pip install databricks-connect 
```
and for running notebooks locally 

```bash
pip install ipykernel
```

### 2. Configure the CLI

```bash
databricks configure
```

Your credentials are stored in `~/.databrickscfg` (no separate connect file needed).

### 3. Verify connection

```bash
databricks connect test
```

---

## 🧩 Example usage

All notebooks should import the shared utility function:

```python
from utils.spark import get_spark

spark = get_spark()
df = spark.range(5)
df.show()
```

✅ No modification is required when switching between local and workspace environments.

---

## 🧱 How it works

- **Detection:**  
  The presence of the `dbruntime` module indicates code is running inside a Databricks cluster.  
  Its absence means the notebook is running locally.

- **Session creation:**  
  - **In Databricks:** uses `SparkSession.builder.getOrCreate()`  
  - **Locally:** uses `DatabricksSession` from `databricks.connect`

---

## 🧰 Troubleshooting

| Issue | Likely Cause | Fix |
|--------|---------------|-----|
| `ModuleNotFoundError: No module named 'databricks.connect'` | Databricks Connect not installed locally | `pip install databricks-connect` |
| `Permission denied` or `Invalid token` | Expired CLI token | `databricks configure` |
| `cluster not found` | No default cluster set | Set `DATABRICKS_CLUSTER_ID` in your env or config |


---

**Maintainer note:**  
This pattern is fully compatible with **Databricks Connect v14+** and **Unity Catalog**-enabled workspaces.
