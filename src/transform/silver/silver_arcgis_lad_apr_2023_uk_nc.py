from core.utils.writers import delta_writer, add_tags, add_descriptions
from core.utils.readers import read_table
from core.utils.util import cast_columns, config_reader, de_dupe, rename_columns
from pyspark.sql import functions as F
 
# column renaming
# type casting
# filtering
# normalisation
# deduplication
# enrichment?
# validation?
# partitioning?

# retrieve all parameters
dataset = dbutils.widgets.get("dataset")

# read dataset yml config
config = config_reader(dataset)

# read table
df = read_table("dev_service_line_team_1", "bronze", dataset)

# drop duplicates
df = df.dropDuplicates()

# de-duplicate based on primary / composite key
df = de_dupe(df, config["primary_key"], config["de_dupe_col"])

# cast to relevant types
df = cast_columns(df, config)

# rename columns
df = rename_columns(df, config)

# filter only for England
df = df.filter(F.col("local_authority_district_code_2023").startswith("E"))

# write to silver
delta_writer(df, "dev_service_line_team_1", "silver", config["dataset"], "overwrite")
add_tags("dev_service_line_team_1", "silver", config["dataset"], config)
add_descriptions("dev_service_line_team_1", "silver", config["dataset"], config)
