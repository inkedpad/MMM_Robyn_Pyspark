# config.py — Databricks version

# Input paths — files you uploaded to Databricks FileStore
RAW_DATA_PATH = "/Workspace/Users/mohitbansal77b@gmail.com/MMM_Robyn_Pyspark/Robyn_pyspark_innerfolder/Codes/Customer_Input/sample_data_v7.csv"
METADATA_PATH = "/Workspace/Users/mohitbansal77b@gmail.com/MMM_Robyn_Pyspark/Robyn_pyspark_innerfolder/Codes/Customer_Input/Metric_Metadata.csv"

# Output paths — write back into Workspace
OUTPUT_DATA_PATH     = "/Workspace/Users/mohitbansal77b@gmail.com/MMM_Robyn_Pyspark/Robyn_pyspark_innerfolder/Codes/Preprocessing_Output/Final_Processed_Data"
OUTPUT_META_PATH     = "/Workspace/Users/mohitbansal77b@gmail.com/MMM_Robyn_Pyspark/Robyn_pyspark_innerfolder/Codes/Preprocessing_Output/Final_Metadata.csv"
OUTPUT_REGISTRY_PATH = "/Workspace/Users/mohitbansal77b@gmail.com/MMM_Robyn_Pyspark/Robyn_pyspark_innerfolder/Codes/Preprocessing_Output/Candidate_Registry.json"

# Other constants — unchanged
GROUPBY_COLS  = ["Region", "Category", "Brand"]
DATE_COL      = "Date"
START_FREQ    = "daily"
END_FREQ      = "weekly"

PROPHET_COUNTRY = 'IN'
ITERATIONS      = 2000
TRIALS          = 5
PARETO_FRONTS   = 3
MIN_CANDIDATES  = 200
ADSTOCK         = 'geometric'