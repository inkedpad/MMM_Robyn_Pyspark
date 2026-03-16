from pathlib import Path

# ## paths for pre processing 
# RAW_DATA_PATH = Path(r"C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Data Prep\sample_data_v7.csv")
# METADATA_PATH = Path(r"C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Metric_Metadata.csv")

# OUTPUT_DATA_PATH = Path(r"C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Output_MMM\Final_Processed_Data.csv")
# OUTPUT_META_PATH = Path(r"C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Output_MMM\Final_Metadata.csv")
# OUTPUT_REGISTRY_PATH = Path(r"C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Output_MMM\Candidate_Registry.json")

# ## paths for pre modelling 
# OUTPUT_BEST_REGISTRY_PATH = Path(r'C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Output_MMM\Best_Registry.json')
# OUTPUT_RIDGE_DATA_PATH = Path(r'C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Output_MMM\Ready_For_Ridge_Data.csv')
# OUTPUT_GLOBAL_DATA_PATH = Path(r'C:\Users\ShrutiAgrawal\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM\Output_MMM\Global_ready_data.csv')


## paths for pre processing 
# RAW_DATA_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Customer_Input\sample_data_v7.csv")
# METADATA_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Customer_Input\Metric_Metadata.csv")

# OUTPUT_DATA_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Preprocessing_Output\Final_Processed_Data.csv")
# OUTPUT_META_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Preprocessing_Output\Final_Metadata.csv")
# OUTPUT_REGISTRY_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Preprocessing_Output\Candidate_Registry.json")

# # paths for pre modelling 
# OUTPUT_PREMODELLING_REGISTRY_PATH = Path(r'C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\premodelling_output\Modelling_Registry.json')
# OUTPUT_PREMODELLING_DATA_PATH = Path(r'C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\premodelling_output\modelling_data.csv')
# OUTPUT_PREMODELLING_PRICE_LOOKUP = Path(r'C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\premodelling_output\price_lookup.csv')
# OUTPUT_PREMODELLING_SPEND_LOOKUP = Path(r'C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\premodelling_output\spend_lookup.csv')

# # paths for modelling output
# OUTPUT_MODELLING_POSTERIOR_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Pymc\Codes\Codes\pymc_modelling_output\mmm_posterior.nc")
# OUTPUT_MODELLING_SCALING_META = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Pymc\Codes\Codes\pymc_modelling_output\mmm_scaling_meta.json")
# OUTPUT_MODELLING_SAMPLE_STATS = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Pymc\Codes\Codes\pymc_modelling_output\mmm_sample_stats.nc")

RAW_DATA_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Customer_Input\sample_data_v7.csv")
METADATA_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Customer_Input\Metric_Metadata.csv")

OUTPUT_DATA_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Preprocessing_Output\Final_Processed_Data.csv")
OUTPUT_META_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Preprocessing_Output\Final_Metadata.csv")
OUTPUT_REGISTRY_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Preprocessing_Output\Candidate_Registry.json")

# paths for pre modelling 
OUTPUT_PREMODELLING_REGISTRY_PATH = Path(r'C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\premodelling_output\Modelling_Registry.json')
OUTPUT_PREMODELLING_DATA_PATH = Path(r'C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\premodelling_output\modelling_data.csv')
# OUTPUT_PREMODELLING_PRICE_LOOKUP = Path(r'C:\Users\mohit\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM_Robyn_Local\MMM_Robyn\Codes\premodelling_output\price_lookup.csv')
# OUTPUT_PREMODELLING_SPEND_LOOKUP = Path(r'C:\Users\mohit\OneDrive - Polestar Solutions & Services India Private Limited\Desktop\MMM_Robyn_Local\MMM_Robyn\Codes\premodelling_output\spend_lookup.csv')

# ── ADD THESE TO YOUR EXISTING config.py ────────────────────────────────────

# Modelling output directory
ROBYN_OUTPUT_PATH = Path(r"C:\Users\mohit.bansal\Desktop\MMM_Robyn\Codes\Modelling_Output")

## other lists and elements
GROUPBY_COLS = ["Region", "Category", "Brand"]

DATE_COL = "Date"

START_FREQ = "daily"
END_FREQ = "weekly"
# Prophet
PROPHET_COUNTRY = 'IN'

# Training config — production settings for Databricks
ITERATIONS    = 2000
TRIALS        = 5
PARETO_FRONTS = 3
MIN_CANDIDATES = 200
# ITERATIONS    = 200
# TRIALS        = 2
# PARETO_FRONTS = 3
# MIN_CANDIDATES = 200

# Adstock type — 'geometric' or 'weibull_cdf' or 'weibull_pdf'
ADSTOCK = 'geometric'