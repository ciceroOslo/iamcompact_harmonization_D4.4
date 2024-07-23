"""Script to explore intake reading Global Carbon Budget LUC data with intake"""

# %%
# Import libraries
from pathlib import Path

import intake
import polars as pl

# %%
# URL for the GCB land-use change emissions Excel file
gcb_luc_url: str = r'https://data.icos-cp.eu/licence_accept' \
    + '?ids=%5B%22PjBw_YJoPb_jPh7OpeAYgjo3%22%5D'

# %%
# Get recommendations for available readers from intake
reader_recommendations = intake.recommend(gcb_luc_url)

# %% 
# Create intake datadtype instance for the excel data source, and for a cached
# file
xldata_source: intake.datatypes.Excel = intake.datatypes.Excel(gcb_luc_url)
cache_dir: Path = Path(__file__).parent / 'data_cache'
xldata_cached: 

# %%
# Then load the data
loadeddata = plxlreader.read()
