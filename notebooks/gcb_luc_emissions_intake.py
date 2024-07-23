"""Script to explore intake reading Global Carbon Budget LUC data with intake"""

# %%
# Import libraries
from pathlib import Path
from typing import Type

import intake
from intake.readers import (
    FileData,
    FileByteReader,
    Excel,
    PolarsExcel,
)
from intake.readers.convert import GenericFunc
import polars as pl

# %%
# URL for the GCB land-use change emissions Excel file
gcb_luc_url: str = r'https://data.icos-cp.eu/licence_accept' \
    + '?ids=%5B%22PjBw_YJoPb_jPh7OpeAYgjo3%22%5D'

# %%
# Get recommendations for available readers from intake
reader_recommendations = intake.recommend(gcb_luc_url)

# %% 
# Create intake datatype instance for the excel data source
source_file_datatype_reader: FileData = FileData(url=gcb_luc_url)
source_file_bytes_reader: FileByteReader = FileByteReader(data=source_file_datatype_reader)

# %%
# Define a GenericFunction converter that will write data to cache, and return
# it as a datatype instance of the specified type
cache_dir: Path = Path(__file__).parent / 'data_cache'

def write_data_to_cache(
        data: bytes,
        cache_file: Path,
        datatype_cls: Type[FileData] = FileData,
) -> FileData:
    """Write data to cache and return a FileData instance.

    Parameters
    ----------
    data : FileByteReader
        Data to write to the cache.
    cache_file : Path
        Path to the cache file.
    datatype_cls : Type[FileData], optional
        Type of the returned datatype instance, by default FileData.

    Returns
    -------
    FileData
        Datatype instance of the cached data.
    """
    # Write data to file
    with open(cache_file, 'wb') as _f:
        _f.write(data)
    # Create datatype instance of the cached data
    return datatype_cls(url=cache_file)
###END def write_data_to_cache

cache_file_writer: GenericFunc = GenericFunc(
    data=source_file_bytes_reader,
    func=write_data_to_cache,
    cache_file=cache_dir / 'gcb_luc_emissions_cache.xlsx',
    datatype_cls=Excel
)
