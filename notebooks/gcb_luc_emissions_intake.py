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
    BaseReader,
)
from intake.readers.transform import (
    Method,
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
        url: str,
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
    with open(Path(url), 'wb') as _f:
        _f.write(data)
    # Create datatype instance of the cached data
    return datatype_cls(url=url)
###END def write_data_to_cache

cache_file_writer: GenericFunc = GenericFunc(
    data=source_file_bytes_reader,
    func=write_data_to_cache,
    url=str(cache_dir / 'gcb_luc_emissions_cache.xlsx'),
    datatype_cls=Excel
)

# %%
# Create an intake datatype instance for the excel data source
gcb_luc_emissions_xl_file: Excel = cache_file_writer.read()

# %%
# Create a dict of readers, one for each model/datasheet
model_names: list[str] = ['BLUE', 'H&C2023', 'OSCAR']
pl_excel_readers: dict[str, BaseReader] = {
    _model_name: PolarsExcel(
        data=gcb_luc_emissions_xl_file,
        sheet_name=_model_name,
        read_options={'header_row': 7, 'skip_rows': 1},
        metadata={'unit': 'GtC/yr'},
    ).Method(method_name='rename', mapping={'unit: Tg C/year': 'year'}) \
        .Method(
            pl.exclude(['year', 'model']).cast(float),
            method_name='with_columns',
        )
    for _model_name in model_names
}

# %%
# Leave intake space and work with polars for the remaining steps. Make
# a dict of DataFrames.
pl_dataframes: dict[str, pl.DataFrame] = {
    _model_name: _pl_reader.read() for _model_name, _pl_reader in pl_excel_readers.items()
}

# %%
# Add a column with the model name to each DataFrame, and then concatenate them.
# Break lines so that no line has more than 80 characters.
pl_dataframes = {
    _model_name: _pl_df.with_columns(pl.lit(_model_name).alias('model'))
    for _model_name, _pl_df in pl_dataframes.items()
}
common_df: pl.DataFrame = pl.concat(pl_dataframes.values(), how='vertical')

# %%
# Group `common_df` by 'year' and calculate the standard deviation divided by
# the mean for each column.
start_year: int = 2005
sel: pl.Expr = pl.exclude('model')
rel_std_df: pl.DataFrame = common_df.group_by('year').agg(
    sel.std() / sel.mean().abs()
).sort(by='year').filter(pl.col('year') >= start_year)

# %%
# Do the same calculation as above, but using the difference between max and min
# instead of std
rel_range_df: pl.DataFrame = common_df.group_by('year').agg(
    (sel.max() - sel.min()) / sel.mean().abs()
).sort(by='year').filter(pl.col('year') >= start_year)