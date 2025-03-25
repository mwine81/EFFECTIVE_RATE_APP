import polars as pl
from polars import col as c
from app import add_grouping, agg_grouping



def awp_nadac_to_percent(data,base,is_brand):
    mean_awp = base.filter(c.is_brand == is_brand).select(c.awp.mean()).collect().item()
    mean_nadac = base.filter(c.is_brand == is_brand).select(c.nadac.mean()).collect().item()
    return(
        data
        .with_columns(c.awp / mean_awp,c.nadac / mean_nadac)
    )

base = pl.scan_parquet(r'C:\Users\mwine\3 Axis Advisors Dropbox\Matthew matt@3axisadvisors.com\datalake\projects\Jan_2025\ncpa_wv\claims\*.parquet')

def create_source_data(is_brand: int):
    return (
    base
    .filter(c.dos.dt.year() == 2024)
    .filter(c.is_brand == is_brand)
    .filter(c.awp.is_not_null())
    .filter(c.nadac.is_not_null())
    .select(c.awp,c.nadac)
    .sort(by='awp')
    .with_row_index(offset=1)
    .pipe(add_grouping,1000)
    .pipe(agg_grouping)
    .pipe(awp_nadac_to_percent,base,is_brand)
    )

# create_source_data(is_brand=0).collect().write_parquet(r'data/generic_scaler.parquet')
# create_source_data(is_brand=1).collect().write_parquet(r'data/brand_scaler.parquet')