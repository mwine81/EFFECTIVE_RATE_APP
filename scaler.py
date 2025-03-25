import polars as pl
from polars import col as c

def grp_scaler(data,sample_size):
    end = data.select(pl.len()).collect().item()
    grp_frame = (pl.DataFrame({'index':pl.linear_spaces(1,end,sample_size,closed='left',eager=True)}).explode('index').with_columns(c.index.floor().cast(pl.UInt32)).with_row_index('grp',offset=1)).lazy()
    return grp_frame

def add_grouping(data, grp_size):
    grp_frame = grp_scaler(data,grp_size)
    return (
        data
        .join(grp_frame,on='index',how='left')
        .sort(by='index')
        .with_columns(c.grp.forward_fill())
     )

def agg_grouping(data):
    return(
        data
        .group_by(c.grp)
        .agg(c.awp.mean(),c.nadac.mean())
        .rename({'grp': 'index'})
    )


def awp_nadac_to_percent(data,base,is_brand):
    mean_awp = base.filter(c.is_brand == is_brand).select(c.awp.mean()).collect().item()
    mean_nadac = base.filter(c.is_brand == is_brand).select(c.nadac.mean()).collect().item()
    return(
        data
        .with_columns(c.awp / mean_awp,c.nadac / mean_nadac)
    )

#add base rxfile to extract scaling data such as wv dataset
def create_source_data(is_brand: int, base):
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

if __name__ == '__main__':

    # create_source_data(is_brand=0).collect().write_parquet(r'data/generic_scaler.parquet')
    # create_source_data(is_brand=1).collect().write_parquet(r'data/brand_scaler.parquet')