
import polars as pl
from polars import col as c
import streamlit as st

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

brand_scaler = pl.scan_parquet(r'data\brand_scaler.parquet')
generic_scaler = pl.scan_parquet(r'data\generic_scaler.parquet')

col1,col2 = st.columns(2)
with col1:
    generic_awp = st.number_input('generic_awp',value= 183.44)
    generic_nadac = st.number_input('generic_nadac',value= 9.76)
    brand_awp = st.number_input('brand_awp',value= 924.51)
    brand_nadac = st.number_input('brand_nadac',value= 741.03)
with col2:
    gdr = st.number_input('Generic Dispense Rate',value= 88)
    ger = st.number_input('Generic Effective Rate',value= 88)
    ber = st.number_input('Brand Effective Rate',value= 20)
    fee = st.number_input('Dispensing Fee',value= .40)

brand = (
brand_scaler
.pipe(add_grouping,100-gdr)
.pipe(agg_grouping)
.with_columns(c.awp * brand_awp,c.nadac * brand_nadac)
.with_columns(icp = c.awp * (1-(ber/100)), fee = fee)
.with_columns(margin = c.icp+c.fee - c.nadac)
.with_columns(b_g = pl.lit('Brand'))
)
#
generic = (
generic_scaler
.pipe(add_grouping,ger)
.pipe(agg_grouping)
.with_columns(c.awp * generic_awp,c.nadac * generic_nadac)
.with_columns(icp = c.awp * (1-(ger/100)),fee=fee)
.with_columns(margin = c.icp+c.fee - c.nadac)
.with_columns(b_g = pl.lit('Generic'))
)

data = pl.concat([brand,generic]).sort(by='margin').with_columns(cum_sum = c.margin.cum_sum()).drop('index').with_row_index('index')
total_margin = data.select(c.margin.sum().round(2)).collect().item()
st.text(f'Total Margin per 100 Rxs ${total_margin} or ${round(total_margin/100,2)} per Rx')
st.bar_chart(data.collect(),x='index',y='cum_sum',color='b_g')
