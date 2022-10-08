import streamlit as st
import streamlit.components.v1 as components
from pivottablejs import pivot_ui
import duckdb
st.set_page_config(
    page_title="Example of using DuckDB",
    page_icon="✅",
    layout="wide",
                  )
col1, col2 = st.columns([3, 1])
SQL = st.text_input('Write a SQL Query','select  hour(tpep_pickup_datetime) as hour , sum(total_amount) as sum  from "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-06.parquet"  group by 1 order by sum desc')
try :
   con=duckdb.connect()
   con.execute("create or replace view lineitem as select * from parquet_scan('lineitem/*/*.parquet',filename=true,HIVE_PARTITIONING=1)")
   con.execute("install httpfs; load httpfs")
   df = con.execute(SQL).df()
except :
 st.write("Your SQL is not correct")
st.write("Build your chart")
t = pivot_ui(df)

with open(t.src) as t:
    components.html(t.read(), width=900, height=1000, scrolling=True)
################################################################################
def convert_df(df):
            # IMPORTANT: Cache the conversion to prevent computation on every rerun
            return df.to_csv().encode('utf-8')

csv = convert_df(df)
col2.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='large_df.csv',
            mime='text/csv',
        )
