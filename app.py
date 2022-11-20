import streamlit as st
import duckdb
st.set_page_config(
    page_title="Example of using DuckDB with http files",
    page_icon="✅",
    layout="wide",
                  )
col1, col2 = st.columns([3, 1])
SQL = st.text_area('Write a SQL Query','''SELECT DISTINCT passenger_count    , ROUND (SUM (fare_amount),0) as TotalFares     , ROUND (AVG (fare_amount),0) as AvgFares
from read_parquet(['https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-08.parquet','https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-07.parquet',
'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-06.parquet'])
group by all order by avgfares desc
 ''')
try :
   con=duckdb.connect()
   con.execute("install httpfs; load httpfs")
   df = con.execute(SQL).df()
   st.write(df)
except Exception as er:
 st.write(er)
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
