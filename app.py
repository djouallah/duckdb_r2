import streamlit as st
import duckdb
st.set_page_config(
    page_title="Example of using DuckDB with http files",
    page_icon="✅",
    layout="wide",
                  )
col1, col2 = st.columns([3, 1])
SQL = st.text_input('Write a SQL Query','''select * from parquet_metadata('https://s3-eu-west-1.amazonaws.com/pstorage-ucl-2748466690/16218152/complete.parquet') limit 5 ''')
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
