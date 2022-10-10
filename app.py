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
SQL = st.text_input('Write a SQL Query','select * from  "https://github.com/djouallah/tcph_web/raw/main/lineitem.parquet" limit 300 ')
try :
   con=duckdb.connect()
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
