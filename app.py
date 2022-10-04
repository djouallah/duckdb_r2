from distutils.log import error
import streamlit as st
import boto3, os
import duckdb
SQL = st.text_input('Write a SQL Query', 'select  distinct filename, count(*)  from lineitem group by 1')
try :
   con=duckdb.connect()
   con.execute("create or replace view lineitem as select * from parquet_scan('/lineitem/*/*.parquet',filename=true,HIVE_PARTITIONING=1)")
   df = con.execute(SQL).df()
   st.write(df)
except :
 st.write("Your SQL is not correct")


def download() :
 s3 = boto3.resource('s3',
  endpoint_url = st.secrets["endpoint_url_secret"] ,
  aws_access_key_id = st.secrets["aws_access_key_id_secret"],
  aws_secret_access_key = st.secrets["aws_secret_access_key_secret"]
  )
 bucket = s3.Bucket('delta')

 remote=[]
 for item in bucket.objects.all():
   remote.append(item.key)
 #st.write(remote)
 local=[]
 for path, subdirs, files in os.walk('lineitem'):
    for name in files:
        local.append(os.path.join(path,name).replace("\\","/"))
 #st.write(local)
 l=set(remote) - set(local)
 for s3_object in l:
    path, filename = os.path.split(s3_object)
    os.makedirs(path)
    bucket.download_file(s3_object, path +"/"+filename)
    st.write(path +"/"+filename)
#download()
