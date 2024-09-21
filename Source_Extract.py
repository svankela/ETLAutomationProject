#Extract different type of source files and load into staging(MySQL)
import pandas as pd
from sqlalchemy import create_engine,text
import json
import pyarrow.parquet as pq

conn_mysql=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')

conn_oracle=create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")

#Load CSV files to Stage(MySQL)
def load_csv_mysql(file_path,table_name):
    df=pd.read_csv(file_path)
    df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)

 #Load JSON file to Stage(MySQL)
def load_json_mysql(file_path,table_name):
    df=pd.read_json(file_path)
    df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)   

 #Load XML file to Stage(MySQL)
def load_xml_mysql(file_path,table_name):
    df=pd.read_xml(file_path,xpath='.//item')
    df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)   

#Load PARQUET file to Stage(MySQL)
def load_parquet_mysql(file_path,table_name):
    df=pd.read_parquet(file_path)
    df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)  

#Load Oracle data to Stage(MySQL)
def load_oracle_mysql(query,table_name):
    df=pd.read_sql(query,conn_oracle)
    df.to_sql(table_name,conn_mysql,if_exists="replace",index=False) 

if __name__ =="__main__":
    load_csv_mysql('product_data.csv','product_staging')
    load_csv_mysql('sales_data.csv','sales_staging')
    load_xml_mysql('inventory_data.xml','inventory_staging')
    load_json_mysql('supplier_data.json','supplier_staging')
    load_parquet_mysql('MTCars_data.parquet','mtcars_staging')
    load_oracle_mysql('select * from Store','store_staging')
