#Extract different type of source files and load into staging(MySQL)
import pandas as pd
from sqlalchemy import create_engine,text
import json
import pyarrow.parquet as pq
import logging
from Script.config import MYSQL_HOST,MYSQL_PORT,MYSQL_USER,MYSQL_PASSWORD,MYSQL_DATABASE,ORACLE_USER,ORACLE_PASSWORD,ORACLE_HOST,ORACLE_PORT,ORACLE_SERVICE


# Set up logging configuration
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',        # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO    # Set the logging level
)
logger = logging.getLogger(__name__)


#conn_mysql=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')
conn_mysql = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

#conn_oracle=create_engine("oracle+cx_oracle://system:admin@localhost:1521/xe")
conn_oracle = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

#Load CSV files to Stage(MySQL)
def load_csv_mysql(file_path,table_name):
    try:
        logger.info(f"Starting to load CSV file: {file_path} into table: {table_name}")
        df=pd.read_csv(file_path)
        df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)
        logger.info(f"Succcessfully loaded CSV file: {file_path} into table: {table_name}")
    except Exception as e:
         logger.error(f"Error while loading CSV file: {file_path} into table: {table_name} - {e}")  

 #Load JSON file to Stage(MySQL)
def load_json_mysql(file_path,table_name):
    try:
        logger.info(f"Starting to load JSON file: {file_path} into table: {table_name}")
        df=pd.read_json(file_path)
        df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)  
        logger.info(f"Succcessfully loaded JSON file: {file_path} into table: {table_name}")
    except Exception as e:
        logger.error(f"Error while loading JSON file: {file_path} into table: {table_name} - {e}")

 #Load XML file to Stage(MySQL)
def load_xml_mysql(file_path,table_name):
    try:
        logger.info(f"Starting to load XML file: {file_path} into table: {table_name}")
        df=pd.read_xml(file_path,xpath='.//item')
        df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)
        logger.info(f"Succcessfully loaded XML file: {file_path} into table: {table_name}")
    except Exception as e:
        logger.error(f"Error while loading XML file: {file_path} into table: {table_name} - {e}")


#Load PARQUET file to Stage(MySQL)
def load_parquet_mysql(file_path,table_name):
    try:
        logger.info(f"Starting to load PARQUET file: {file_path} into table: {table_name}")
        df=pd.read_parquet(file_path)
        df.to_sql(table_name,conn_mysql,if_exists='replace',index=False)  
        logger.info(f"Succcessfully loaded PARQUET file: {file_path} into table: {table_name}")
    except Exception as e:
        logger.error(f"Error while loading PARQUET file: {file_path} into table: {table_name} - {e}")    

#Load Oracle data to Stage(MySQL)
def load_oracle_mysql(query,table_name):
    try:
        logger.info(f"Starting to load Oracle data using query: {query} into table: {table_name}")
        df=pd.read_sql(query,conn_oracle)
        df.to_sql(table_name,conn_mysql,if_exists="replace",index=False) 
        logger.info(f"Successfully loaded Oracle data into table: {table_name}")
    except Exception as e:
        logger.error(f"Error while loading Oracle data into table: {table_name} - {e}")

if __name__ =="__main__":

    # Loading different types of data into MySQL staging
    logger.info("Data Extraction started for product file......")
    load_csv_mysql('product_data.csv','product_staging')
    logger.info("Data Extraction completed successfully for product file......")

    logger.info("Data Extraction started for Sales file......")
    load_csv_mysql('sales_data.csv','sales_staging')
    logger.info("Data Extraction completed successfully for Sales file......")

    logger.info("Data Extraction started for Inventory file......")
    load_xml_mysql('inventory_data.xml','inventory_staging')
    logger.info("Data Extraction completed successfully for Inventory file......")

    logger.info("Data Extraction started for Supplier file......")
    load_json_mysql('supplier_data.json','supplier_staging')
    logger.info("Data Extraction completed successfully for Supplier file......")

    logger.info("Data Extraction started for mtcars file......")
    load_parquet_mysql('MTCars_data.parquet','mtcars_staging')
    logger.info("Data Extraction completed successfully for mtcars file......")

    logger.info("Data Extraction started for store data......")
    load_oracle_mysql('select * from Store','store_staging')
    logger.info("Data Extraction completed successfully for store data......")    