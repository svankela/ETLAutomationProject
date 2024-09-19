import pandas as pd
import json
from sqlalchemy import create_engine,text

# create mysql database commection
mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/enterpriseretaildwh')


def load_csv_mysql(file_path,table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)

def load_xml_mysql(file_path,table_name):
    df = pd.read_xml(file_path,xpath='.//item')
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)

def load_json_mysql(file_path,table_name):
    df = pd.read_json(file_path)
    df.to_sql(table_name,mysql_engine,if_exists='replace',index=False)



   

    


'''
# for sales_data.csv
df = pd.read_csv('sales_data.csv')
df.to_sql('sales_staging',mysql_engine,if_exists='replace',index=False)

# for product_data.csv
df = pd.read_csv('product_data.csv')
df.to_sql('product_staging',mysql_engine,if_exists='replace',index=False)

# for inventory_data.xml
df = pd.read_xml('inventory_data.xml',xpath='.//item')
df.to_sql('inventory_staging',mysql_engine,if_exists='replace',index=False)

# for supplier_data.xml
df = pd.read_json('supplier_data.json')
df.to_sql('supplier_staging',mysql_engine,if_exists='replace',index=False)
'''
'''
load_csv_mysql('sales_data.csv','sales_staging')
load_csv_mysql('product_data.csv','product_staging')
load_xml_mysql('inventory_data.xml','invenstory_staging')
load_json_mysql('supplier_data.json','supplier_staging')
'''

if __name__=="__main__":
    load_csv_mysql('sales_data.csv','sales_staging')
    load_csv_mysql('product_data.csv','product_staging')
    load_xml_mysql('inventory_data.xml','invenstory_staging')
    load_json_mysql('supplier_data.json','supplier_staging')