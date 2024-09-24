import pandas as pd
import json
from sqlalchemy import create_engine,text
import logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

# create mysql database commection
conn_mysql=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')

def sales_filter_data():
    try:
        logger.info(f"Starting to load filter transformation into the table: sales_filter")
        query=text(""" select * from sales_staging where sale_date >= '2024-09-05' """)
        df=pd.read_sql(query,conn_mysql)
        df.to_sql('sales_filter',conn_mysql,if_exists="replace",index=False)
        logger.info(f"Succesfully applied filter transformation and loaded the table: sales_filter")
    except Exception as e:
        logger.error(f"Error while applying and loading into filter table: sales_filter - {e}")

def sales_router_data():
    try:
        logger.info(f"Loading router 1 data into the table: sales_low")
        query_low =text(""" select * from sales_filter where region='Low' """)
        df_low=pd.read_sql(query_low,conn_mysql)
        df_low.to_sql('sales_low',conn_mysql,if_exists="replace",index=False)
        logger.info(f"Succesfully loaded router 1 data into the table: sales_low")
    except Exception as e:    
        logger.error(f"Error while loading router 1 data into the table: sales_low - {e}")
    try:   
        logger.info(f"Loading router 2 data into the table: sales_high")
        query_high =text(""" select * from sales_filter where region='high' """)
        df_high=pd.read_sql(query_high,conn_mysql)
        df_high.to_sql('sales_high',conn_mysql,if_exists="replace",index=False)
        logger.info(f"Succesfully loaded router 2 data into the table: sales_high")
    except Exception as e:    
        logger.error(f"Error while loading router 2 data into the table: sales_high - {e}")

def sales_aggregator_data():
    try:
        logger.info(f"Loading aggregator data for the table: monthly_sales_aggregator")
        query=text(""" select product_id,month(sale_date) as month,year(sale_date) as year,
            sum(quantity*price) as total_sales from sales_filter
        group by product_id,month(sale_date),year(sale_date) """)
        df=pd.read_sql(query,conn_mysql)
        df.to_sql('monthly_sales_aggregator',conn_mysql,if_exists="replace",index=False)
        logger.info(f"Succesfully loaded aggregator data into the table: monthly_sales_aggregator")
    except Exception as e:
        logger.error(f"Error while loading aggregator data into the table: monthly_sales_aggregator - {e}")        


def sales_joiner_data():
    try:
        logger.info(f"Loading joiner data into the table: sales_details")
        query=text(""" SELECT ss.sales_id, ss.product_id, ps.product_name, ss.store_id, st.store_name, 
        ss.quantity, ss.price, (ss.quantity*ss.price) as Total_Sales, ss.sale_date
        FROM sales_filter ss
        Inner join product_staging ps
        ON ss.product_id=ps.product_id
        Inner join store_staging st
        ON ss.store_id=st.store_id """)
        df=pd.read_sql(query,conn_mysql)
        df.to_sql('sales_details',conn_mysql,if_exists="replace",index=False)
        logger.info(f"Succesfully loaded joiner data into the table: sales_details")
    except Exception as e:
        logger.error(f"Error while loading joiner data into the table: sales_details - {e}")    

def inventory_data():
    try:
       logger.info(f"Loading inventory data into the table: aggregated_inventory_data") 
       query=text("""Select store_id, sum(quantity_on_hand) as total_inventory
       From inventory_staging
       Group by store_id """)
       df=pd.read_sql(query,conn_mysql)
       df.to_sql('aggregated_inventory_data',conn_mysql,if_exists="replace",index=False)
       logger.info(f"Succesfully loaded inventory data into the table: aggregated_inventory_data")
    except Exception as e:
        logger.error(f"Error while loading joiner data into the table: aggregated_inventory_data - {e}")

    
if __name__ =="__main__":
   sales_filter_data()
   sales_router_data()
   sales_aggregator_data()
   sales_joiner_data()
   inventory_data()


