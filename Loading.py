import pandas as pd
from sqlalchemy import create_engine,text
import json
import logging
# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

mysql_con=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')

def load_sales_fact():
    try:
        logger.info(f"Loading started for the table fact_sales")
        query=text(""" 
               INSERT IGNORE INTO fact_sales(sales_id, product_id,store_id,quantity,total_sales,sale_date) 
               select sales_id, product_id,store_id,quantity,total_sales,sale_date
               from sales_details; 
               """)
        # Execute the insert query
        with mysql_con.connect() as conn:
            conn.execute(query)
            conn.commit()
        logger.info(f"Data successfully loaded into Fact table fact_sales from sale_details")
    except Exception as e:
        logger.error(f"Error while loading into the table fact_sales - {e}") 


def load_fact_inventory():
    try:
        logger.info(f"Loading started for the table fact_inventory")
        query=text(""" 
               INSERT IGNORE INTO fact_inventory(product_id,store_id,quantity_on_hand,last_updated) 
               select product_id,store_id,quantity_on_hand,last_updated
               from inventory_staging; 
               """)
        # Execute the insert query
        with mysql_con.connect() as conn:
            conn.execute(query)
            conn.commit()
        logger.info(f"Data successfully loaded into Fact table fact_inventory")
    except Exception as e:
        logger.error(f"Error while loading into the fact table fact_inventory - {e}") 

def load_monthly_sales_summary():
    try:
        logger.info(f"Loading started for the table monthly_sales_summary")
        query=text(""" 
               INSERT IGNORE INTO monthly_sales_summary(product_id,month,year,total_sales) 
               select product_id,month,year,total_sales
               from monthly_sales_aggregator; 
               """)
        # Execute the insert query
        with mysql_con.connect() as conn:
            conn.execute(query)
            conn.commit()
        logger.info(f"Data successfully loaded into table monthly_sales_summary")
    except Exception as e:
        logger.error(f"Error while loading into the table monthly_sales_summary - {e}") 


def load_inventory_levels_by_store():
    try:
        logger.info(f"Loading started for the table inventory_levels_by_store")
        query=text(""" 
               INSERT IGNORE INTO inventory_levels_by_store(store_id,total_inventory) 
               select store_id,total_inventory
               from aggregated_inventory_data; 
               """)
        # Execute the insert query
        with mysql_con.connect() as conn:
            conn.execute(query)
            conn.commit()
        logger.info(f"Data successfully loaded into table inventory_levels_by_store")
    except Exception as e:
        logger.error(f"Error while loading into the table inventory_levels_by_store - {e}") 


if __name__=="__main__":
    load_sales_fact()
    load_fact_inventory()
    load_monthly_sales_summary()
    load_inventory_levels_by_store()  
