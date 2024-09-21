import pandas as pd
import json
from sqlalchemy import create_engine,text

# create mysql database commection
conn_mysql=create_engine('mysql+pymysql://root:Sur_nar26@localhost:3306/etlretailproject')

def sales_filter_data():
    query=text(""" select * from sales_staging where sale_date >= '2024-09-05' """)
    df=pd.read_sql(query,conn_mysql)
    df.to_sql('sales_filter',conn_mysql,if_exists="replace",index=False)

def sales_router_data():
    query=text(""" select * from sales_filter where region='High' union
select * from sales_filter where region='Low' """)
    df=pd.read_sql(query,conn_mysql)
    df.to_sql('sales_router',conn_mysql,if_exists="replace",index=False)

def sales_aggregator_data():
    query=text(""" select product_id,month(sale_date) as mon1,year(sale_date) as year1,
            sum(quantity*price) as sum1 from sales_staging
    group by product_id,month(sale_date),year(sale_date) """)
    df=pd.read_sql(query,conn_mysql)
    df.to_sql('sales_aggregator',conn_mysql,if_exists="replace",index=False)

def sales_joiner_data():
    query=text(""" SELECT ss.sales_id, ss.product_id, ps.product_name, ss.store_id, st.store_name, 
    ss.quantity, ss.price, (ss.quantity*ss.price) as Total_Sales, ss.sale_date
    FROM sales_staging ss
    Inner join product_staging ps
    ON ss.product_id=ps.product_id
    Inner join store_staging st
    ON ss.store_id=st.store_id """)
    df=pd.read_sql(query,conn_mysql)
    df.to_sql('sales_joiner',conn_mysql,if_exists="replace",index=False)

if __name__ =="__main__":
   sales_filter_data()
   sales_router_data()
   sales_aggregator_data()
   sales_joiner_data()


