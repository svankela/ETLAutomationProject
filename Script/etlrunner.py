import Script.Extraction as extract
import Script.Transformation as transform
import Script.Loading as load

if __name__=="__main__":
    extract.load_csv_mysql('Data/product_data.csv','product_staging')
    extract.load_csv_mysql('Data/sales_data.csv','sales_staging')
    extract.load_xml_mysql('Data/inventory_data.xml','inventory_staging')
    extract.load_json_mysql('Data/supplier_data.json','supplier_staging')
    extract.load_parquet_mysql('Data/MTCars_data.parquet','mtcars_staging')
    extract.load_oracle_mysql('select * from Store','store_staging')

    transform.sales_filter_data()
    transform.sales_router_data()
    transform.sales_aggregator_data()
    transform.sales_joiner_data()
    transform.inventory_data()

    load.load_sales_fact()
    load.load_fact_inventory()
    load.load_monthly_sales_summary()
    load.load_inventory_levels_by_store()