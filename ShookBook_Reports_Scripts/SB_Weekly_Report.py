import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv 



load_dotenv()


#DBdetails
DB_CONFIG = {
   'driver': os.getenv('DB_DRIVER'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME')
}


#this is a dictionary object that contains the queries we want to run
#the keys will be the names of the sheets in the Excel file (named after the query name)
#the values are the SQL queries

#the dictionary will look like this:
#ALL_QUERIES = {
#    "newCust-totalOrderWithQuant": """
#    ...
#    """
#     }

ALL_QUERIES = {

    "newCust-totalOrderWithQuant": """
select
    o.customer_id,
    o.store_id,
    s.name,
    MIN(o.delivery_date) as "first order",
    MAX(o.delivery_date) as "last order",
    COUNT(o.delivery_date) as "number of orders",
    avg(o.sum) as avg_sum,
    o.first_name,
    o.last_name,
    o.phone
from orders o
         join store s on s.id = o.store_id
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  AND o.store_id NOT IN ('84', '85')
  AND o.status NOT IN(4,11,1)
group by o.customer_id, o.store_id, s.name, o.first_name, o.last_name, o.phone
having MIN(o.delivery_date) >= '{CUSTOMER_CUTOFF_DATE}'
order by count(o.id) desc;
""",

"packing": """
select
    o.id,
    o.wp_id,
    o.delivery_date,
    o.delivery_window,
    o.status,
    o.clearing_status,
    o.dhl_package_number,
    o.is_deleted,
    o.sum as "order sum",
-- o.snh as "delivery fee",
    SUM(op.quantity_needed) "total_order_quantity_needed",
    SUM(op.quantity) as "total_order_quantity",
    SUM(CASE WHEN p.packing_action = 1 THEN op.quantity ELSE 0 END) AS type_1_quantity,
    o.store_id,
    s.name,
    o.packing_worker_id,
    wp.first_name,
    wp.last_name
from orders o
         join order_product op on op.order_id = o.id
         join store s on s.id = o.store_id
         join products p on p.id = op.product_id
         left join workers wp on wp.id = o.packing_worker_id
         left join workers wd on wd.id = o.dispatcher_worker_id
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and o.status NOT IN (4,11)
group by o.id,o.delivery_date
order by o.delivery_date desc;
""",

"packing by employee": """

select
    o.packing_worker_id,
    wp.first_name,
    wp.last_name,
    count(DISTINCT o.id),
    sum(op.quantity),
    sum(DISTINCT o.sum)
from orders o
         join order_product op on op.order_id = o.id
         left join workers wp on wp.id = o.packing_worker_id
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and o.status NOT IN (4,11)
group by o.packing_worker_id
order by count(o.id) desc;
""",

"weekly - missing in orders": """
select
    o.store_id,
    s.name,
    o.id as "order id",
    o.sum as "order sum",
    op.quantity_needed as "order quantity",
    op.quantity as "delivered quantity",
    op.quantity_needed - op.quantity as "missing quantity",
    op.product_id,
    p.name,
    p.name_heb,
    p.name_weight,
    o.packing_worker_id,
    w.first_name,
    w.last_name
from orders o
         join order_product op on op.order_id = o.id
         join products p on p.id = op.product_id
         join store s on s.id = o.store_id
         join workers w on w.id = o.packing_worker_id
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and op.quantity_needed > op.quantity
  and o.store_id NOT IN (82,84,85)
  and o.status NOT IN (4,11)
order by o.id desc;
""",

"weekly products": """
#weekly products
SELECT
    p.id AS "product id",
    p.name_heb AS "product name",
    p.category_id AS "category id",
    c1.name AS "category name",
    p.price AS "shookbook price",
    p.low_cost_price AS "990 price",
    op.unit_price AS "order price",
    AVG(op.unit_price) AS "average product price",
    SUM(op.quantity_needed) AS "order quantity by client",
    SUM(op.quantity_needed * op.unit_price) AS "total for q",
    SUM(op.quantity) AS "order quantity billed",
    SUM(op.quantity * op.unit_price) AS "total for q",
    SUM(op.quantity_delivered) AS "order delivered",
    SUM(op.quantity_delivered * op.unit_price) AS "total for q",
    SUM(op.quantity_replaceable) AS "order replaceable",
    SUM(op.quantity_replaceable * op.unit_price) AS "total for q"
FROM order_product op
         INNER JOIN orders o ON op.order_id = o.id
         INNER JOIN products p ON op.product_id = p.id
         INNER JOIN categories c1 ON p.category_id = c1.id
WHERE o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
    and o.store_id NOT IN ('84', '85')
    and o.status NOT IN (4,11)
GROUP BY p.id, p.name
ORDER BY SUM(op.quantity_needed) DESC;
""",

"weekly orders": """
#weekly orders
select
    o.customer_id,
    MIN(o.delivery_date) as "first order",
    MAX(o.delivery_date) as "last order",
    avg(o.sum),
    count(o.id),
    o.first_name,
    o.last_name,
    o.phone
from orders o
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and o.store_id NOT IN ('84', '85')
  and o.status NOT IN (4,11)
group by o.customer_id
order by count(o.id) desc;
""",

"2nd month orders": """
#2nd month orders
select
    o.customer_id,
    MIN(o.delivery_date) as "first order",
    MAX(o.delivery_date) as "last order",
    avg(o.sum),
    count(o.id),
    o.first_name,
    o.last_name,
    o.phone
from orders o
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and o.store_id NOT IN ('84', '85')
  and o.status NOT IN (4,11)
group by o.customer_id
order by count(o.id) desc;
""",

"yearly orders": """
select
    o.customer_id,
    MIN(o.delivery_date) as "first order",
    MAX(o.delivery_date) as "last order",
    avg(o.sum),
    count(o.id),
    o.first_name,
    o.last_name,
    o.phone
from orders o
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and o.store_id NOT IN ('84', '85')
  and o.status NOT IN(4,11)
group by o.customer_id
order by count(o.id) desc;
""",

"yearlyOrders-orderedLastWeek": """
select
    o.customer_id,
    MIN(o.delivery_date) as "first order",
    MAX(o.delivery_date) as "last order",
    avg(o.sum),
    count(o.id),
    o.first_name,
    o.last_name,
    o.phone
from orders o
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and o.store_id NOT IN ('84', '85')
  and o.status NOT IN (4,11)
group by o.customer_id
having MAX(o.delivery_date) >= '{CUSTOMER_CUTOFF_DATE}'
order by count(o.id) desc;
""",

"weekly with coupons": """
#weekly with coupons
select o.id, o.customer_id, o.created_date, o.sum, o.discount_sum, o.discount_promotions, o.coupons
from orders o
where o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
  and o.store_id NOT IN ('84', '85')
  and o.status NOT IN(4,11)
order by o.id desc;
""",

"weekly by zones": """
#weekly by zones
select o.store_id, s.name, cg.description, count(o.id), sum(o.sum)
from orders o
         join cities c on c.name = o.city
         join city_groups cg on cg.id = c.city_group_id
         join store s on s.id = o.store_id
WHERE o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
-- and o.store_id NOT IN (84)
    and cg.description != ''
    and o.status NOT IN (4,11)
group by o.store_id, s.name, cg.description;
-- order by cg.description ASC
""",

"weekly_zones_by_desc": """
select o.store_id, s.name, count(o.id), cg.description, sum(o.sum)
from orders o
         join cities c on c.name = o.city
         join city_groups cg on cg.id = c.city_group_id
         join store s on s.id = o.store_id
    WHERE o.delivery_date BETWEEN '{START_DATE}' and '{END_DATE}'
    and o.store_id NOT IN (84)
    and cg.description != ''
    and o.status NOT IN (4,11)
group by
      cg.description
order by count(o.id) desc
""",

}


def get_date_range():
    """Request date range from the user."""
    print("Please enter the date range (format: YYYY-MM-DD).")
    
    # default is automatic calculation of the last 7 days
    use_default = input("Use the last 7 days? (y/n, default 'yes'): ")
    if use_default.lower() in ('', 'y', 'yes', 'כן'):
        end_date_dt = datetime.now()
        start_date_dt = end_date_dt - timedelta(days=7)
        start_date_str = start_date_dt.strftime('%Y-%m-%d')
        end_date_str = end_date_dt.strftime('%Y-%m-%d')
        cutoff_date_str = start_date_str
        print(f"Using date range: {start_date_str} to {end_date_str}")
        print(f"Using cutoff date: {cutoff_date_str}")
        return start_date_str, end_date_str, cutoff_date_str

    # if we want to put the date manually
    print("\nPlease enter date range manually:")
    start_date = input("  Start date (YYYY-MM-DD): ")
    end_date = input("  End date (YYYY-MM-DD): ")
    
    print("\nPlease enter cutoff date for new customers:")
    cutoff_date = input(f"  Cutoff date (leave empty to use '{start_date}'): ")
    
  
    if cutoff_date == '': 
        cutoff_date = start_date
    
 
    print(f"Using date range: {start_date} to {end_date}")
    print(f"Using cutoff date: {cutoff_date}")


    return start_date, end_date, cutoff_date
    
    

def main():
    print("--- Starting automated report script ---")

    # 1. Get dates from the user
    start_date, end_date, cutoff_date = get_date_range()
    
    # Create a filename with the dates
    output_filename = f"Shookbook_weekly_report_{start_date}_to_{end_date}.xlsx"
    
    # 2. Create a connection to the database
    missing_config = [key for key, value in DB_CONFIG.items() if not value]
    if missing_config:
        print(" ERROR: Not all connection details are defined in/loaded from .env file.")
        print(f"Missing connection info: {', '.join(missing_config)}")
        print("Please ensure the .env file exists and the variable names match (DB_DRIVER, DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME).")
        return
        
        #database connection details as a string (url + passward + port + database) 
        #the createengine uses this string to create a connection to the database
    connection_string = (
        f"{DB_CONFIG['driver']}://"
        f"{DB_CONFIG['username']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
        f"/{DB_CONFIG['database']}"
    )
    try:
        engine = create_engine(connection_string)
        print("Database connection successful!")

        #if the connection isn't successful, we throw an error and can't run the queries
    except Exception as e:
        print(f"Database connection error: {e}")
        return

    # 3. Run the queries and collect the results
    print("Starting to run queries...")
    
  
    #creating a Date Range dictionary (object) reffrencing the dates we got from the user from the get_date_range function
    #these are will be the variables for the dates in the queries

    DATE_RANGE = {
        'START_DATE': start_date, #DATE_RANGE['START_DATE'] is the start date
        'END_DATE': end_date, #DATE_RANGE['END_DATE'] is the end date
        'CUSTOMER_CUTOFF_DATE': cutoff_date #DATE_RANGE['CUSTOMER_CUTOFF_DATE'] is the cutoff date
    }

    #creating an empty dictionary object to store the results of the queries

    queries_results_to_export = {}

#iterating over the ALL_QUERIES dictionary object and running the queries
#the .item() method returns a tuple of the key and value
# it automatically iterates over the dictionary and assigns the key to the 1st parameter and value to the 2nd parameter (in  my case - sheet_name and sql_query respectively)
# sheet_name will get the key from the dictionary (the query name)
#sql_query will get the value from the dictionary (the query itself)
    for sheet_name, sql_query in ALL_QUERIES.items():
        print(f"  > Running query: '{sheet_name}'...")
        try:
          #scanning the sql_query (the query itself in the dictionary) and replacing the placeholders with the values from the DATE_RANGE dictionary
          #if it dosent find any, it just leaves the queryas is)
            formatted_query = sql_query.format_map(DATE_RANGE)

            #running the query, and using "Pandas" library to read the results, turn them into a table, and store them in a panda DF (dataframe) object we call "results_table_df"

            results_table_df = pd.read_sql(formatted_query, con=engine)
            


         #we fill our results dictionary object with the query name as the key, and the results table dataframe as the value
            queries_results_to_export[sheet_name] = results_table_df
            print(f"    > Success! Found {len(results_table_df)} records.")

        except Exception as e:
            print(f"    > !!! FAILED !!! Query '{sheet_name}' execution failed: {e}")
          
            queries_results_to_export[sheet_name] = pd.DataFrame({'Error': [str(e)]})

   
   

    print(f"\nExporting all results to file: {output_filename} ...")
    try:
   
    # 4.using pandas library to export all results to one Excel file 
    # "writer" is just a pandas object that allows us to write to the Excel file
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            for sheet_name, results_table_df in queries_results_to_export.items():
                #writing the results table dataframe file into a new Excel file
                #"sheet_name" will bethe name of the sheet in the Excel file (the query name)
                #index=False means we don't want to write the index from the DF file column in the Excel file
                results_table_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print("--- Script completed successfully! ---")
        print(f"Open the file '{output_filename}' to see the results.")
    
    except Exception as e:
        print(f"!!! CRITICAL ERROR while saving Excel file: {e}")


# Running the main function
if __name__ == "__main__":
    main()