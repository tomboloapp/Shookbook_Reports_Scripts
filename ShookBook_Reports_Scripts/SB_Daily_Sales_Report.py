import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv 


# Load variables from .env file into environment
load_dotenv()

# -----------------------------------------------------------------
# STAGE 1: Configuration (Loaded from .env)
# -----------------------------------------------------------------
DB_CONFIG = {
    'driver': os.getenv('DB_DRIVER'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME') 
}

# -----------------------------------------------------------------
# STAGE 2: Query Warehouse
# -----------------------------------------------------------------
ALL_QUERIES = {
    "daily_sales_report": """
Select
p.id,
p.name,
p.name_heb,
p.price,
p.product_list,
op.quantity,
op.quantity_needed

from products p
Join order_product op on op.product_id = p.id
Join orders o on o.id = op.order_id
Where o.delivery_date = '{DELIVERY_DATE}'
and (o.delivery_window in (0,1))
And o.status IN (0,3,7)
"""
}

# -----------------------------------------------------------------
# STAGE 3: Logic
# -----------------------------------------------------------------

def get_report_date():
    """Requests a single report date from the user."""
    print("Please enter the report date (format: YYYY-MM-DD).")
    
    # Option: automatic calculation of "yesterday"
    use_default = input("Use yesterday's date? (y/n, default 'yes'): ")
    if use_default.lower() in ('', 'y', 'כן'):
        yesterday_dt = datetime.now() - timedelta(days=1)
        report_date_str = yesterday_dt.strftime('%Y-%m-%d')
        print(f"Using date: {report_date_str}")
        return report_date_str # Returns a single value

    # Manual input
    print("\nPlease enter date range manually:")
    report_date_str = input("  Report date (YYYY-MM-DD): ")
    if report_date_str == '': # Basic validation
        print("No date entered. Exiting.")
        sys.exit() # Exits the script if nothing was entered
        
    print(f"Using date: {report_date_str}")
    return report_date_str # Returns a single value

def main():
    print("--- Starting automated report script ---")

    # 1. Get a single date from the user
    delivery_date = get_report_date() 
    
    # Create a filename suitable for a single day
    output_filename = f"Shookbook_daily_Sales_report_{delivery_date}.xlsx"
    
    # 2. Create a connection to the database
    #    (Validation check updated to look for DB_NAME)
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
    except Exception as e:
        print(f"Database connection error: {e}")
        return

    # 3. Run the queries and collect the results
    print("Starting to run queries...")
    
    
    #creating a Delivery Date dictionary (object) reffrencing the dates we got from the user from the get_date_range function
    #these are will be the variables for the dates in the queries

    DELIVERY_DATE = {
        'DELIVERY_DATE': delivery_date #DELIVERY_DATE['DELIVERY_DATE'] is the delivery date
    }

    #creating an empty delivery Results Dictionary object to store the results of the queries

    delivery_results_to_export = {}

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
            formatted_query = sql_query.format_map(DELIVERY_DATE)

            #running the query, and using "Pandas" library to read the results, turn them into a table, and store them in a panda DF (dataframe) object we call "results_table_df"

            results_table_df = pd.read_sql(formatted_query, con=engine)
            


         #we fill our results dictionary object with the query name as the key, and the results table dataframe as the value
            delivery_results_to_export[sheet_name] = results_table_df
            print(f"    > Success! Found {len(results_table_df)} records.")

        except Exception as e:
            print(f"    > !!! FAILED !!! Query '{sheet_name}' execution failed: {e}")
          
            delivery_results_to_export[sheet_name] = pd.DataFrame({'Error': [str(e)]})

   
   

    print(f"\nExporting all results to file: {output_filename} ...")
    try:
   
    # 4.using pandas library to export all results to one Excel file 
    # "writer" is just a pandas object that allows us to write to the Excel file
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            for sheet_name, results_table_df in delivery_results_to_export.items():
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