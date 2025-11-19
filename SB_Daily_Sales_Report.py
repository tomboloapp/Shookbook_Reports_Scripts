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
    p.id as Product_ID,
    p.name as Name_Hebrew,
    p.name_heb as Product_Description,
    
   
    SUM(op.quantity_needed) AS Total_Needed,
    SUM(op.quantity) AS Total_Supplied,
    p.price AS Price_Per_Unit,
    SUM(p.price * op.quantity) AS Total_Cost,
    p.product_list AS Product_List

from products p
    Join order_product op on op.product_id = p.id
    Join orders o on o.id = op.order_id
Where 
    
    (o.delivery_date = '{DELIVERY_DATE}' AND o.delivery_window = 1) 
    OR 
    (o.delivery_date = '{DAY_TOMORROW}' AND o.delivery_window = 0)
    
    And o.status IN (0,3,7)
GROUP BY
    p.id, p.name, p.name_heb, p.price, p.product_list;
"""
}


# -----------------------------------------------------------------
# STAGE 3: Logic
# -----------------------------------------------------------------

def get_report_date():
    """Requests a single report date from the user."""
    # --- NO CHANGE IN THIS FUNCTION ---
    print("Please enter the report date (format: YYYY-MM-DD).")
    
    # Option: automatic calculation of "yesterday"
    use_default = input("Use yesterday's date? (y/n, default 'yes'): ")
    if use_default.lower() in ('', 'y', 'כן'):
        yesterday_dt = datetime.now() - timedelta(days=1)
        report_date_str = yesterday_dt.strftime('%Y-%m-%d')
        print(f"Using date: {report_date_str}")
        return report_date_str # Returns a single value

    # Manual input
    print("\nPlease enter date manually:")
    report_date_str = input("    Report date (YYYY-MM-DD): ")
    if report_date_str == '': # Basic validation
        print("No date entered. Exiting.")
        sys.exit() # Exits the script if nothing was entered
        
    print(f"Using date: {report_date_str}")
    return report_date_str # Returns a single value

def main():
    print("--- Starting automated report script ---")

    # 1. Get a single date from the user
    delivery_date_str = get_report_date() 
    
    # Create a filename suitable for a single day
    output_filename = f"Shookbook_daily_Sales_report_{delivery_date_str}.xlsx"
    
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
    
    

    
    # Convert the date string back to a datetime object for calculation
    try:
        delivery_date_obj = datetime.strptime(delivery_date_str, '%Y-%m-%d')
    except ValueError:
        # This handles if the user typed an invalid date format
        print(f"ERROR: The date '{delivery_date_str}' is not in the correct YYYY-MM-DD format. Exiting.")
        sys.exit()
        
   # Calculate the day tomorrow 
    day_tomorrow_obj = delivery_date_obj + timedelta(days=1)
    
    # Convert the day tomorrow back to a string
    day_tomorrow_str = day_tomorrow_obj.strftime('%Y-%m-%d')
    
# Create a dictionary with BOTH date strings
    DATE_VARS = {
        'DELIVERY_DATE': delivery_date_str,
        'DAY_TOMORROW': day_tomorrow_str
    }
    print(f"Querying for {DATE_VARS['DELIVERY_DATE']} (window 1) and {DATE_VARS['DAY_TOMORROW']} (window 0).")


    #creating an empty delivery Results Dictionary object to store the results of the queries
    delivery_results_to_export = {}

    #iterating over the ALL_QUERIES dictionary object and running the queries
    for sheet_name, sql_query in ALL_QUERIES.items():
        print(f"  > Running query: '{sheet_name}'...")
        try:
            # --- CHANGED: Using DATE_VARS instead of the old dictionary ---
            formatted_query = sql_query.format_map(DATE_VARS)

            #running the query, and using "Pandas" library to read the results
            results_table_df = pd.read_sql(formatted_query, con=engine)
            
            #we fill our results dictionary object
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