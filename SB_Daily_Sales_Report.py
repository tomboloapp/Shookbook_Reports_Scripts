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
    p.id as `מזהה מוצר`,
    p.name as `שם מוצר`,
    p.name_heb as `תיאור מוצר`,
    SUM(op.quantity_needed) AS  `הכמות הנדרשת`,
    SUM(op.quantity) AS Total_Supplied,
    p.price AS `מחיר ליחידה`,
    SUM(p.price * op.quantity) AS `עלות כוללת`,
    p.product_list AS `רשימת מוצרים`

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

def get_date_range_list():
    """
    Asks user for a date or range (DDMMYY, DD.MM.YY, DD/MM/YY).
    Returns a list of date strings in 'YYYY-MM-DD' format.
    """
    print("\nEnter date range (e.g., 111125-131125) or single date (111125):")
    user_input = input("Input: ").strip()
    
    if not user_input:
        print("No input provided. Exiting.")
        sys.exit()

    # Split the input by '-' to check if it's a range ; each date will be a part
    # User-Defined Name: 'parts' (holds the start and end parts)
    parts = user_input.split('-')
    
    # Allowed formats
    # Library/Framework Code: formats recognized by datetime
    valid_formats = ['%d%m%y', '%d.%m.%y', '%d/%m/%y']
    
    parsed_dates = []
    
    # Helper function to try parsing a single string
    def try_parse(d_str):
        for fmt in valid_formats:
            try:
                # Core Language Syntax: try/except flow control
                return datetime.strptime(d_str.strip(), fmt)
            except ValueError:
                continue
        print(f"Error: Could not recognize format for '{d_str}'. Use DDMMYY.")
        sys.exit()

    start_date = try_parse(parts[0])
    
    # If there is a second part, it's a range. Otherwise start=end.
    end_date = try_parse(parts[1]) if len(parts) > 1 else start_date
    
    # Generate the list of all dates in between
    # User-Defined Name: 'date_list'
    date_list = []
    current = start_date
    while current <= end_date:
        date_list.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
        
    return date_list

def main():
    print("--- Starting automated report script ---")

    # 1. Get list of dates
    # User-Defined Name: dates_to_process (List of strings)
    dates_to_process = get_date_range_list()
    
    # Create a dynamic filename based on start and end dates
    # User-Defined Name: start_str, end_str
    start_str = dates_to_process[0]
    end_str = dates_to_process[-1]
    output_filename = f"Sales_Report_Range_{start_str}_to_{end_str}.xlsx"
    
    # 2. Database Connection (Same as before)
    missing_config = [key for key, value in DB_CONFIG.items() if not value]
    if missing_config:
        print(f"Missing connection info: {', '.join(missing_config)}")
        return

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

    print(f"\nPreparing to export {len(dates_to_process)} days to: {output_filename}")

    # 3. Open the Excel Writer ONCE (Context Manager)
    # We keep the file open while we loop through the dates
    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            
            # --- MAIN LOOP: Iterate over each date ---
            for current_date_str in dates_to_process:
                print(f"\n--- Processing Date: {current_date_str} ---")
                
                # A. Logic: Calculate Tomorrow
                try:
                    curr_obj = datetime.strptime(current_date_str, '%Y-%m-%d')
                    next_obj = curr_obj + timedelta(days=1)
                    next_day_str = next_obj.strftime('%Y-%m-%d')
                except ValueError as e:
                    print(f"Date error: {e}")
                    continue

                # B. Update Variables for Query
                DATE_VARS = {
                    'DELIVERY_DATE': current_date_str,
                    'DAY_TOMORROW': next_day_str
                }

                # C. Run Query
                # We assume only one query type exists in ALL_QUERIES for now
                for query_name, sql_template in ALL_QUERIES.items():
                    formatted_query = sql_template.format_map(DATE_VARS)
                    
                    try:
                        df = pd.read_sql(formatted_query, con=engine)
                        
                        # Check if empty
                        if df.empty:
                            print("   > No data found for this date.")
                            # Create a dummy DF so we still have a tab
                            df = pd.DataFrame({'Status': ['No Data']})
                        else:
                            print(f"   > Found {len(df)} records.")

                        # D. Write to Excel Sheet
                        # We name the sheet after the DATE (e.g., "2025-11-11")
                        sheet_title = current_date_str 
                        df.to_excel(writer, sheet_name=sheet_title, index=False)
                        
                        # --- NEW: RIGHT-TO-LEFT (RTL) CONFIGURATION ---
                        # Library/Framework Code: Accessing the worksheet object from openpyxl
                        worksheet = writer.sheets[sheet_title]
                        worksheet.sheet_view.rightToLeft = True

                    except Exception as e:
                        print(f"   > Query failed: {e}")

        print("\n--- Script completed successfully! ---")
        print(f"File saved: {output_filename}")

    except Exception as e:
        print(f"CRITICAL FILE ERROR: {e}")


# Running the main function
if __name__ == "__main__":
    main()