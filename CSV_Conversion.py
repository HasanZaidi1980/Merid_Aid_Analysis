import pyodbc
import pandas as pd
import os

# --- Configuration (UPDATE THESE) ---
# Use the full path to your IPEDS ACCDB file
ACCD_FILE = r"C:\Users\amatu\Downloads\IPEDS_2022-23_Final\IPEDS202223.accdb" 
# Use a directory where you want all the CSV files to be saved
OUTPUT_DIR = r"C:\Users\amatu\Downloads\Phase_3" 
# --- Function Definitions ---

def get_table_names(conn):
    """Retrieves a list of non-system table names from the Access database."""
    print("Fetching list of table names...")
    # Get all tables from the database metadata
    table_list = []
    
    # conn.cursor().tables() returns metadata about tables/views/system tables
    for table_info in conn.cursor().tables(tableType='TABLE'):
        # Filter out system tables (which usually start with 'MSys' or are 'Temporary')
        table_name = table_info.table_name
        if not table_name.startswith('MSys') and table_name != 'Temporary':
            table_list.append(table_name)
            
    print(f"Found {len(table_list)} tables to export.")
    return table_list

def accdb_to_csv_multiple_tables(accdb_path, output_directory):
    """
    Connects to an Access .accdb file, reads all user tables, and saves each 
    one to a separate CSV file in the specified directory.
    """
    # 1. Construct the connection string
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={accdb_path};'
    )
    
    if not os.path.exists(accdb_path):
        print(f"Error: ACCDB file not found at {accdb_path}")
        return

    # Ensure output directory exists
    os.makedirs(output_directory, exist_ok=True)

    try:
        # 2. Establish database connection
        print(f"Connecting to the database: {accdb_path}")
        conn = pyodbc.connect(conn_str)

        # 3. Get all table names
        tables_to_export = get_table_names(conn)
        
        total_tables = len(tables_to_export)
        exported_count = 0

        # 4. Iterate and export each table
        for table_name in tables_to_export:
            exported_count += 1
            print("-" * 50)
            print(f"({exported_count}/{total_tables}) Exporting table: **{table_name}**")
            
            # Define the output CSV path for the current table
            output_csv_path = os.path.join(output_directory, f"{table_name}.csv")
            
            try:
                # Read data directly into a Pandas DataFrame
                sql_query = f'SELECT * FROM [{table_name}]'
                print(f"   -> Executing SQL: {sql_query}")
                df = pd.read_sql(sql_query, conn)
                
                # Save the DataFrame as a CSV file
                # index=False is crucial to avoid adding an unnecessary index column
                df.to_csv(output_csv_path, index=False, encoding='utf-8') 
                
                print(f"   -> Success: Saved {len(df)} rows to {os.path.basename(output_csv_path)}")
                
                # Explicitly delete DataFrame to free memory immediately after use
                del df 
                
            except Exception as table_e:
                print(f"   -> **Error** exporting table {table_name}: {table_e}")
                
        # 5. Close the database connection
        conn.close()
        print("\n" + "=" * 50)
        print("✅ Conversion process complete.")
        print(f"All tables exported to: {OUTPUT_DIR}")
        print("=" * 50)

    except pyodbc.Error as ex:
        # Handle connection errors (e.g., driver missing)
        print("\n" + "#" * 50)
        print(f"🚫 A critical ODBC error occurred:")
        print(f"   Error: {ex.args[1]}")
        print("   **Action Required:** Please ensure the correct **Microsoft Access ODBC Driver** (e.g., 'Microsoft Access Driver (*.mdb, *.accdb)') is installed and accessible to your Python environment (often requires 64-bit Python if using a 64-bit driver).")
        print("#" * 50)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Execution ---

if __name__ == "__main__":
    accdb_to_csv_multiple_tables(ACCD_FILE, OUTPUT_DIR)