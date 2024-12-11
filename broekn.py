import arcpy
import os
import pandas as pd
from datetime import datetime
import pyodbc
database_fields = {
    "database1": [
        "url": "https://example.com/database1.gdb",
        "adjustments": [
            ["ProcessDate", "DATE"],
            ["Source", "TEXT"], 
            ["Category", "TEXT"]
        ],
        "updates": [
            ["UpdateDate", "DATE"],
            ["Status", "TEXT"]
        ]
    ],
    "database2": [
        ["UpdateDate", "DATE"],
        ["Status", "TEXT"]
    ],
    "database3": [
        ["UpdateDate", "DATE"],
        ["Status", "TEXT"]
    ],
    "database4": [
        ["UpdateDate", "DATE"],
        ["Status", "TEXT"]
    ],
    "database5": [
        ["UpdateDate", "DATE"],
        ["Status", "TEXT"]
    ]
}
for adjustment in database_fields["database1"]["adjustments"]:
    print(adjustment)
    
def handle_individual_database(name, file):
    # Set the workspace
    arcpy.env.workspace = gdb_path
    
    # Get all feature classes in the geodatabase
    feature_classes = arcpy.ListFeatureClasses()
    

    for fc in feature_classes:
        # Determine which columns to add based on the geodatabase name
       
        fields_to_add = database_fields[name_of_this_datbase]

        for db_name, fields in database_fields.items():
            if db_name in item.lower():
                fields_to_add = fields
                break
        else:
            fields_to_add = []
        
        # Add the new fields
        for field_name, field_type in fields_to_add:
            if not arcpy.ListFields(fc, field_name):
                arcpy.AddField_management(fc, field_name, field_type)
        
        # Convert to SQL Server format and upload
        table_name = f"{os.path.splitext(item)[0]}_{fc}"
        arcpy.conversion.FeatureClassToFeatureClass(
            fc,
            f"Database Connections/SQL_Server_Connection.sde",
            table_name
        )

        # Save changes to the SQL Server database
        conn.commit()

def process_geodatabases():
    # Set up logging
    log_file = f"gdb_processing_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    # SQL Server connection parameters
    server = 'your_server_name'
    database = 'your_database_name'
    trusted_connection = 'yes'
    
    # Connect to SQL Server
    conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection={trusted_connection}'
    conn = pyodbc.connect(conn_str)
    
    # List of URLs to download .gdb files from
    gdb_urls = [
        "https://example.com/database1.gdb",
        "https://example.com/database2.gdb"
        # Add more URLs as needed
    ]
    
    # Create temporary directory for downloaded files
    cwd = os.path.join(os.getcwd(), "temp_gdbs")
    if not os.path.exists(cwd):
        os.makedirs(cwd)
        
    # Download GDB files
    source_dir = cwd
    for url in gdb_urls:
        filename = os.path.basename(url)
        filepath = os.path.join(cwd, filename)
        urllib.request.urlretrieve(url, filepath)
    
    # Process each geodatabase in the directory
    for item in os.listdir(source_dir):
        if item.endswith('.gdb'):
            gdb_path = os.path.join(source_dir, item)
            
            handle_individual_database(item, gdb_path)
            
                
                with open(log_file, 'a') as log:
                    log.write(f"{datetime.now()}: Processed {fc} from {item}\n")
    
    print("Processing completed successfully!")
        
if __name__ == "__main__":
    process_geodatabases()
