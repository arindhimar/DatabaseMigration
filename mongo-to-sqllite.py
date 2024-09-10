import pymongo
import sqlite3
import re
import json
from typing import Dict, Any
from bson.objectid import ObjectId

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["employee_test1"]
collection = db["employee_test1"]

# Connect to SQLite
sqlite_conn = sqlite3.connect("employee-test2.db")
cursor = sqlite_conn.cursor()

# Function to sanitize names for SQL
def sanitize(name: str) -> str:
    return re.sub(r'\W|^(?=\d)', '_', name)

# Check if table exists
def check_if_table_exists(table_name: str) -> bool:
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{sanitize(table_name)}';")
    return cursor.fetchone() is not None

# Function to create a new table if it doesn't exist
def create_table(table_name: str, columns: Dict[str, Any]):
    sanitized_table_name = sanitize(table_name)
    column_defs = ', '.join([f"{sanitize(col)} TEXT" for col in columns.keys() if col != '_id'])
    create_query = f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} (_id TEXT PRIMARY KEY, {column_defs});"
    
    try:
        cursor.execute(create_query)
        sqlite_conn.commit()
        print(f"Table {sanitized_table_name} created with columns: {columns.keys()}")
    except sqlite3.Error as e:
        print(f"Error creating table {sanitized_table_name}: {e}")

# Insert data into the table
def insertData(table_name: str, data: Dict[str, Any], _id: str):
    sanitized_table_name = sanitize(table_name)
    
    # Prepare columns and values
    columns = ', '.join([sanitize(key) for key in data.keys()])
    placeholders = ', '.join(['?' for _ in data.values()])
    values = list(data.values())
    
    # Ensure '_id' is part of the insertion
    if '_id' not in data:
        columns = '_id, ' + columns
        placeholders = '?, ' + placeholders
        values.insert(0, _id)
    
    # Identify and handle unsupported types
    supported_values = []
    for value in values:
        if isinstance(value, ObjectId):
            supported_values.append(str(value))
        elif isinstance(value, (dict, list)):
            supported_values.append(json.dumps(value))
        elif value is None:
            supported_values.append(None)
        elif isinstance(value, (str, int, float, bool)):
            supported_values.append(value)
        else:
            print(f"Unsupported data type: {type(value)}")
            return
    
    values = supported_values
    
    # Construct the SQL query for insertion
    insert_query = f"""
    INSERT OR REPLACE INTO {sanitized_table_name} ({columns})
    VALUES ({placeholders});
    """
    
    try:
        print(f"Executing query: {insert_query} with values {values}")
        cursor.execute(insert_query, values)
        sqlite_conn.commit()
        print(f"Data inserted into {sanitized_table_name}: {data}")
    except sqlite3.Error as e:
        print(f"Error inserting data into {sanitized_table_name}: {e}")

# Process the document
def process_document(document: Dict[str, Any]):
    main_table_name = 'main_table'
    
    # Create the main table if it doesn't exist
    if not check_if_table_exists(main_table_name):
        create_table(main_table_name, document)
    
    _id = str(document.get('_id'))  # Convert _id to string
    
    # Extract non-dictionary values for main table
    main_data = {key: value for key, value in document.items() if not isinstance(value, dict)}
    insertData(main_table_name, main_data, _id)
    
    # Process nested documents
    for key, value in document.items():
        if isinstance(value, dict):
            # Create a new table for the nested document
            nested_table_name = sanitize(key)
            if not check_if_table_exists(nested_table_name):
                create_table(nested_table_name, value)
            insertData(nested_table_name, value, _id)  # Insert the nested dictionary into the new table

# Fetch and process all MongoDB documents
documents = collection.find()

for document in documents:
    process_document(document)

# Close the SQLite connection
sqlite_conn.close()
