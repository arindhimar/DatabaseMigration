import pymongo
import json
import re

def preprocess_json(json_string):
    # Remove invalid control characters
    return re.sub(r'[\x00-\x1F\x7F]', '', json_string)

# Connect to MongoDB
try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["student_test1"]
    collection = db["student_test1"]
except pymongo.errors.ConnectionFailure as e:
    print("Error connecting to MongoDB:", e)
    exit(1)

# Load and preprocess data from JSON file with proper encoding
file_data = []
try:
    with open("student-data.json", encoding='utf-8') as file:
        json_string = file.read()
        json_string = preprocess_json(json_string)
        file_data = json.loads(json_string)
except (UnicodeDecodeError, json.JSONDecodeError) as e:
    print(f"Error reading or decoding JSON file: {e}")
    exit(1)

# Validate and insert data
try:
    if isinstance(file_data, list):
        if file_data:  # Check if the list is not empty
            collection.insert_many(file_data)
            print("Data inserted successfully!")
        else:
            print("The JSON array is empty.")
    elif isinstance(file_data, dict): 
        collection.insert_one(file_data)
        print("Data inserted successfully!")
    else:
        print("Unsupported JSON data format.")
except pymongo.errors.PyMongoError as e:
    print("Error inserting data:", e)
