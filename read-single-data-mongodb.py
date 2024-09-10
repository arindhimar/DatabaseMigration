import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["student_test1"]  # Replace with your database name
collection = db["student_test1"]  # Replace with your collection name

# Define a specific query to fetch a single document
query = {"id":1}  # Replace with your actual query

# Fetch a single document
document = collection.find_one(query)

# Print the document
if document:
    print("Document found:")
    print(document)
else:
    print("No document matches the query.")
