import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["listingsAndReviews"]  # Replace with your database name
collection = db["listingsAndReviews"]  # Replace with your collection name

# Fetch all documents
documents = collection.find()

# Print all documents
for document in documents:
    print("Document found:")
    print(document)
