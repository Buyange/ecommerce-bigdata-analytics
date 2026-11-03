from pymongo import MongoClient

# Connect to the Mongo container (port 27017 is mapped to your laptop)
client = MongoClient("mongodb://localhost:27018", serverSelectionTimeoutMS=3000)

# server_info() forces a real round trip, so it fails if Mongo is unreachable
print("Connected. MongoDB version:", client.server_info()["version"])

# Mongo creates a database and collection the first time you write to them
db = client["ecommerce"]
db.test.insert_one({"hello": "world"})
print("Documents in test collection:", db.test.count_documents({}))
db.test.drop()   # clean up