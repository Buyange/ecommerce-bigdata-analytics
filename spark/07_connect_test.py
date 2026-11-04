from pymongo import MongoClient
import happybase

# MongoDB: service name 'mongo', the container's own port 27017 (not 27018)
db = MongoClient("mongodb://mongo:27017", serverSelectionTimeoutMS=5000)["ecommerce"]
print("MongoDB OK. transactions:", db.transactions.count_documents({}))

# HBase: service name 'hbase', Thrift port 9090
connection = happybase.Connection("hbase", port=9090)
print("HBase OK. tables:", connection.tables())
key, data = next(connection.table("user_sessions").scan(limit=1))
print("first HBase row key:", key.decode())