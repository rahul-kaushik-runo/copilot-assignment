from pymongo import MongoClient

# --- MongoDB connection settings ---
MONGO_URI = "mongodb://localhost:27017"  # Replace with your URI
DATABASE_NAME = "callCrm"     # Replace with your database name

def list_indexes():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    # Get all collection names
    collections = db.list_collection_names()

    for collection_name in collections:
        print(f"{collection_name}:\n")
        
        # Get indexes for the collection
        indexes = db[collection_name].list_indexes()
        
        for index in indexes:
            print(index)
        
        print("\n")  # Add a blank line between collections

if __name__ == "__main__":
    list_indexes()
