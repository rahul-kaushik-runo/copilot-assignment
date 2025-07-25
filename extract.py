from pymongo import MongoClient
from collections import defaultdict
import pandas as pd

def get_database_collections_and_schemas(uri, db_name, sample_size=10):
    """
    Retrieve all collection names and their schemas from a MongoDB database.
    
    Args:
        uri (str): MongoDB connection URI
        db_name (str): Database name
        sample_size (int): Number of documents to sample for schema detection
        
    Returns:
        dict: A dictionary with collection names as keys and their schemas as values
    """
    try:
        # Connect to MongoDB
        client = MongoClient(uri)
        db = client[db_name]
        
        # Get all collection names
        collection_names = db.list_collection_names()
        
        schemas = {}
        
        for collection_name in collection_names:
            collection = db[collection_name]
            
            # Sample documents to determine schema
            sample_docs = collection.aggregate([{'$sample': {'size': sample_size}}])
            
            # Analyze schema from sampled documents
            schema = defaultdict(set)
            for doc in sample_docs:
                for field, value in doc.items():
                    schema[field].add(type(value).__name__)
            
            # Convert sets to strings for better display
            schema = {field: ", ".join(types) for field, types in schema.items()}
            schemas[collection_name] = schema
        
        return schemas
    
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        client.close()

def display_schemas(schemas):
    """
    Display the collection schemas in a readable format.
    
    Args:
        schemas (dict): Dictionary containing collection schemas
    """
    if not schemas:
        print("No schemas to display.")
        return
    
    for collection, schema in schemas.items():
        print(f"\nCollection: {collection}")
        print("Fields and their data types:")
        for field, types in schema.items():
            print(f"  {field}: {types}")

if __name__ == "__main__":
    # MongoDB connection details
    MONGODB_URI = "mongodb://localhost:27017/"  # Update with your MongoDB URI
    DATABASE_NAME = "runo"                     # Update with your database name
    
    # Get schemas
    schemas = get_database_collections_and_schemas(MONGODB_URI, DATABASE_NAME)
    
    if schemas:
        print(f"\nFound {len(schemas)} collections in database '{DATABASE_NAME}':")
        display_schemas(schemas)
    else:
        print("No collections found or an error occurred.")