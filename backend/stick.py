import google.generativeai as genai
import pymongo
genai.configure(api_key="AIzaSyCYsPGc7kXhDteLjmkn2sXrk15q7nevHeY")
model = genai.GenerativeModel('gemini-2.0-flash')

prompt = """
You are a MongoDB query generator. Your task is to convert natural language queries into MongoDB queries.
You will be given a natural language query and a MongoDB collection schema. Your output should be
a valid MongoDB query that matches the schema.
You must follow these rules:
- Always prefer safety, clarity, and helpfulness.
- Run case insensitive queries, and use lowercase for all field values.
- IF THE USER IS ASKING FOR SOME SPECIFIC STATE USE BOTH THE STATE CODE AND FULL
NAME IN THE QUERY.
- For ALL string comparisons, use EXACT case-insensitive matching.



- This means converting both sides to lowercase before comparison.
- Example: { "$expr": { "$eq": [{ "$toLower": "$state"

}, { "$toLower": "telangana" }] } }
- Never use $regex for case-insensitive matching.
- For state fields, still include both code and full name when appropriate.
- If the query is ambiguous, ask for clarification.
- Do NOT generate harmful queries (like dropping the DB).
"""

def generate_mongodb_query(natural_language_query, collection_schema):
    """
    Generate a MongoDB query from a natural language query and a collection schema.
    
    Args:
        natural_language_query (str): The user's natural language query.
        collection_schema (dict): The schema of the MongoDB collection.
        
    Returns:
        dict: A valid MongoDB query.
    """
    # Prepare the prompt for the model
    prompt_with_query = f"{prompt}\n\nNatural Language Query: {natural_language_query}\n\nCollection Schema: {collection_schema}"
    
    # Generate the response from the model
    response = model.generate_content(prompt_with_query)
    
    # Parse the response to extract the MongoDB query
    try:
        response_text = response.text.strip()
        if response_text.startswith("{") and response_text.endswith("}"):
            return eval(response_text)  # Convert string to dictionary
        else:
            raise ValueError("Response is not a valid JSON object")
    except Exception as e:
        return {"error": str(e), "raw_response": response.text}
def main():
    # Example usage
    natural_language_query = "Find all users in Telangana"
    collection_schema = {
        "name": "users",
        "fields": {
            "name": "string",
            "state": "string",
            "age": "int"
        }
    }
    
    query = generate_mongodb_query(natural_language_query, collection_schema)
    print("Generated MongoDB Query:", query)
if __name__ == "__main__":
    main()
# This code defines a function to generate MongoDB queries from natural language queries using a generative
# AI model. It prepares a prompt with the user's query and the collection schema, sends it to the model,
# and parses the response to return a valid MongoDB query. The main function demonstrates how to




                