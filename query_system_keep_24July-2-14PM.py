import json
import google.generativeai as genai
from pymongo import MongoClient
import time
from difflib import get_close_matches


with open('backend/schema.txt', 'r') as file:
    file_contents = file.read()

SCHEMAS_STR = file_contents



class NLToMongoDBQuerySystem:
    def __init__(self):
        # Configuration - replace with your actual values
        API_KEY = "AIzaSyCYsPGc7kXhDteLjmkn2sXrk15q7nevHeY"
        MONGODB_URI = "mongodb://localhost:27017"
        DB_NAME = "runo"

        # Initialize Gemini
        genai.configure(api_key=API_KEY)
        self.model = genai.GenerativeModel('gemini-2.5-flash')

        # Initialize MongoDB connection
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DB_NAME]
        self.query_history = []
        
        # Schema processing
        self.schema_summaries = self._create_schema_summaries()
        self.full_schemas = self._parse_full_schemas()

        # Value synonyms (manually maintained or learned)
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }

    def _create_schema_summaries(self):
        """Create concise one-line summaries of each collection"""
        summaries = {}
        collections = SCHEMAS_STR.strip().split("collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            first_line = coll_text[name_end+1:].split('\n')[0].strip()
            summaries[collection_name] = first_line
        return summaries
    
    def _parse_full_schemas(self):
        """Parse full schemas into a dictionary for quick access"""
        schemas = {}
        collections = SCHEMAS_STR.strip().split("collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            schemas[collection_name] = f"collection: {collection_name}\n{coll_text[name_end+1:]}"
        return schemas
    
    def normalize_query(self, query):
        """Normalize natural language query using value synonyms"""
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

    def _select_best_collection(self, query_text):
        """Phase 1: Have LLM select the best collection based on summaries"""
        normalized_query = self.normalize_query(query_text)
        
        prompt = f"""
You are a MongoDB expert assistant. Given the following collection summaries, 
select the SINGLE most appropriate collection for this query:

USER QUERY: "{normalized_query}"

AVAILABLE COLLECTIONS:
{self._format_collection_summaries()}

INSTRUCTIONS:
1. Analyze the user's query intent
2. Compare with each collection's purpose
3. Select ONLY ONE collection name that best matches
4. Respond ONLY with the collection name in this format: "collection: <name>"

EXAMPLE RESPONSES:
- "collection: report-agent-disposition"
- "collection: report-history"
"""
        response = self.model.generate_content(prompt)
        try:
            # Extract collection name from response
            if "collection:" in response.text.lower():
                return response.text.split("collection:")[1].strip().split()[0].strip('"\'')
            return None
        except Exception as e:
            print(f"Error parsing collection selection: {str(e)}")
            return None
            
    def _format_collection_summaries(self):
        """Format collection summaries for selection prompt"""
        return "\n".join(
            f"- {name}: {summary}" 
            for name, summary in self.schema_summaries.items()
        )
        
    def _generate_query_for_collection(self, query_text, collection_name):
        """Phase 2: Generate query for specific collection"""
        normalized_query = self.normalize_query(query_text)
        schema = self.full_schemas.get(collection_name)
        if not schema:
            return {"error": f"Collection {collection_name} not found"}
            
        # Prepare value synonyms string
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])
        print(schema)
            
        prompt = f"""
You are a MongoDB query expert. Given this collection schema, generate a query for:

USER QUERY: "{normalized_query}"

COLLECTION SCHEMA:
{schema}

VALUE SYNONYMS TO CONSIDER (use canonical values in query):
{value_synonyms_str}

Respond ONLY with a JSON object: {{ "collection": "{collection_name}", "query": <mongo_query> }}

CRITICAL RULES:
1. Use proper MongoDB query syntax
2. For text matching, use case-insensitive regex: {{ "$regex": "pattern", "$options": "i" }}
3. For counting, use aggregation pipeline with $group
4. NEVER use invalid top-level operators like $count, $sum
5. Match query terms to schema fields exactly
6. For state queries, consider both full names and abbreviations using $or
7. Generate effecient queries, don generate slow queries

VALID QUERY FORMATS:
- Simple find: {{ "field": "value" }}
- Regex find: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- Aggregation: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]

CRITICAL RULES - NEVER VIOLATE:
1. For simple queries, use find() format: {{ "field": "value" }}
2. For counting, use aggregation pipeline with $group: [{{ "$group": {{ "_id": null, "count": {{ "$sum": 1 }} }} }}]
3. For complex operations, use aggregation pipeline: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]
4. NEVER use these invalid operators: $count, $sum as top-level, $avg as top-level
5. For case-insensitive text matching, use: {{ "field": {{ "$regex": "value", "$options": "i" }} }}

VALID QUERY FORMATS ONLY:
- Simple find: {{ "name": "john", "age": 25 }}
- Regex find: {{ "name": {{ "$regex": "john", "$options": "i" }} }}
- Range find: {{ "age": {{ "$gte": 18, "$lte": 65 }} }}
- Array find: {{ "tags": {{ "$in": ["tag1", "tag2"] }} }}
- Aggregation: [{{ "$match": {{ "status": "active" }} }}, {{ "$group": {{ "_id": "$category", "count": {{ "$sum": 1 }} }} }}]

EXAMPLES OF WHAT TO GENERATE:
- "find all users" → {{ "query": {{}} }}
- "count users" → {{ "query": [{{ "$group": {{ "_id": null, "count": {{ "$sum": 1 }} }} }}] }}
- "find users named john" → {{ "query": {{ "name": {{ "$regex": "john", "$options": "i" }} }} }}
- "users from telangana" → {{ "query": {{ "$or": [{{ "state": {{ "$regex": "telangana", "$options": "i" }} }}, {{ "state": {{ "$regex": "TG", "$options": "i" }} }}] }} }}


EXAMPLE OUTPUTS:
- {{ "collection": "report-agent-disposition", "query": {{ "callType": "inbound" }} }}
- {{ "collection": "report-history", "query": [{{ "$match": {{ "type": "agent-performance" }} }}] }}
"""
        response = self.model.generate_content(prompt)
        try:
            # Parse response
            response_text = response.text
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            return json.loads(response_text.strip().strip('"\''))
        except Exception as e:
            return {"error": f"Failed to parse query: {str(e)}", "raw_response": response.text}

    def natural_language_to_query(self, natural_language_query):
        """Main method to convert NL to MongoDB query (combines phase 1 and 2)"""
        # Phase 1: Collection selection
        collection_name = self._select_best_collection(natural_language_query)
        if not collection_name:
            return {"error": "Could not determine appropriate collection"}
        
        # Phase 2: Query generation
        query_result = self._generate_query_for_collection(natural_language_query, collection_name)
        if "error" in query_result:
            return query_result
            
        # Store successful query in history
        self.query_history.append((natural_language_query, query_result))
        if len(self.query_history) > 10:
            self.query_history.pop(0)
            
        return query_result
        
    def _convert_to_case_insensitive(self, query):
        """Convert query to case-insensitive version"""
        if not isinstance(query, dict):
            return query
            
        new_query = {}
        for key, value in query.items():
            if key.startswith("$"):
                # Handle operators like $and, $or
                new_query[key] = [self._convert_to_case_insensitive(v) for v in value] if isinstance(value, list) else value
            else:
                if isinstance(value, dict):
                    # Handle nested queries
                    new_query[key] = self._convert_to_case_insensitive(value)
                elif isinstance(value, str):
                    # Convert to case-insensitive regex match
                    new_query[key] = {"$regex": f"^{value}$", "$options": "i"}
                else:
                    new_query[key] = value
        return new_query

    def _get_operation_type(self, query):
        """Determine the MongoDB operation type from the query"""
        if isinstance(query, list):
            return "aggregate"
        for op_type in ["find", "insertOne", "insertMany", "updateOne", "updateMany", 
                        "deleteOne", "deleteMany", "countDocuments", "distinct"]:
            if op_type in query:
                return op_type
        return "find"
    
    def execute_query(self, collection_name, query):
        """Execute the generated MongoDB query"""
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            results = None
            
            if operation_type == "aggregate":
                pipeline = query if isinstance(query, list) else query.get("aggregate", [])
                # Process each $match stage for case-insensitive matching
                processed_pipeline = []
                for stage in pipeline:
                    if "$match" in stage:
                        stage["$match"] = self._convert_to_case_insensitive(stage["$match"])
                    processed_pipeline.append(stage)
                results = list(collection.aggregate(processed_pipeline, allowDiskUse=True))
            elif operation_type == "find":
                filter_query = query.get("find", query)
                filter_query = self._convert_to_case_insensitive(filter_query)
                cursor = collection.find(filter_query)
                results = list(cursor)
            else:
                return {"error": f"Unsupported operation type: {operation_type}"}
            
            return {
                "results": json.loads(json.dumps(results, default=str)),
                "count": len(results),
                "query_type": operation_type
            }
        except Exception as e:
            return {"error": str(e)}
        
    def _generate_results_explanation(self, nl_query, mongo_query, results, collection_name):
        """Phase 3: Generate user-friendly results explanation"""
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')
        
        prompt = f"""
You are a helpful MongoDB assistant explaining query results in simple terms.


INSTRUCTIONS:
1. Say Hi 
2. Summarize the key findings from the results
3. Highlight any important numbers or patterns
4. Keep it concise (1-2 short paragraphs max)
5. Use natural, conversational language, but also keep it professional and simple and easy to understand
6. If no results found, suggest possible reasons
7. Tell it in a way that seems natural and human-like, not robotic,don give detailed technical explanations
8. Avoid technical jargon, use simple terms, and keep it friendly
9. Don't talk about the query used or how it was generated, focus on the results and their meaning

# USER ORIGINAL QUESTION: "{nl_query}"

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}

QUERY USED:
{json.dumps(mongo_query, indent=2)}

SAMPLE RESULTS (first 3):
{json.dumps(sample_results, indent=2, default=str)}

"""
        response = self.model.generate_content(prompt)
        return response.text

    def process_query(self, natural_language_query, include_explanation=True):
        """Complete end-to-end query processing"""
        # Phase 1 & 2: Get collection and query
        result = self.natural_language_to_query(natural_language_query)
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }

        collection_name = result["collection"]
        print('The collection used by AI', collection_name)
        mongo_query = result["query"]

        # Execute query
        results = self.execute_query(collection_name, mongo_query)
        
        # Fallback logic for case sensitivity
        if "error" not in results and results.get("count", 0) == 0:
            ci_query = self._convert_to_case_insensitive(mongo_query)
            results = self.execute_query(collection_name, ci_query)
            mongo_query = ci_query

        # Build response
        response = {
            "status": "success" if "error" not in results else "error",
            "collection": collection_name,
            "generated_query": mongo_query,
        }

        if "error" in results:
            response["message"] = results["error"]
        else:
            response.update({
                "results": results["results"],
                "count": results["count"],
                "query_type": results["query_type"]
            })
            if include_explanation:
                # Phase 3: Generate explanation
                response["explanation"] = self._generate_results_explanation(
                    natural_language_query,
                    mongo_query,
                    results,
                    collection_name
                )

        return response