import json
import google.generativeai as genai
from pymongo import MongoClient
from bson import ObjectId
import time
from difflib import get_close_matches
 
 
with open('backend/schema.txt', 'r') as file:
    file_contents = file.read()
 
SCHEMAS_STR = file_contents
 
 
class NLToMongoDBQuerySystem:
    def __init__(self):
        # Configuration - replace with your actual values
        API_KEY = "AIzaSyDzq0RE9mmQR6ipTNu4AffCGU6u7FmXQ38"
        MONGODB_URI = "mongodb://localhost:27017"
        DB_NAME = "callCrm"
 
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
 
        # Role-based access control configuration
        self.company_field_mapping = {
            # Define which field represents company/organization in each collection
            "report-agent-disposition": "cId",
            "report-history": "cId", 
            "agents": "cId",
            "calls": "cId",
            "email-interaction": "companyId",
            "email-template": "companyId",
            "process": "companyId",
            "roles": "cId",
            "user": "company._id",
            "call-interaction": "companyId",
            "api-key": "companyId",
            "company": "_id",
            "crm-field": "companyId",
            "crm-interaction": "companyId",
            "customer": "cId",
            "recurring-interaction": "companyId",
            "report-agent-disposition": "companyId",
            "report-agent-login": "companyId",
            "report-history": "companyId",
            "roles": "cId",
            "sms-interaction": "companyId",
            "sms-template": "companyId",
            "time-log": "companyId",
            "whatsapp-interaction": "companyId",
            "transaction": "companyId",
        }
        self.role_config = {
            "process_manager": {
                "allowed_collections": [
                    "allocation",
                    "call-interaction",
                    "crm-interaction",
                    "email-interaction",
                    "sms-interaction",
                    "whatsapp-interaction",
                    "customer-assign-log",
                    "rechurn-log",
                    "report-agent-disposition",
                    "report-agent-login",
                    "time-log",
                    "recurring-interaction",
                    "customer",
                    "customer-details"
                ],
                "restricted_fields": {
                    # Fields that should be removed from queries for this role
                    "company": ["billing", "integrations", "autoDialer"],
                    "user": ["tokens", "deviceInfo", "ctmP"],
                    "call-interaction": ["details.recordingUrl"]
                },
                "process_field_mapping": {
                    # Define which field represents process in each collection
                    "allocation": "processId",
                    "call-interaction": "user.process._id",
                    "crm-interaction": "processId",
                    "email-interaction": "sender.processId",
                    "sms-interaction": "user.processId",
                    "whatsapp-interaction": "user.processId",
                    "customer-assign-log": "pId",
                    "rechurn-log": "pId",
                    "report-agent-disposition": "user.process._id",
                    "report-agent-login": "user.process._id",
                    "time-log": "user.process._id",
                    "recurring-interaction": "user.process._id",
                    "customer": "pId",
                    "customer-details": "pId"
                }
            }
        }
 
 
            #
       
       
       
    def _get_process_filter(self, collection_name, process_ids):
        """Generate process filter for the given collection"""
        if not process_ids:
            return None

        process_field = self.role_config["process_manager"]["process_field_mapping"].get(collection_name)
        if not process_field:
            return None

        # Convert string process_ids to ObjectIds if needed
        processed_ids = []
        for pid in process_ids:
            if isinstance(pid, str) and ObjectId.is_valid(pid):
                processed_ids.append(ObjectId(pid))
            elif isinstance(pid, ObjectId):
                processed_ids.append(pid)
            else:
                continue

        if not processed_ids:
            return None

        # Handle nested field paths (like "user.process._id")
        if "." in process_field:
            parts = process_field.split(".")
            filter_expr = {parts[0]: {"$elemMatch": {parts[1]: {"$in": processed_ids}}}}
        else:
            filter_expr = {process_field: {"$in": processed_ids}}

        return filter_expr

    def _inject_process_filter(self, query, process_filter):
        """Inject process filter into the query"""
        if not process_filter:
            return query

        if isinstance(query, list):
            # Aggregation pipeline
            modified_pipeline = []
            match_stage_found = False

            for stage in query:
                if "$match" in stage:
                    existing_match = stage["$match"]
                    if "$and" in existing_match:
                        existing_match["$and"].append(process_filter)
                    else:
                        stage["$match"] = {"$and": [existing_match, process_filter]}
                    match_stage_found = True
                modified_pipeline.append(stage)

            if not match_stage_found:
                modified_pipeline.insert(0, {"$match": process_filter})

            return modified_pipeline
        elif isinstance(query, dict):
            if not query:
                return process_filter
            else:
                return {"$and": [query, process_filter]}
        return query

    def _apply_role_restrictions(self, query_result, role, user_data):
        """Apply role-specific restrictions to the query"""
        if role != "process_manager":
            return query_result

        collection_name = query_result["collection"]
        
        # Check if collection is allowed for process managers
        allowed_collections = self.role_config["process_manager"]["allowed_collections"]
        if collection_name not in allowed_collections:
            return {"error": f"Access to collection '{collection_name}' is restricted for process managers"}

        # Get user's process IDs
        process_ids = user_data.get("process_ids", [])
        if not process_ids:
            return {"error": "Process manager has no assigned processes"}

        # Apply process filter
        process_filter = self._get_process_filter(collection_name, process_ids)
        if process_filter:
            query_result["query"] = self._inject_process_filter(query_result["query"], process_filter)
            query_result["process_filtered"] = True
            query_result["process_ids"] = process_ids

        # Remove restricted fields from the query
        restricted_fields = self.role_config["process_manager"]["restricted_fields"].get(collection_name, [])
        if restricted_fields:
            query_result["query"] = self._remove_restricted_fields(query_result["query"], restricted_fields)

        return query_result

    def _remove_restricted_fields(self, query, fields_to_remove):
        """Recursively remove restricted fields from a query"""
        if not isinstance(query, dict):
            return query

        new_query = {}
        for key, value in query.items():
            if key in fields_to_remove:
                continue
            if isinstance(value, dict):
                new_query[key] = self._remove_restricted_fields(value, fields_to_remove)
            elif isinstance(value, list):
                new_query[key] = [self._remove_restricted_fields(item, fields_to_remove) 
                               if isinstance(item, dict) else item 
                               for item in value]
            else:
                new_query[key] = value
        return new_query
 
 
    def _create_schema_summaries(self):
        """Create concise one-line summaries of each collection"""
        summaries = {}
        collections = SCHEMAS_STR.strip().split("Collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            first_line = coll_text[name_end+1:].split('\n')[0].strip()
            summaries[collection_name] = first_line
        return summaries
 
    def _parse_full_schemas(self):
        """Parse full schemas into a dictionary for quick access"""
        schemas = {}
        collections = SCHEMAS_STR.strip().split("Collection: ")[1:]
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
 
    def _get_company_filter(self, collection_name, company_id):
    
        if not company_id:
            return None

        # Get the single company field for this collection
        company_field = self.company_field_mapping.get(collection_name)
        if not company_field:
            # Fallback to default if not specified
            company_field = "cId"

        # Handle ObjectId conversion
        try:
            if isinstance(company_id, str):
                if ObjectId.is_valid(company_id):
                    company_id = ObjectId(company_id)
                # If not valid ObjectId string, keep as string
            elif isinstance(company_id, ObjectId):
                pass  # Already an ObjectId
        except Exception as e:
            print(f"Warning: Could not process company_id {company_id}: {e}")
            return None

        return {company_field: company_id}
 
    def _inject_company_filter(self, query, company_filter):
        if not company_filter:
            return query

        # Handle different query types
        if isinstance(query, list):
            # Aggregation pipeline
            modified_pipeline = []
            match_stage_found = False

            for stage in query:
                if "$match" in stage:
                    # Check if company filter already exists in the match stage
                    existing_match = stage["$match"]
                    
                    # Get the company field name from company_filter
                    company_field = list(company_filter.keys())[0]
                    company_value = company_filter[company_field]
                    
                    # Check if company filter already exists
                    has_company_filter = False
                    
                    if company_field in existing_match:
                        has_company_filter = True
                    elif "$and" in existing_match:
                        # Check if any condition in $and already has the company filter
                        for condition in existing_match["$and"]:
                            if company_field in condition and condition[company_field] == company_value:
                                has_company_filter = True
                                break
                    
                    # Only add company filter if it doesn't already exist
                    if not has_company_filter:
                        if "$and" in existing_match:
                            existing_match["$and"].append(company_filter)
                        else:
                            stage["$match"] = {"$and": [existing_match, company_filter]}
                    
                    match_stage_found = True
                modified_pipeline.append(stage)

            # If no $match stage found, add one at the beginning
            if not match_stage_found:
                modified_pipeline.insert(0, {"$match": company_filter})

            return modified_pipeline

        elif isinstance(query, dict):
            # Regular find query
            if not query:  # Empty query
                return company_filter
            else:
                # Check if company filter already exists
                company_field = list(company_filter.keys())[0]
                if company_field in query:
                    # Company filter already exists, don't add duplicate
                    return query
                else:
                    # Merge with existing query using $and
                    return {"$and": [query, company_filter]}

        return query
 
    def _select_best_collection(self, query_text):
        """Phase 1: Have LLM select the best collection based on summaries"""
        normalized_query = self.normalize_query(query_text)
 
        print("normalized_query", normalized_query)
 
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
 
    def _generate_query_for_collection(self, query_text, collection_name, company_id=None):
        """Phase 2: Generate query for specific collection with company context"""
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
 
        # Add company context to the prompt if provided
        company_context = ""
        if company_id:
            company_field = self.company_field_mapping.get(collection_name, "cId")
            company_context = f"""
IMPORTANT SECURITY CONTEXT:
- User belongs to company: {company_id}
- Company field in this collection: {company_field}
- DO NOT include company filtering in your query - this will be added automatically
- Focus only on the user's actual query requirements
- The system will automatically ensure data security by adding company filters
"""
        print(schema)
 
        prompt = f"""
You are a MongoDB query expert with role-based access control. You think step by step before generating any query and understand natural language queries very effectively. Given this collection schema, generate a query for:
 
USER QUERY: "{normalized_query}"
 
COLLECTION SCHEMA:
{schema}
 
{company_context}
 
VALUE SYNONYMS TO CONSIDER (use canonical values in query):
{value_synonyms_str}
 
Respond ONLY with a JSON object: {{ "collection": "{collection_name}", "query": <mongo_query> }}
 
CRITICAL RULES:
1. Use proper MongoDB query syntax
2. For text matching, use case-insensitive regex: {{ "$regex": "pattern", "$options": "i" }}
3. For EXACT text matching, use case-insensitive regex with ^ and $ anchors: {{ "$regex": "^pattern$", "$options": "i" }}
4. For partial matching (if explicitly requested), use: {{ "$regex": "pattern", "$options": "i" }}
5. For counting, use aggregation pipeline with $group
6. NEVER use invalid top-level operators like $count, $sum
7. Match query terms to schema fields exactly
8. For state queries, consider both full names and abbreviations using $or
9. Generate efficient queries, don't generate slow queries
10. Generate queries about only what's required, don't generate queries about everything, use proper filtering
11. SECURITY: Never generate queries that could access other companies' data
12. Focus only on the user's actual query requirements, ignore company filtering
VALID QUERY FORMATS:
- Simple find: {{ "field": "value" }}
- Regex find: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- Aggregation: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]
- For aggeration always use $or never use $and ever. 
 
CRITICAL RULES - NEVER VIOLATE:
1. For simple queries, use find() format: {{ "field": "value" }}
2. For counting, use aggregation pipeline with $group: [{{ "$group": {{ "_id": null, "count": {{ "$sum": 1 }} }} }}]
3. For complex operations, use aggregation pipeline: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]
4. NEVER use these invalid operators: $count, $sum as top-level, $avg as top-level
5. For case-insensitive text matching, use: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
6. NEVER nest $ operators under $in - this is invalid syntax
7. For case-insensitive matching with $in, use separate $or conditions instead
 
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
 
    def natural_language_to_query(self, natural_language_query, company_id=None):
        """Main method to convert NL to MongoDB query with company filtering"""
        # Phase 1: Collection selection
        collection_name = self._select_best_collection(natural_language_query)
        if not collection_name:
            return {"error": "Could not determine appropriate collection"}
 
        # Phase 2: Query generation with company context
        query_result = self._generate_query_for_collection(
            natural_language_query, 
            collection_name, 
            company_id
        )
        if "error" in query_result:
            return query_result
 
        # Phase 3: Inject company filter into the generated query
        if company_id:
            company_filter = self._get_company_filter(collection_name, company_id)
            if company_filter:
                query_result["query"] = self._inject_company_filter(
                    query_result["query"], 
                    company_filter
                )
                query_result["company_filtered"] = True
                query_result["company_id"] = company_id
 
        # Store successful query in history
        self.query_history.append((natural_language_query, query_result))
        if len(self.query_history) > 10:
            self.query_history.pop(0)
 
        return query_result
 
    def _convert_to_case_insensitive(self, query):
        """Convert query to case-insensitive version while preserving ObjectIds"""
        if not isinstance(query, dict):
            return query
 
        new_query = {}
        for key, value in query.items():
            if key.startswith("$"):
                # Handle operators like $and, $or
                if isinstance(value, list):
                    new_query[key] = [self._convert_to_case_insensitive(v) for v in value]
                else:
                    new_query[key] = value
            else:
                if isinstance(value, dict):
                    # Handle nested queries - preserve ObjectIds
                    if "$oid" in value:
                        # This is a serialized ObjectId, convert it back
                        try:
                            new_query[key] = ObjectId(value["$oid"])
                        except:
                            new_query[key] = self._convert_to_case_insensitive(value)
                    else:
                        new_query[key] = self._convert_to_case_insensitive(value)
                elif isinstance(value, str):
                    # Check if this might be an ObjectId field
                    if key.lower().endswith('id') or key.lower() in ['company', 'organization', 'user', 'agent']:
                        # Might be an ID field, check if it's a valid ObjectId
                        if ObjectId.is_valid(value):
                            new_query[key] = ObjectId(value)
                        else:
                            # Convert to case-insensitive regex match
                            new_query[key] = {"$regex": f"^{value}$", "$options": "i"}
                    else:
                        # Convert to case-insensitive regex match
                        new_query[key] = {"$regex": f"^{value}$", "$options": "i"}
                elif isinstance(value, ObjectId):
                    # Preserve ObjectIds as-is
                    new_query[key] = value
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
 
            # Convert ObjectIds to strings for JSON serialization
            return {
                "results": json.loads(json.dumps(results, default=self._json_serializer)),
                "count": len(results),
                "query_type": operation_type
            }
        except Exception as e:
            return {"error": str(e)}
 
    def _json_serializer(self, obj):
        """Custom JSON serializer to handle ObjectIds and other MongoDB types"""
        if isinstance(obj, ObjectId):
            return str(obj)
        return str(obj)
 
    def _serialize_for_json(self, obj):
        """Recursively serialize MongoDB objects for JSON response"""
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {key: self._serialize_for_json(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._serialize_for_json(item) for item in obj]
        else:
            return obj
 
    def _generate_results_explanation(self, nl_query, mongo_query, results, collection_name):
 
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
    7. Tell it in a way that seems natural and human-like, not robotic, don't give detailed technical explanations
    8. Avoid technical jargon, use simple terms, and keep it friendly
    9. Don't talk about the query used or how it was generated, focus on the results and their meaning
    10. Say the question the user asked in the beginning, in your own words, to show you understood it
 
    USER ORIGINAL QUESTION: "{nl_query}"
 
    DATABASE COLLECTION: {collection_name}
    QUERY TYPE: {query_type}
    NUMBER OF RESULTS: {result_count}
 
    QUERY USED:
    {json.dumps(self._serialize_for_json(mongo_query), indent=2)}
 
    SAMPLE RESULTS (first 3):
    {json.dumps(sample_results, indent=2, default=str)}
    """
        response = self.model.generate_content(prompt)
        return response.text
 
    def process_query(self, natural_language_query, company_id=None, 
                     user_role=None, user_data=None, include_explanation=True):
        """Complete end-to-end query processing with role-based access control"""
        # Validate company_id is provided for security
        if not company_id:
            return {
                "status": "error",
                "message": "Company ID is required for data access. Please authenticate properly.",
                "security_error": True
            }

        # Ensure company_id is ObjectId if it's a valid string
        if isinstance(company_id, str) and ObjectId.is_valid(company_id):
            company_id = ObjectId(company_id)

        # Phase 1 & 2: Get collection and query with company filtering
        result = self.natural_language_to_query(natural_language_query, company_id)
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }

        # Apply role-based restrictions
        if user_role and user_data:
            result = self._apply_role_restrictions(result, user_role, user_data)
            if "error" in result:
                return {
                    "status": "error",
                    "message": result["error"],
                    "role_restriction": True
                }

        collection_name = result["collection"]
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
            "generated_query": self._serialize_for_json(mongo_query),
            "company_filtered": result.get("company_filtered", False),
            "company_id": str(company_id) if isinstance(company_id, ObjectId) else company_id
        }

        if user_role == "process_manager":
            response["process_filtered"] = result.get("process_filtered", False)
            response["process_ids"] = result.get("process_ids", [])

        if "error" in results:
            response["message"] = results["error"]
        else:
            response.update({
                "results": results["results"],
                "count": results["count"],
                "query_type": results["query_type"]
            })
            if include_explanation:
                response["explanation"] = self._generate_results_explanation(
                    natural_language_query,
                    mongo_query,
                    results,
                    collection_name
                )

        return response
 
    def get_user_company_id(self, user_id):
        """Helper method to get company_id from user_id - returns ObjectId"""
        # This is a placeholder - implement based on your authentication system
        # You might query a users collection or use JWT tokens, session data, etc.
        try:
            users_collection = self.db["users"]  # Adjust collection name as needed
            user = users_collection.find_one({"userId": user_id})
            if user and "cId" in user:
                company_id = user["cId"]
                # Ensure it's an ObjectId
                if isinstance(company_id, str) and ObjectId.is_valid(company_id):
                    return ObjectId(company_id)
                elif isinstance(company_id, ObjectId):
                    return company_id
            return None
        except Exception as e:
            print(f"Error fetching user company: {str(e)}")
            return None
 
    def update_company_field_mapping(self, collection_name, company_field):
        """Method to update company field mappings for collections"""
        self.company_field_mapping[collection_name] = company_field
 
 
# Example usage with role-based access control
def main():
    nl_system = NLToMongoDBQuerySystem()
    
    # Process manager user data
    company_id = ObjectId("67c6da5aa4171809121d2990")
    user_role = "both"
    user_data = {
        "process_ids": [
            ObjectId("67c6da6ececeaaabda7a386b")  # Sales process
            # ObjectId("507f1f77bcf86cd799439013")   # Support process
        ]
    }

    # Process manager query
    user_query = "Give my customer details who are in odisha"
    result = nl_system.process_query(
        user_query, 
        company_id=company_id,
        user_role=user_role,
        user_data=user_data
    )

    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()