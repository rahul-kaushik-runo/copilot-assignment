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

        # Define lookup relationships between collections
        self.lookup_relationships = {
            # User-related lookups
            "user": {
                "company": {
                    "from": "company",
                    "localField": "company._id",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "role": {
                    "from": "roles",
                    "localField": "role._id",
                    "foreignField": "_id",
                    "as": "roleDetails"
                },
                "license": {
                    "from": "license",
                    "localField": "license._id",
                    "foreignField": "_id",
                    "as": "licenseDetails"
                }
            },
            
            # Customer-related lookups
            "customer": {
                "company": {
                    "from": "company",
                    "localField": "cId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "details": {
                    "from": "customer-details",
                    "localField": "_id",
                    "foreignField": "cusId",
                    "as": "customerDetails"
                },
                "allocations": {
                    "from": "allocation",
                    "localField": "_id",
                    "foreignField": "customer._id",
                    "as": "allocations"
                }
            },
            
            # Allocation-related lookups
            "allocation": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "process": {
                    "from": "process",
                    "localField": "assigned.processId",
                    "foreignField": "_id",
                    "as": "processDetails"
                },
                "assignedUser": {
                    "from": "user",
                    "localField": "assigned.toId",
                    "foreignField": "_id",
                    "as": "assignedUserDetails"
                }
            },
            
            # Call interaction lookups
            "call-interaction": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "user": {
                    "from": "user",
                    "localField": "user._id",
                    "foreignField": "_id",
                    "as": "userDetails"
                },
                "customer": {
                    "from": "customer",
                    "localField": "customer._id",
                    "foreignField": "_id",
                    "as": "customerDetails"
                }
            },
            
            # Email interaction lookups
            "email-interaction": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "sender": {
                    "from": "user",
                    "localField": "sender._id",
                    "foreignField": "_id",
                    "as": "senderDetails"
                },
                "customer": {
                    "from": "customer",
                    "localField": "customer._id",
                    "foreignField": "_id",
                    "as": "customerDetails"
                }
            },
            
            # SMS interaction lookups
            "sms-interaction": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "user": {
                    "from": "user",
                    "localField": "user._id",
                    "foreignField": "_id",
                    "as": "userDetails"
                },
                "customer": {
                    "from": "customer",
                    "localField": "customer._id",
                    "foreignField": "_id",
                    "as": "customerDetails"
                }
            },
            
            # WhatsApp interaction lookups
            "whatsapp-interaction": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "user": {
                    "from": "user",
                    "localField": "user._id",
                    "foreignField": "_id",
                    "as": "userDetails"
                },
                "customer": {
                    "from": "customer",
                    "localField": "customer._id",
                    "foreignField": "_id",
                    "as": "customerDetails"
                }
            },
            
            # CRM interaction lookups
            "crm-interaction": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "customer": {
                    "from": "customer",
                    "localField": "customer._id",
                    "foreignField": "_id",
                    "as": "customerDetails"
                },
                "process": {
                    "from": "process",
                    "localField": "assigned.processId",
                    "foreignField": "_id",
                    "as": "processDetails"
                }
            },
            
            # Transaction lookups
            "transaction": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "user": {
                    "from": "user",
                    "localField": "userId",
                    "foreignField": "_id",
                    "as": "userDetails"
                },
                "licenses": {
                    "from": "license",
                    "localField": "licenseIds",
                    "foreignField": "_id",
                    "as": "licenseDetails"
                }
            },
            
            # License lookups
            "license": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                }
            },
            
            # Process lookups
            "process": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                }
            },
            
            # Reports lookups
            "report-agent-disposition": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "user": {
                    "from": "user",
                    "localField": "user._id",
                    "foreignField": "_id",
                    "as": "userDetails"
                }
            },
            
            "report-agent-login": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "user": {
                    "from": "user",
                    "localField": "user._id",
                    "foreignField": "_id",
                    "as": "userDetails"
                }
            },
            
            # Time log lookups
            "time-log": {
                "company": {
                    "from": "company",
                    "localField": "companyId",
                    "foreignField": "_id",
                    "as": "companyDetails"
                },
                "user": {
                    "from": "user",
                    "localField": "user._id",
                    "foreignField": "_id",
                    "as": "userDetails"
                }
            }
        }

        # Role-based access control configuration
        self.company_field_mapping = {
            # Define which field represents company/organization in each collection
            "report-agent-disposition": "companyId",
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
            "report-agent-login": "companyId",
            "sms-interaction": "companyId",
            "sms-template": "companyId",
            "time-log": "companyId",
            "whatsapp-interaction": "companyId",
            "transaction": "companyId",
            "license": "companyId",
            "allocation": "companyId",
            "customer-details": "cId",
            "customer-assign-log": "cId",
            "rechurn-log": "cId",
            "rechurn-status": "companyId"
        }

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

    def _identify_lookup_requirements(self, query_text):
        """Identify if the query requires lookups and which ones"""
        normalized_query = query_text.lower()
        
        # Keywords that indicate need for lookups
        lookup_indicators = {
            "company": ["company name", "company details", "organization"],
            "user": ["user name", "user details", "agent name", "employee"],
            "customer": ["customer name", "customer details", "client"],
            "process": ["process name", "process details", "workflow"],
            "role": ["role name", "permissions", "access level"],
            "license": ["license details", "subscription", "plan"],
            "assignedUser": ["assigned to", "allocated to", "responsible user"],
            "senderDetails": ["sender name", "sent by"],
            "licenses": ["all licenses", "license list"]
        }
        
        required_lookups = []
        for lookup_type, keywords in lookup_indicators.items():
            if any(keyword in normalized_query for keyword in keywords):
                required_lookups.append(lookup_type)
        
        # Also check for cross-collection queries
        cross_collection_patterns = [
            "with", "along with", "including", "together with",
            "show me", "get", "fetch", "retrieve"
        ]
        
        needs_lookup = any(pattern in normalized_query for pattern in cross_collection_patterns)
        
        return required_lookups, needs_lookup

    def _add_lookup_stages(self, collection_name, query, required_lookups):
        """Add lookup stages to aggregation pipeline"""
        if not isinstance(query, list):
            # Convert find query to aggregation pipeline
            query = [{"$match": query}] if query else [{"$match": {}}]
        
        # Get available lookups for this collection
        available_lookups = self.lookup_relationships.get(collection_name, {})
        
        # Add lookup stages for required lookups
        for lookup_type in required_lookups:
            if lookup_type in available_lookups:
                lookup_config = available_lookups[lookup_type]
                lookup_stage = {
                    "$lookup": {
                        "from": lookup_config["from"],
                        "localField": lookup_config["localField"],
                        "foreignField": lookup_config["foreignField"],
                        "as": lookup_config["as"]
                    }
                }
                query.append(lookup_stage)
                
                # Optionally unwind if it's a single document lookup
                if lookup_type in ["company", "user", "process", "role", "assignedUser"]:
                    query.append({
                        "$unwind": {
                            "path": f"${lookup_config['as']}",
                            "preserveNullAndEmptyArrays": True
                        }
                    })
        
        return query

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
        """Phase 2: Generate query for specific collection with company context and lookups"""
        normalized_query = self.normalize_query(query_text)
        schema = self.full_schemas.get(collection_name)
        if not schema:
            return {"error": f"Collection {collection_name} not found"}

        # Identify lookup requirements
        required_lookups, needs_lookup = self._identify_lookup_requirements(query_text)
        
        # Get available lookups for this collection
        available_lookups = self.lookup_relationships.get(collection_name, {})
        lookup_info = ""
        
        if needs_lookup and available_lookups:
            lookup_info = f"""
AVAILABLE LOOKUPS FOR {collection_name.upper()}:
{self._format_lookup_info(collection_name)}

DETECTED LOOKUP REQUIREMENTS: {', '.join(required_lookups)}

LOOKUP USAGE INSTRUCTIONS:
1. If the query needs data from related collections, use aggregation pipeline with $lookup stages
2. Always use $lookup followed by $unwind for single document relationships
3. For array relationships, just use $lookup without $unwind
4. Add lookups BEFORE any filtering on the looked-up data
5. Use proper field names from the looked-up collections in your query
"""

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
You are a MongoDB query expert with role-based access control and lookup capabilities. You think step by step before generating any query and understand natural language queries very effectively. Given this collection schema, generate a query for:

USER QUERY: "{normalized_query}"

COLLECTION SCHEMA:
{schema}

{company_context}

{lookup_info}

VALUE SYNONYMS TO CONSIDER (use canonical values in query):
{value_synonyms_str}

Respond ONLY with a JSON object: {{ "collection": "{collection_name}", "query": <mongo_query>, "has_lookups": <boolean> }}

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
13. Use projection based on what fields the user needs, don't return everything
14. If the user asks queries like "Who am I?", "What is my role?", "What are my permissions?", "who are my customers etc". use the current user
15. LOOKUPS: If query needs related data, use aggregation pipeline with $lookup stages
16. LOOKUPS: Always follow $lookup with $unwind for single document relationships
17. LOOKUPS: Use proper field names from looked-up collections in filters and projections

VALID QUERY FORMATS:
- Simple find: {{ "field": "value" }}
- Regex find: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- Aggregation: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]
- Aggregation with lookup: [{{ "$lookup": {{ ... }} }}, {{ "$unwind": "$lookupField" }}, {{ "$match": {{ ... }} }}]

CRITICAL RULES - NEVER VIOLATE:
1. For simple queries, use find() format: {{ "field": "value" }}
2. For counting, use aggregation pipeline with $group: [{{ "$group": {{ "_id": null, "count": {{ "$sum": 1 }} }} }}]
3. For complex operations, use aggregation pipeline: [{{ "$match": {{ ... }} }}, {{ "$group": {{ ... }} }}]
4. NEVER use these invalid operators: $count, $sum as top-level, $avg as top-level
5. For case-insensitive text matching, use: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
6. NEVER nest $ operators under $in - this is invalid syntax
7. For case-insensitive matching with $in, use separate $or conditions instead
8. For lookups, always use aggregation pipeline format

EXAMPLE OUTPUTS:
- {{ "collection": "report-agent-disposition", "query": {{ "callType": "inbound" }}, "has_lookups": false }}
- {{ "collection": "user", "query": [{{ "$lookup": {{ "from": "company", "localField": "company._id", "foreignField": "_id", "as": "companyDetails" }} }}, {{ "$unwind": "$companyDetails" }}, {{ "$match": {{ "companyDetails.name": {{ "$regex": "tech", "$options": "i" }} }} }}], "has_lookups": true }}
"""
        response = self.model.generate_content(prompt)
        try:
            # Parse response
            response_text = response.text
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            result = json.loads(response_text.strip().strip('"\''))
            
            # If the AI didn't detect lookups but we think they're needed, add them
            if not result.get("has_lookups", False) and needs_lookup and required_lookups:
                result["query"] = self._add_lookup_stages(collection_name, result["query"], required_lookups)
                result["has_lookups"] = True
                result["auto_added_lookups"] = required_lookups
            
            return result
        except Exception as e:
            return {"error": f"Failed to parse query: {str(e)}", "raw_response": response.text}

    def _format_lookup_info(self, collection_name):
        """Format lookup information for the prompt"""
        lookups = self.lookup_relationships.get(collection_name, {})
        if not lookups:
            return "No lookups available for this collection."
        
        info_lines = []
        for lookup_name, config in lookups.items():
            info_lines.append(
                f"- {lookup_name}: Join with '{config['from']}' collection "
                f"on {config['localField']} = {config['foreignField']} "
                f"(result in '{config['as']}')"
            )
        return "\n".join(info_lines)

    def natural_language_to_query(self, natural_language_query, company_id=None):
        """Main method to convert NL to MongoDB query with company filtering and lookups"""
        # Phase 1: Collection selection
        collection_name = self._select_best_collection(natural_language_query)
        if not collection_name:
            return {"error": "Could not determine appropriate collection"}

        # Phase 2: Query generation with company context and lookups
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
11. If results include data from multiple collections (due to lookups), explain the relationships clearly

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

    def process_query(self, natural_language_query, company_id=None, include_explanation=True):
        """Complete end-to-end query processing with company-based access control and lookups"""
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

        # Phase 1 & 2: Get collection and query with company filtering and lookups
        result = self.natural_language_to_query(natural_language_query, company_id)
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }

        collection_name = result["collection"]
        print(f'The collection used by AI: {collection_name}')
        print(f'Company filter applied: {result.get("company_filtered", False)}')
        print(f'Has lookups: {result.get("has_lookups", False)}')
        if result.get("auto_added_lookups"):
            print(f'Auto-added lookups: {result["auto_added_lookups"]}')
        
        mongo_query = result["query"]

        # Debug: Print the query before execution
        print(f'Generated query: {json.dumps(mongo_query, indent=2, default=self._json_serializer)}')

        # Execute query
        results = self.execute_query(collection_name, mongo_query)

        # Fallback logic for case sensitivity (but preserve ObjectIds)
        if "error" not in results and results.get("count", 0) == 0:
            ci_query = self._convert_to_case_insensitive(mongo_query)
            print(f'Fallback case-insensitive query: {json.dumps(ci_query, indent=2, default=self._json_serializer)}')
            results = self.execute_query(collection_name, ci_query)
            mongo_query = ci_query

        # Build response
        response = {
            "status": "success" if "error" not in results else "error",
            "collection": collection_name,
            "generated_query": self._serialize_for_json(mongo_query),
            "company_filtered": result.get("company_filtered", False),
            "company_id": str(company_id) if isinstance(company_id, ObjectId) else company_id,
            "has_lookups": result.get("has_lookups", False),
            "lookup_collections": self._get_lookup_collections_used(mongo_query) if result.get("has_lookups", False) else []
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

    def _get_lookup_collections_used(self, query):
        """Extract which collections were used in lookups"""
        lookup_collections = []
        if isinstance(query, list):
            for stage in query:
                if "$lookup" in stage:
                    lookup_collections.append(stage["$lookup"]["from"])
        return lookup_collections

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

    def add_lookup_relationship(self, collection_name, lookup_name, from_collection, local_field, foreign_field, as_field):
        """Method to dynamically add new lookup relationships"""
        if collection_name not in self.lookup_relationships:
            self.lookup_relationships[collection_name] = {}
        
        self.lookup_relationships[collection_name][lookup_name] = {
            "from": from_collection,
            "localField": local_field,
            "foreignField": foreign_field,
            "as": as_field
        }

    def get_available_lookups(self, collection_name):
        """Get all available lookups for a collection"""
        return self.lookup_relationships.get(collection_name, {})

    def test_lookup_query(self, collection_name, lookup_types, sample_filter=None):
        """Test method to generate and execute a sample lookup query"""
        if collection_name not in self.lookup_relationships:
            return {"error": f"No lookups defined for collection {collection_name}"}
        
        # Build test aggregation pipeline
        pipeline = []
        
        # Add sample filter if provided
        if sample_filter:
            pipeline.append({"$match": sample_filter})
        
        # Add lookup stages
        for lookup_type in lookup_types:
            if lookup_type in self.lookup_relationships[collection_name]:
                lookup_config = self.lookup_relationships[collection_name][lookup_type]
                pipeline.append({
                    "$lookup": {
                        "from": lookup_config["from"],
                        "localField": lookup_config["localField"],
                        "foreignField": lookup_config["foreignField"],
                        "as": lookup_config["as"]
                    }
                })
                
                # Add unwind for single document lookups
                if lookup_type in ["company", "user", "process", "role", "assignedUser"]:
                    pipeline.append({
                        "$unwind": {
                            "path": f"${lookup_config['as']}",
                            "preserveNullAndEmptyArrays": True
                        }
                    })
        
        # Limit results for testing
        pipeline.append({"$limit": 5})
        
        try:
            collection = self.db[collection_name]
            results = list(collection.aggregate(pipeline))
            return {
                "pipeline": pipeline,
                "results": json.loads(json.dumps(results, default=self._json_serializer)),
                "count": len(results)
            }
        except Exception as e:
            return {"error": str(e), "pipeline": pipeline}


# Example usage with lookup system
def main():
    # Initialize the system
    nl_system = NLToMongoDBQuerySystem()

    # Example: User from company with ObjectId
    company_id = ObjectId("67c6da5aa4171809121d2990")  # Example ObjectId

    # Test queries that require lookups
    test_queries = [
        "Show me all users with their call interactions"
    ]

    print("=== Testing Lookup System ===\n")
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. Testing Query: '{query}'")
        print("-" * 50)
        
        # Process query with company filtering and lookups
        result = nl_system.process_query(query, company_id=company_id)
        
        print(f"Status: {result['status']}")
        print(f"Collection: {result['collection']}")
        print(f"Has Lookups: {result.get('has_lookups', False)}")
        if result.get('lookup_collections'):
            print(f"Lookup Collections: {', '.join(result['lookup_collections'])}")
        print(f"Results Count: {result.get('count', 0)}")
        
        if result['status'] == 'error':
            print(f"Error: {result['message']}")
        else:
            print(f"Query Type: {result['query_type']}")
            # Print first result as sample
            if result.get('results'):
                print("Sample Result:")
                print(json.dumps(result['results'][0], indent=2)[:500] + "..." if len(json.dumps(result['results'][0], indent=2)) > 500 else json.dumps(result['results'][0], indent=2))

    # Test specific lookup functionality
    print("\n=== Testing Lookup Relationships ===")
    
    # Test getting available lookups
    available_lookups = nl_system.get_available_lookups("user")
    print(f"Available lookups for 'user' collection: {list(available_lookups.keys())}")
    
    # Test adding a custom lookup
    nl_system.add_lookup_relationship(
        "custom-collection", 
        "customLookup", 
        "other-collection", 
        "customId", 
        "_id", 
        "customDetails"
    )
    print("Added custom lookup relationship")
    
    # Test lookup query building
    test_result = nl_system.test_lookup_query(
        "call-interaction", 
        ["company", "user", "customer"],
        {"details.type": "outbound"}
    )
    
    if "error" not in test_result:
        print(f"Test lookup query successful, returned {test_result['count']} results")
        print("Pipeline used:")
        print(json.dumps(test_result['pipeline'], indent=2, default=str))
    else:
        print(f"Test lookup query failed: {test_result['error']}")

    print(nl_system._generate_results_explanation(
        "What are the call interactions for users?",
        test_result['pipeline'],
        test_result,
        "call-interaction"
    ))

if __name__ == "__main__":
    main() 