import json
import google.generativeai as genai
from pymongo import MongoClient
import time
from difflib import get_close_matches


class NLToMongoDBQuerySystem:
    def __init__(self):
        # Configuration - replace with your actual values
        API_KEY = "AIzaSyDzq0RE9mmQR6ipTNu4AffCGU6u7FmXQ38"
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
        with open('backend/schema.txt', 'r') as file:
            file_contents = file.read()
        self.SCHEMAS_STR = file_contents
        self.schema_summaries = self._create_schema_summaries()
        self.full_schemas = self._parse_full_schemas()
        self.collection_relationships = self._identify_collection_relationships()

        # Value synonyms (manually maintained or learned)
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }

    def _identify_collection_relationships(self):
        """Identify potential relationships between collections based on schema"""
        relationships = {
        "license": {
            "company": ["companyId"],
            "user": ["user._id"]
        },
        "email-interaction": {
            "company": ["companyId"],
            "customer": ["customer._id"],
            "user": ["sender._id"]
        },
        "rechurn-log": {
            "company": ["cId"],
            "process": ["pId"],
            "customer": ["cusId"],
            "user": ["rBy", "rTo"]
        },
        "report-history": {
            "company": ["cId"],
            "user": ["userId"],
            "process": ["pId"]
        },
        "company": {
            "api-key": ["_id"],
            "process": ["_id"],
            "user": ["_id"]
        },
        "api-key": {
            "company": ["companyId"],
            "user": ["creator._id"]
        },
        "call-interaction": {
            "company": ["companyId"],
            "user": ["user._id"],
            "customer": ["customer._id"],
            "process": ["user.process._id"]
        },
        "customer-assign-log": {
            "company": ["cId"],
            "process": ["pId"],
            "customer": ["cusId"],
            "user": ["asgn.tId", "asgn.fId"]
        },
        "transaction": {
            "company": ["companyId"],
            "user": ["userId"],
            "license": ["licenseIds"]
        },
        "roles": {
            "company": ["cId"]
        },
        "crm-interaction": {
            "company": ["companyId"],
            "customer": ["customer._id"],
            "user": ["assigned.fromId", "assigned.toId"]
        },
        "allocation": {
            "company": ["companyId"],
            "customer": ["customer._id"],
            "user": ["assigned.toId", "assigned.fromId"],
            "process": ["assigned.processId"]
        },
        "time-log": {
            "company": ["companyId"],
            "user": ["user._id"],
            "process": ["user.process._id"]
        },
        "user": {
            "company": ["company._id"],
            "roles": ["role._id"],
            "process": ["process._id"],
            "license": ["license._id"]
        },
        "customer": {
            "company": ["cId"],
            "user": ["creator._id"]
        },
        "whatsapp-interaction": {
            "company": ["companyId"],
            "customer": ["customer._id"],
            "user": ["user._id"]
        },
        "process": {
            "company": ["companyId"]
        },
        "report-agent-login": {
            "company": ["companyId"],
            "user": ["user._id"],
            "process": ["user.process._id"]
        },
        "sms-interaction": {
            "company": ["companyId"],
            "customer": ["customer._id"],
            "user": ["user._id"]
        },
        "email-template": {
            "company": ["companyId"],
            "process": ["processId"],
            "user": ["userId"]
        },
        "whatsapp-template": {
            "company": ["companyId"],
            "process": ["processId"],
            "user": ["userId"]
        },
        "cloud-virtual-number": {
            "company": ["companyId"]
        },
        "customer-details": {
            "company": ["cId"],
            "process": ["pId"],
            "customer": ["cusId"]
        },
        "report-agent-disposition": {
            "company": ["companyId"],
            "user": ["user._id"],
            "process": ["user.process._id"]
        },
        "crm-field": {
            "company": ["companyId"],
            "process": ["processId"]
        },
        "rechurn-status": {
            "company": ["companyId"],
            "user": ["user._id"],
            "process": ["user.processId"]
        },
        "recurring-interaction": {
            "company": ["companyId"],
            "customer": ["customer._id"],
            "user": ["user._id"]
        },
        "sms-template": {
            "company": ["companyId"],
            "process": ["processId"],
            "user": ["userId"]
        }
    }
        # Auto-detect additional relationships by analyzing field names
        collections = self.SCHEMAS_STR.strip().split("Collection: ")[1:]
        collection_fields = {}
        
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            fields = []
            for line in coll_text[name_end+1:].split('\n'):
                if ':' in line:
                    field = line.split(':')[0].strip()
                    fields.append(field)
            collection_fields[collection_name] = fields
        
        # Find common field names between collections
        for coll1, fields1 in collection_fields.items():
            for coll2, fields2 in collection_fields.items():
                if coll1 != coll2:
                    common_fields = set(fields1) & set(fields2)
                    if common_fields:
                        if coll1 not in relationships:
                            relationships[coll1] = {}
                        relationships[coll1][coll2] = list(common_fields)
        
        return relationships

    def _create_schema_summaries(self):
        """Create concise one-line summaries of each collection"""
        summaries = {}
        collections = self.SCHEMAS_STR.strip().split("Collection: ")[1:]
        for coll_text in collections:
            name_end = coll_text.find('\n')
            collection_name = coll_text[:name_end].strip()
            first_line = coll_text[name_end+1:].split('\n')[0].strip()
            summaries[collection_name] = first_line
        return summaries
    
    def _parse_full_schemas(self):
        """Parse full schemas into a dictionary for quick access"""
        schemas = {}
        collections = self.SCHEMAS_STR.strip().split("Collection: ")[1:]
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

    def _analyze_query_complexity(self, query_text):
        """Enhanced Phase 0: Determine if query requires single or multiple collections"""
        normalized_query = self.normalize_query(query_text)
        
        prompt = f"""
You are a MongoDB expert assistant. Analyze this user query to determine if it requires data from ONE collection or MULTIPLE collections.

USER QUERY: "{normalized_query}"

AVAILABLE COLLECTIONS:
{self._format_collection_summaries()}

RELATIONSHIPS BETWEEN COLLECTIONS:
{json.dumps(self.collection_relationships, indent=2)}

ANALYSIS INSTRUCTIONS:
1. Look for queries that ask for relationships, comparisons, or joins between different data types
2. Look for queries that need data from multiple domains (e.g., agent performance AND call history)
3. Look for queries with "and", "with", "combined with", "along with" that suggest multiple data sources
4. Look for queries asking for correlations, ratios, or calculations across different data types
5. Consider known relationships between collections when determining if multiple collections are needed

IMPORTANT: Only suggest MULTIPLE collections if the query EXPLICITLY requires combining data from different sources.
Simple queries that can be answered from one collection should be marked as SINGLE.

Respond with EXACTLY ONE of these formats:
- "SINGLE: <collection_name>" - if query needs only one collection
- "MULTIPLE: <collection1>, <collection2>" - if query needs exactly two collections
- "MULTIPLE: <collection1>, <collection2>, <collection3>" - if query needs three or more collections
- "RELATED: <primary_collection>, <secondary_collection>, <relationship_field>" - if query needs related collections

EXAMPLES:
- "show me all agents" → "SINGLE: agents"
- "agents with their call history" → "RELATED: agents, call-history, agent_id"
- "performance metrics for agents handling inbound calls" → "MULTIPLE: agent-performance, call-disposition"
- "count total calls" → "SINGLE: calls"
- "agent performance data" → "SINGLE: agent-performance"

RESPONSE:"""

        response = self.model.generate_content(prompt)
        try:
            response_text = response.text.strip()
            if response_text.startswith("SINGLE:"):
                collection = response_text.split("SINGLE:")[1].strip()
                return {"type": "single", "collections": [collection]}
            elif response_text.startswith("MULTIPLE:"):
                collections_str = response_text.split("MULTIPLE:")[1].strip()
                collections = [c.strip() for c in collections_str.split(",")]
                return {"type": "multiple", "collections": collections}
            elif response_text.startswith("RELATED:"):
                parts = response_text.split("RELATED:")[1].strip().split(",")
                primary = parts[0].strip()
                secondary = parts[1].strip()
                relationship_field = parts[2].strip() if len(parts) > 2 else None
                return {
                    "type": "related", 
                    "collections": [primary, secondary],
                    "relationship_field": relationship_field
                }
            else:
                return {"type": "single", "collections": [self._select_best_collection(query_text)]}
        except Exception as e:
            print(f"Error analyzing query complexity: {str(e)}")
            return {"type": "single", "collections": [self._select_best_collection(query_text)]}

    def _generate_related_query(self, query_text, primary_collection, secondary_collection, relationship_field=None):
        """Generate a query for related collections with proper joining logic"""
        # First generate individual queries
        primary_query = self._generate_query_for_collection(query_text, primary_collection)
        secondary_query = self._generate_query_for_collection(query_text, secondary_collection)
        
        if "error" in primary_query or "error" in secondary_query:
            return {
                "error": f"Could not generate queries for related collections: {primary_query.get('error', '')} {secondary_query.get('error', '')}"
            }
        
        # If no explicit relationship field was provided, try to find one
        if not relationship_field:
            possible_relationships = self.collection_relationships.get(primary_collection, {}).get(secondary_collection, [])
            relationship_field = possible_relationships[0] if possible_relationships else None
        
        return {
            "collection": primary_collection,
            "query": primary_query["query"],
            "related_query": {
                "collection": secondary_collection,
                "query": secondary_query["query"],
                "relationship_field": relationship_field,
                "primary_field": relationship_field  # Assuming same field name for now
            }
        }

    def execute_related_query(self, query_result):
        """Execute queries for related collections and join the results"""
        primary_collection = query_result["collection"]
        primary_query = query_result["query"]
        related_info = query_result["related_query"]
        secondary_collection = related_info["collection"]
        secondary_query = related_info["query"]
        relationship_field = related_info["relationship_field"]
        
        # Execute primary query
        primary_results = self.execute_query(primary_collection, primary_query)
        if "error" in primary_results or not primary_results["results"]:
            return primary_results
        
        # Get all relationship values from primary results
        relationship_values = []
        for doc in primary_results["results"]:
            if relationship_field in doc:
                value = doc[relationship_field]
                if isinstance(value, list):
                    relationship_values.extend(value)
                else:
                    relationship_values.append(value)
        
        if not relationship_values:
            return {
                "error": f"No relationship field '{relationship_field}' found in primary results",
                "primary_results": primary_results
            }
        
        # Add filter to secondary query to only get related documents
        if isinstance(secondary_query, list):
            # Handle aggregation pipeline
            secondary_query = [{"$match": {relationship_field: {"$in": relationship_values}}}] + secondary_query
        else:
            # Handle find query
            if "$match" in secondary_query:
                secondary_query["$match"][relationship_field] = {"$in": relationship_values}
            else:
                secondary_query[relationship_field] = {"$in": relationship_values}
        
        # Execute secondary query
        secondary_results = self.execute_query(secondary_collection, secondary_query)
        if "error" in secondary_results:
            return {
                "error": f"Primary query succeeded but secondary failed: {secondary_results['error']}",
                "primary_results": primary_results
            }
        
        # Join the results
        joined_results = []
        for primary_doc in primary_results["results"]:
            primary_key = primary_doc.get(relationship_field)
            if not primary_key:
                continue
                
            # Find matching secondary documents
            matching_secondary = []
            for secondary_doc in secondary_results["results"]:
                secondary_key = secondary_doc.get(relationship_field)
                if secondary_key == primary_key or (
                    isinstance(secondary_key, (list, tuple)) and primary_key in secondary_key
                ):
                    matching_secondary.append(secondary_doc)
            
            # Create joined document
            joined_doc = {
                **primary_doc,
                f"related_{secondary_collection}": matching_secondary
            }
            joined_results.append(joined_doc)
        
        return {
            "results": joined_results,
            "count": len(joined_results),
            "query_type": "related",
            "execution_details": {
                "primary_collection": primary_collection,
                "primary_count": primary_results["count"],
                "secondary_collection": secondary_collection,
                "secondary_count": secondary_results["count"],
                "relationship_field": relationship_field
            }
        }

    def natural_language_to_query(self, natural_language_query):
        """Enhanced main method to convert NL to MongoDB query"""
        # Phase 0: Analyze query complexity
        complexity_analysis = self._analyze_query_complexity(natural_language_query)
        
        if complexity_analysis["type"] == "single":
            # Single collection logic
            collection_name = complexity_analysis["collections"][0]
            if not collection_name:
                return {"error": "Could not determine appropriate collection"}
            
            query_result = self._generate_query_for_collection(natural_language_query, collection_name)
            if "error" in query_result:
                return query_result
                
            # Store successful query in history
            self.query_history.append((natural_language_query, query_result))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
                
            return query_result
            
        elif complexity_analysis["type"] == "related":
            # Related collections logic
            primary_collection = complexity_analysis["collections"][0]
            secondary_collection = complexity_analysis["collections"][1]
            relationship_field = complexity_analysis.get("relationship_field")
            
            query_result = self._generate_related_query(
                natural_language_query,
                primary_collection,
                secondary_collection,
                relationship_field
            )
            
            if "error" in query_result:
                return query_result
                
            # Store in history
            self.query_history.append((natural_language_query, query_result))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
                
            return query_result
            
        else:  # Multiple collections - non-related
            collections = complexity_analysis["collections"]
            
            # Generate individual queries for each collection
            all_queries = {}
            for collection_name in collections:
                query_result = self._generate_query_for_collection(natural_language_query, collection_name)
                if "error" not in query_result:
                    all_queries[collection_name] = query_result["query"]
            
            if not all_queries:
                return {"error": "Could not generate queries for any collection"}
            
            # Return a combined result that maintains backward compatibility
            primary_collection = collections[0]  # Use first collection as primary
            
            result = {
                "collection": primary_collection,  # Primary collection for backward compatibility
                "query": all_queries.get(primary_collection, {}),  # Primary query
                "multi_collection_data": {  # Additional data for multi-collection handling
                    "is_multi_collection": True,
                    "all_collections": collections,
                    "all_queries": all_queries
                }
            }
            
            # Store in history
            self.query_history.append((natural_language_query, result))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
                
            return result

    def process_query(self, natural_language_query, include_explanation=True):
        """Enhanced end-to-end query processing with better multi-collection support"""
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
        
        # Check the type of query we're dealing with
        if "related_query" in result:
            print(f'Related collection query detected: {result["related_query"]["collection"]}')
            # Execute related collection query
            results = self.execute_related_query(result)
        elif result.get("multi_collection_data", {}).get("is_multi_collection", False):
            print(f'Multi-collection query detected: {result["multi_collection_data"]["all_collections"]}')
            # Execute multi-collection query
            results = self.execute_multi_collection_query(result)
        else:
            # Execute single collection query
            mongo_query = result["query"]
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
            "generated_query": result.get("query", {}),
        }
        
        # Add metadata based on query type
        if "related_query" in result:
            response["query_type"] = "related"
            response["related_collection"] = result["related_query"]["collection"]
            response["relationship_field"] = result["related_query"]["relationship_field"]
        elif "multi_collection_data" in result:
            response["multi_collection_info"] = {
                "is_multi_collection": True,
                "all_collections": result["multi_collection_data"]["all_collections"],
                "collections_used": results.get("collections_used", []),
                "execution_details": results.get("execution_details", {})
            }

        if "error" in results:
            response["message"] = results["error"]
        else:
            response.update({
                "results": results["results"],
                "count": results["count"],
                "query_type": results.get("query_type", "find")
            })
            if include_explanation:
                is_multi = "multi_collection_data" in result or "related_query" in result
                response["explanation"] = self._generate_results_explanation(
                    natural_language_query,
                    result.get("query", {}),
                    results,
                    collection_name,
                    is_multi
                )

        return response

    # ... (keep all other existing methods unchanged) ...