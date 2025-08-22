import json
import google.generativeai as genai
from pymongo import MongoClient
from bson import ObjectId
import time
from datetime import datetime, timedelta, timezone
import re
from typing import Dict, List, Optional, Any, Tuple
import threading
from functools import lru_cache
from difflib import get_close_matches
import pytz
import os
import llamaserver

# Load schema from file
with open('schema.txt', 'r') as file:
    SCHEMAS_STR = file.read()
    
    


class CollectionRelationshipMapper:
    """Maps relationships between collections for intelligent lookup generation"""
    
    def __init__(self):
        # Define relationships between collections (from -> to : foreign_key -> local_key)
        self.relationships = {
            # User-related relationships
            "user": {
                "company": {"local": "company._id", "foreign": "_id"},
                "roles": {"local": "role._id", "foreign": "_id"},
                "process": {"local": "process._id", "foreign": "_id"},
                "license": {"local": "license._id", "foreign": "_id"}
            },
            
            # Customer-related relationships
            "customer": {
                "company": {"local": "cId", "foreign": "_id"},
                "customer-details": {"local": "_id", "foreign": "cusId"}
            },
            
            "customer-details": {
                "customer": {"local": "cusId", "foreign": "_id"},
                "company": {"local": "cId", "foreign": "_id"},
                "process": {"local": "pId", "foreign": "_id"},
                "user": {"local": "asgn.toId", "foreign": "_id"}
            },
            
            # Interaction collections
            "call-interaction": {
                "company": {"local": "companyId", "foreign": "_id"},
                "customer": {"local": "customer._id", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.process._id", "foreign": "_id"}
            },
            
            "email-interaction": {
                "company": {"local": "companyId", "foreign": "_id"},
                "customer": {"local": "customer._id", "foreign": "_id"},
                "user": {"local": "sender._id", "foreign": "_id"},
                "process": {"local": "sender.processId", "foreign": "_id"}
            },
            
            "sms-interaction": {
                "company": {"local": "companyId", "foreign": "_id"},
                "customer": {"local": "customer._id", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.processId", "foreign": "_id"}
            },
            
            "whatsapp-interaction": {
                "company": {"local": "companyId", "foreign": "_id"},
                "customer": {"local": "customer._id", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.processId", "foreign": "_id"}
            },
            
            "crm-interaction": {
                "company": {"local": "companyId", "foreign": "_id"},
                "customer": {"local": "customer._id", "foreign": "_id"},
                "process": {"local": "assigned.processId", "foreign": "_id"}
            },
            
            # Allocation and assignment
            "allocation": {
                "company": {"local": "companyId", "foreign": "_id"},
                "customer": {"local": "customer._id", "foreign": "_id"},
                "user": {"local": "assigned.toId", "foreign": "_id"},
                "process": {"local": "assigned.processId", "foreign": "_id"}
            },
            
            "customer-assign-log": {
                "company": {"local": "cId", "foreign": "_id"},
                "customer": {"local": "cusId", "foreign": "_id"},
                "process": {"local": "pId", "foreign": "_id"},
                "user": {"local": "asgn.tId", "foreign": "_id"}
            },
            
            # Report collections
            "report-history": {
                "company": {"local": "cId", "foreign": "_id"},
                "user": {"local": "userId", "foreign": "_id"},
                "process": {"local": "pId", "foreign": "_id"}
            },
            
            "report-agent-login": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.process._id", "foreign": "_id"}
            },
            
            "report-agent-disposition": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.process._id", "foreign": "_id"}
            },
            
            # Templates
            "email-template": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "userId", "foreign": "_id"},
                "process": {"local": "processId", "foreign": "_id"}
            },
            
            "sms-template": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "userId", "foreign": "_id"},
                "process": {"local": "processId", "foreign": "_id"}
            },
            
            "whatsapp-template": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "userId", "foreign": "_id"},
                "process": {"local": "processId", "foreign": "_id"}
            },
            
            # Other collections
            "time-log": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.process._id", "foreign": "_id"}
            },
            
            "license": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"}
            },
            
            "transaction": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "userId", "foreign": "_id"},
                "license": {"local": "licenseIds", "foreign": "_id"}  # Array relationship
            },
            
            "roles": {
                "company": {"local": "cId", "foreign": "_id"}
            },
            
            "process": {
                "company": {"local": "companyId", "foreign": "_id"}
            },
            
            "api-key": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "creator._id", "foreign": "_id"}
            },
            
            "rechurn-log": {
                "company": {"local": "cId", "foreign": "_id"},
                "customer": {"local": "cusId", "foreign": "_id"},
                "process": {"local": "pId", "foreign": "_id"},
                "user": {"local": "rTo", "foreign": "_id"}  # rechurned to user
            },
            
            "rechurn-status": {
                "company": {"local": "companyId", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.processId", "foreign": "_id"}
            },
            
            "crm-field": {
                "company": {"local": "companyId", "foreign": "_id"},
                "process": {"local": "processId", "foreign": "_id"}
            },
            
            "recurring-interaction": {
                "company": {"local": "companyId", "foreign": "_id"},
                "customer": {"local": "customer._id", "foreign": "_id"},
                "user": {"local": "user._id", "foreign": "_id"},
                "process": {"local": "user.process._id", "foreign": "_id"}
            },
            
            "cloud-virtual-number": {
                "company": {"local": "companyId", "foreign": "_id"}
            }
        }
        
        # Common lookup patterns for optimization
        self.common_patterns = {
            "user_with_company": ["user", "company"],
            "user_with_role": ["user", "roles"],
            "customer_with_details": ["customer", "customer-details"],
            "interaction_with_customer": ["call-interaction", "customer"],
            "interaction_with_user": ["call-interaction", "user"],
            "allocation_with_customer": ["allocation", "customer"],
            "report_with_user": ["report-history", "user"]
        }
    
    def get_lookup_path(self, from_collection: str, to_collection: str) -> Optional[Dict[str, str]]:
        """Get the lookup configuration between two collections"""
        if from_collection in self.relationships:
            if to_collection in self.relationships[from_collection]:
                return self.relationships[from_collection][to_collection]
        return None
    
    def find_relationship_path(self, collections: List[str]) -> List[Tuple[str, str, Dict[str, str]]]:
        """Find the optimal path to join multiple collections"""
        if len(collections) < 2:
            return []
        
        # Start with the first collection as base
        base_collection = collections[0]
        lookup_chain = []
        
        for target_collection in collections[1:]:
            lookup_config = self.get_lookup_path(base_collection, target_collection)
            if lookup_config:
                lookup_chain.append((base_collection, target_collection, lookup_config))
            else:
                # Try reverse lookup
                reverse_config = self.get_lookup_path(target_collection, base_collection)
                if reverse_config:
                    # Reverse the local and foreign fields
                    reversed_config = {
                        "local": reverse_config["foreign"], 
                        "foreign": reverse_config["local"]
                    }
                    lookup_chain.append((base_collection, target_collection, reversed_config))
        
        return lookup_chain


class UserContextCache:
    """Thread-safe cache for user context information"""
    
    def __init__(self, db, cache_ttl=300):  # 5 minutes TTL
        self.db = db
        self.cache = {}
        self.cache_timestamps = {}
        self.cache_ttl = cache_ttl
        self.lock = threading.RLock()
    
    def get_user_context(self, user_id: str) -> Dict[str, Any]:
        """Get user context from cache or database"""
        with self.lock:
            # Check if cached and not expired
            if (user_id in self.cache and 
                user_id in self.cache_timestamps and
                time.time() - self.cache_timestamps[user_id] < self.cache_ttl):
                return self.cache[user_id]
            
            # Fetch from database and cache
            context = self._fetch_user_context_from_db(user_id)
            self.cache[user_id] = context
            self.cache_timestamps[user_id] = time.time()
            return context
    
    def invalidate_user(self, user_id: str):
        """Invalidate cache for specific user"""
        with self.lock:
            self.cache.pop(user_id, None)
            self.cache_timestamps.pop(user_id, None)
    
    def clear_cache(self):
        """Clear entire cache"""
        with self.lock:
            self.cache.clear()
            self.cache_timestamps.clear()
    
    def _fetch_user_context_from_db(self, user_id: str) -> Dict[str, Any]:
        """Fetch complete user context from database with optimized queries"""
        try:
            # Single aggregation query to get all user info at once
            pipeline = [
                {"$match": {"_id": user_id}},
                {
                    "$lookup": {
                        "from": "company",
                        "localField": "company._id",
                        "foreignField": "_id",
                        "as": "company_details"
                    }
                },
                {
                    "$lookup": {
                        "from": "roles",
                        "localField": "role._id",
                        "foreignField": "_id", 
                        "as": "role_details"
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "name": 1,
                        "email": 1,
                        "status": 1,
                        "role": 1,
                        "company": 1,
                        "process": 1,
                        "company_details": {"$arrayElemAt": ["$company_details", 0]},
                        "role_details": {"$arrayElemAt": ["$role_details", 0]}
                    }
                }
            ]
            
            user_data = list(self.db.user.aggregate(pipeline))
            if not user_data:
                return {"error": f"User {user_id} not found"}
            
            user = user_data[0]
            
            # Extract and normalize data
            user_roles = []
            if user.get('role') and user['role'].get('name'):
                user_roles.append(user['role']['name'].lower())
            
            # Get company ID - handle both string and ObjectId
            company_id = None
            if user.get('company') and user['company'].get('_id'):
                company_id = user['company']['_id']
                if isinstance(company_id, str):
                    company_id = ObjectId(company_id)
            
            # Process additional role permissions
            permissions = set()
            if user.get('role_details'):
                role_permissions = user['role_details'].get('permissions', [])
                permissions.update(role_permissions)
            
            # Get accessible collections based on roles
            accessible_collections = self._compute_accessible_collections(user_roles)
            
            return {
                "user_id": user_id,
                "user_name": user.get('name', ''),
                "user_email": user.get('email', ''),
                "roles": user_roles,
                "company_id": company_id,
                "company_name": user.get('company_details', {}).get('name', ''),
                "processes": user.get('process', []),
                "permissions": list(permissions),
                "accessible_collections": accessible_collections,
                "status": user.get('status', 'unknown'),
                "cached_at": time.time()
            }
            
        except Exception as e:
            return {"error": f"Failed to fetch user context: {str(e)}"}
    
    def _compute_accessible_collections(self, user_roles: List[str]) -> List[str]:
        """Compute accessible collections based on user roles"""
        # Role-based access control mapping
        ACCESS_LEVEL = {
            "user": ["call-interaction", "user", "time-log", "company", "process", "customer", "customer-details"],
            "sales": ["call-interaction", "user", "time-log", "allocation", "customer", "customer-details", 
                     "crm-interaction", "email-interaction", "sms-interaction", "whatsapp-interaction",
                     "email-template", "sms-template", "whatsapp-template"],
            "admin": ["call-interaction", "user", "time-log", "allocation", "customer", "customer-details",
                     "crm-interaction", "email-interaction", "sms-interaction", "whatsapp-interaction", 
                     "email-template", "sms-template", "whatsapp-template", "roles", "process", "company",
                     "license", "transaction", "api-key", "report-history", "report-agent-login",
                     "report-agent-disposition", "crm-field", "rechurn-log", "rechurn-status",
                     "customer-assign-log", "recurring-interaction", "cloud-virtual-number"],
            "superadmin": "all"
        }
        
        # Available collections (you'd get this from your schema)
        ALL_COLLECTIONS = [
            "call-interaction", "user", "time-log", "company", "process", "customer", "customer-details",
            "allocation", "crm-interaction", "email-interaction", "sms-interaction", "whatsapp-interaction",
            "email-template", "sms-template", "whatsapp-template", "roles", "license", "transaction",
            "api-key", "report-history", "report-agent-login", "report-agent-disposition", "crm-field",
            "rechurn-log", "rechurn-status", "customer-assign-log", "recurring-interaction", "cloud-virtual-number"
        ]
        
        accessible_collections = set()
        
        for role in user_roles:
            if role in ACCESS_LEVEL:
                if ACCESS_LEVEL[role] == "all":
                    return ALL_COLLECTIONS
                else:
                    accessible_collections.update(ACCESS_LEVEL[role])
        
        # Default access if no roles match
        if not accessible_collections:
            accessible_collections = set(ACCESS_LEVEL.get("user", []))
        
        return list(accessible_collections)


class EnhancedNLToMongoDBQuerySystem:
    """Enhanced version with multi-collection support and optimized lookups"""
    
    def __init__(self):
        # Configuration
        API_KEY = "AIzaSyDzq0RE9mmQR6ipTNu4AffCGU6u7FmXQ38"
        MONGODB_URI = "mongodb://localhost:27017"
        DB_NAME = "callCrm"

        # Initialize Gemini
        genai.configure(api_key=API_KEY)
        self.model = genai.GenerativeModel('gemini-2.5-flash')

        # Initialize MongoDB connection
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DB_NAME]
        
        # Initialize components
        self.user_cache = UserContextCache(self.db, cache_ttl=300)
        self.relationship_mapper = CollectionRelationshipMapper()
        
        # Initialize other components
        self.query_history = []
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
        
        # Collections that need company filtering
        self.company_filtered_collections = {
            "call-interaction": "companyId",
            "company": "_id",
            "user": "company._id",
            "time-log": "companyId", 
            "allocation": "companyId",
            "customer": "cId",
            "customer-details": "cId",
            "crm-interaction": "companyId",
            "email-interaction": "companyId",
            "sms-interaction": "companyId",
            "whatsapp-interaction": "companyId",
            "email-template": "companyId",
            "sms-template": "companyId",
            "whatsapp-template": "companyId",
            "license": "companyId",
            "transaction": "companyId",
            "api-key": "companyId",
            "report-history": "cId",
            "report-agent-login": "companyId",
            "report-agent-disposition": "companyId",
            "crm-field": "companyId",
            "rechurn-log": "cId",
            "rechurn-status": "companyId",
            "customer-assign-log": "cId",
            "recurring-interaction": "companyId",
            "cloud-virtual-number": "companyId"
        }
        
        # Time fields for different collections
        self.time_fields = {
            "call-interaction": ["details.startTime", "details.endTime", "createdAt", "updatedAt"],
            "email-interaction": ["createdAt", "updatedAt"],
            "sms-interaction": ["createdAt", "updatedAt"],
            "whatsapp-interaction": ["createdAt", "updatedAt"],
            "allocation": ["createdAt", "updatedAt"],
            "customer-details": ["cAt", "uAt", "dT"],
            "crm-interaction": ["dateTime", "createdAt", "updatedAt"],
            "user": ["createdAt", "updatedAt", "iStat.iOn", "iStat.aOn"],
            "time-log": ["createdAt", "updatedAt"],
            "report-history": ["fromEpoch", "toEpoch", "createdAt", "updatedAt"],
            "transaction": ["transactionDate", "createdAt", "updatedAt"],
            "license": ["expiryDate", "createdAt", "updatedAt"]
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
    
    # _query_complexity(self, query_text: str, accessible_collections: List[str]) -> Tuple[str, List[str]]:
    #     """
    #     Detect if query requires multiple collections and determine the complexity level
    #     Returns: (complexity_level, required_collections)
    #     """
    #     query_lower = query_text.lower()
    #     def _detect
    #     # Keywords that indicate multi-collection queries
    #     multi_collection_keywords = {
    #         # User + Customer/Allocation combinations
    #         "customers assigned to": ["user", "customer-details", "allocation"],
    #         "my customers": ["user", "customer-details"],
    #         "user's customers": ["user", "customer-details"],
    #         "customers allocated to": ["allocation", "customer", "user"],
    #         "allocation to user": ["allocation", "customer", "user"],
            
    #         # User + Company combinations  
    #         "users in company": ["user", "company"],
    #         "company users": ["user", "company"],
    #         "employees": ["user", "company"],
    #         "my company details": ["user", "company"],
    #         "company information": ["user", "company"],
            
    #         # Interaction + Customer/User combinations
    #         "calls with customer": ["call-interaction", "customer"],
    #         "customer calls": ["call-interaction", "customer"],
    #         "calls by user": ["call-interaction", "user"],
    #         "user calls": ["call-interaction", "user"],
    #         "emails to customer": ["email-interaction", "customer"],
    #         "customer emails": ["email-interaction", "customer"],
            
    #         # Customer + Details combinations
    #         "customer details": ["customer", "customer-details"],
    #         "customer information": ["customer", "customer-details"],
            
    #         # Report + User combinations
    #         "user reports": ["report-agent-login", "user"],
    #         "agent reports": ["report-agent-login", "user"],
    #         "login reports": ["report-agent-login", "user"],
    #         "agent login": ["report-agent-login", "user"],
            
    #         # Process + User combinations
    #         "users in process": ["user", "process"],
    #         "process users": ["user", "process"],
    #         "my process": ["user", "process"],
    #         "user process": ["user", "process"],
            
    #         # Complex multi-table queries
    #         "customer interaction history": ["customer", "call-interaction", "email-interaction", "sms-interaction"],
    #         "user activity report": ["user", "call-interaction", "time-log"],
    #         "allocation history": ["allocation", "customer", "user", "customer-assign-log"],
    #     }
        
    #     # Check for direct keyword matches
    #     for keywords, collections in multi_collection_keywords.items():
    #         if keywords in query_lower:
    #             accessible_required = [col for col in collections if col in accessible_collections]
    #             if len(accessible_required) > 1:
    #                 return "multi_collection", accessible_required
        
    #     # Pattern-based detection for more complex cases
    #     patterns = [
    #         # User-related patterns
    #         (r"(user|agent|employee).*(customer|allocation)", ["user", "customer-details"]),
    #         (r"(customer|client).*(assign|allocat).*(user|agent)", ["customer", "allocation", "user"]),
            
    #         # Interaction patterns  
    #         (r"(call|email|sms|whatsapp).*(customer|client)", ["call-interaction", "customer"]),
    #         (r"(customer|client).*(interaction|call|email)", ["customer", "call-interaction"]),
            
    #         # Report patterns
    #         (r"(report|analytic).*(user|agent)", ["report-agent-login", "user"]),
    #         (r"(user|agent).*(performance|report)", ["user", "report-agent-login"]),
            
    #         # Company patterns
    #         (r"(my|our).*(company|organization)", ["user", "company"]),
    #         (r"company.*(user|employee|staff)", ["company", "user"]),
    #     ]
        
    #     for pattern, collections in patterns:
    #         if re.search(pattern, query_lower):
    #             accessible_required = [col for col in collections if col in accessible_collections]
    #             if len(accessible_required) > 1:
    #                 return "multi_collection", accessible_required
        
    #     # If no multi-collection patterns found, use single collection logic
    #     return "single_collection", []
    
    def _select_collections_for_query(self, query_text: str, accessible_collections: List[str]) -> Tuple[str, List[str]]:
    
        selected_collections = self._select_best_collection(query_text, accessible_collections)
        
        if len(selected_collections) == 1:
            return selected_collections[0], []
        elif len(selected_collections) > 1:
            return selected_collections[0], selected_collections[1:]
        else:
            return None, []

    
    def _select_best_collection(self, query_text: str, accessible_collections: List[str]) -> Optional[List[str]]:
        if not accessible_collections:
            return None

        normalized_query = self.normalize_query(query_text)

        # Filter summaries to only include accessible collections
        accessible_summaries = {
            name: summary for name, summary in self.schema_summaries.items()
            if name in accessible_collections
        }

        prompt = f"""
    You are a MongoDB expert assistant. Analyze this query and determine which collection(s) are needed.

    USER QUERY: "{normalized_query}"

    AVAILABLE COLLECTIONS:
    {self._format_accessible_collection_summaries(accessible_summaries)}

    INSTRUCTIONS:
    1. Analyze the user's query intent
    2. Determine which collections contain the required data
    3. If query needs data from multiple collections, return all needed collections
    4. If only one collection is needed, return just that one
    5. Prioritize selecting only one collection if you think only one is enough, for example if the user asks "tell me the customer assigned to me" just select customer-details as it has the details of who its assigned to. 

    RESPONSE FORMAT - Return ONLY a JSON array:
    ["collection1"] or ["collection1", "collection2", "collection3"]

    EXAMPLES:
    - "show me users" → ["user"]
    - "users with call statistics" → ["user", "call-interaction"]
    - "customer interactions" → ["customer", "call-interaction", "email-interaction"]

    Respond with ONLY the JSON array:
    """

        response = self.model.generate_content(prompt,generation_config=genai.types.GenerationConfig(temperature=0.0))
        
        try:
            # Try to parse the response as JSON array
            response_text = response.text.strip()
            # Remove markdown if present
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()

            # Remove any leading/trailing whitespace and quotes
            response_text = response_text.strip().strip('"\'')
            # Find the first [ and last ] to extract the array
            if '[' in response_text and ']' in response_text:
                response_text = response_text[response_text.find('['):response_text.rfind(']')+1]

            selected_collections = json.loads(response_text)
            # Filter to only accessible collections
            selected_collections = [col for col in selected_collections if col in accessible_collections]
            print(selected_collections)
            return selected_collections if selected_collections else None

        except Exception as e:
            print(f"Error parsing collection selection: {str(e)}")
            return None

    
    def _format_accessible_collection_summaries(self, accessible_summaries):
        """Format accessible collection summaries for selection prompt"""
        return "\n".join(
            f"- {name}: {summary}" 
            for name, summary in accessible_summaries.items()
        )
    
    def _build_lookup_pipeline(self, primary_collection: str, additional_collections: List[str], 
                             base_query: Dict[str, Any], user_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Build an optimized aggregation pipeline with lookups
        """
        pipeline = []
        
        # Start with base match stage (with company filtering if needed)
        if isinstance(base_query, dict):
            pipeline.append({"$match": base_query})
        
        # Add lookups for additional collections
        for collection in additional_collections:
            lookup_config = self.relationship_mapper.get_lookup_path(primary_collection, collection)
            if lookup_config:
                lookup_stage = {
                    "$lookup": {
                        "from": collection,
                        "localField": lookup_config["local"],
                        "foreignField": lookup_config["foreign"],
                        "as": f"{collection}_data"
                    }
                }
                pipeline.append(lookup_stage)
                
                # Add unwind if needed (for single document relationships)
                if not self._is_array_relationship(lookup_config["local"]):
                    pipeline.append({
                        "$unwind": {
                            "path": f"${collection}_data",
                            "preserveNullAndEmptyArrays": True
                        }
                    })
        
        return pipeline
    
    def _is_array_relationship(self, field_path: str) -> bool:
        """Determine if a field represents an array relationship"""
        # Fields that are typically arrays
        array_fields = ["licenseIds", "process", "userIds", "permissions"]
        return any(array_field in field_path for array_field in array_fields)
    
    def _generate_multi_collection_query(self, query_text: str, primary_collection: str, 
                                   additional_collections: List[str], user_context: Dict[str, Any]) -> Dict[str, Any]:
        normalized_query = self.normalize_query(query_text)
        
        # Get schemas for all involved collections
        schemas_text = f"PRIMARY COLLECTION:\n{self.full_schemas.get(primary_collection, '')}\n\n"
        schemas_text += "ADDITIONAL COLLECTIONS:\n"
        for collection in additional_collections:
            schemas_text += f"{self.full_schemas.get(collection, '')}\n\n"
        
        # Get relationship information
        relationships_text = "RELATIONSHIPS:\n"
        for collection in additional_collections:
            lookup_config = self.relationship_mapper.get_lookup_path(primary_collection, collection)
            if lookup_config:
                relationships_text += f"- {primary_collection} -> {collection}: {lookup_config['local']} -> {lookup_config['foreign']}\n"
        
        # Parse time expressions
        time_context = self._parse_time_with_llm_fallback(query_text)
        time_filter_note = ""
        if time_context:
            time_fields_for_collection = self.time_fields.get(primary_collection, [])
            if time_fields_for_collection:
                primary_time_field = time_fields_for_collection[0]
                time_filter_note = f"""
    TIME FILTERING DETECTED:
    - User asked about "{time_context['phrase']}"
    - Time range: {time_context['start_readable']} to {time_context['end_readable']}
    - Unix timestamps: {time_context['start_unix']} to {time_context['end_unix']}
    - Add to $match stage: {{"{primary_time_field}": {{"$gte": {time_context['start_unix']}, "$lte": {time_context['end_unix']}}}}}
    """

        # UPDATED PROMPT - No company filtering mentioned
        # UPDATED PROMPT - Clearer instructions for aggregation pipeline
        prompt = f"""
        You are a MongoDB aggregation expert. Generate a valid aggregation pipeline for this multi-collection query.

        USER QUERY: "{normalized_query}"

        {schemas_text}

        {relationships_text}

        {time_filter_note}

        CRITICAL FORMATTING RULES:
        1. Return ONLY valid JSON without any comments
        2. ALL property names and string values must be in double quotes
        3. NO trailing commas anywhere
        4. NO JavaScript syntax - pure JSON only
        5. NO single quotes - only double quotes

        IMPORTANT AGGREGATION PIPELINE STRUCTURE:
        - An aggregation pipeline is an ARRAY of stages
        - Each stage is an object with exactly ONE operator (like $match, $lookup, $project)
        - NEVER nest aggregation stages inside other operators like $and

        SECURITY NOTE:
        - DO NOT include any company or user filtering in your pipeline
        - Focus only on the business logic of the query
        - Security filtering will be applied separately

        INSTRUCTIONS:
        1. Create an aggregation pipeline ARRAY with proper stages
        2. Start with $match stage for business filtering (NO company filters)
        3. Add $lookup stages for each additional collection using the relationship mappings
        4. Use $unwind for single-document relationships (preserve nulls with preserveNullAndEmptyArrays: true)
        5. Add additional $match stages after lookups if needed for joined data filtering
        6. Add $project stage to select relevant fields from primary and joined collections
        7. Add $group, $sort, or other stages as needed

        CORRECT EXAMPLE FORMAT:
        {{
        "collection": "call-interaction",
        "query": [
            {{"$match": {{"details.type": "missed"}}}},
            {{"$lookup": {{"from": "customer", "localField": "customer._id", "foreignField": "_id", "as": "customer_data"}}}},
            {{"$unwind": {{"path": "$customer_data", "preserveNullAndEmptyArrays": true}}}},
            {{"$project": {{"details.phoneNumber": 1, "customer_data.name": 1, "_id": 0}}}}
        ]
        }}

        WRONG EXAMPLE (DO NOT DO THIS):
        {{
        "collection": "call-interaction", 
        "query": {{
            "$and": [
            {{"companyId": "..."}},
            {{"$match": {{"details.type": "missed"}}}},  // ❌ $match inside $and
            {{"$project": {{"details.phoneNumber": 1}}}}  // ❌ $project inside $and
            ]
        }}
        }}

        Respond with ONLY the JSON object with NO COMMENTS:
        """
        
        try:
            response = self.model.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.0))
            response_text = response.text.strip()
            
            # Clean up response and remove comments
            response_text = self._clean_json_response(response_text)
            
            parsed_result = json.loads(response_text)
            
            return parsed_result
            
        except json.JSONDecodeError as e:
            fixed_json = self._fix_json_issues(response_text if 'response_text' in locals() else response.text)
            try:
                return json.loads(fixed_json)
            except:
                return {"error": f"Failed to parse multi-collection query: {str(e)}"}
        except Exception as e:
            return {"error": f"Failed to generate multi-collection query: {str(e)}"}
        
    
  
            
        except json.JSONDecodeError as e:
            # If JSON parsing fails, try to fix common issues and retry
            fixed_json = self._fix_json_issues(response_text if 'response_text' in locals() else response.text)
            try:
                parsed_result = json.loads(fixed_json)
                if company_id:
                    parsed_result["query"] = self._replace_company_id_placeholder(
                        parsed_result["query"], 
                        company_id,
                        user_context.get('user_id')
                    )
                return parsed_result
            except:
                return {
                    "error": f"Failed to parse multi-collection query as JSON: {str(e)}", 
                    "raw_response": response.text,
                    "cleaned_response": response_text if 'response_text' in locals() else "N/A",
                    "fixed_json": fixed_json if 'fixed_json' in locals() else "N/A"
                }
        except Exception as e:
            return {"error": f"Failed to generate multi-collection query: {str(e)}", "raw_response": response.text}

    
    def _generate_single_collection_query(self, query_text: str, collection_name: str, 
                                    user_context: Dict[str, Any]) -> Dict[str, Any]:
        normalized_query = self.normalize_query(query_text)
        schema = self.full_schemas.get(collection_name)
        if not schema:
            return {"error": f"Collection {collection_name} not found"}
            
        time_context = self._parse_time_with_llm_fallback(query_text)
        
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])
        
        time_filter_note = ""
        if time_context:
            time_fields_for_collection = self.time_fields.get(collection_name, [])
            if time_fields_for_collection:
                primary_time_field = time_fields_for_collection[0]
                time_filter_note = f"""
    TIME FILTERING DETECTED:
    - User asked about "{time_context['phrase']}"
    - Time range: {time_context['start_readable']} to {time_context['end_readable']}
    - Unix timestamps: {time_context['start_unix']} to {time_context['end_unix']}
    - For this collection, use time field: "{primary_time_field}"
    - Add time filter: {{"{primary_time_field}": {{"$gte": {time_context['start_unix']}, "$lte": {time_context['end_unix']}}}}}
    """
        
        # UPDATED PROMPT - No company filtering mentioned
        prompt = f"""
    You are a MongoDB query expert. Generate a valid JSON response for this natural language query.

    USER QUERY: "{normalized_query}"

    COLLECTION SCHEMA:
    {schema}

    {time_filter_note}

    IMPORTANT SECURITY NOTE:
    - DO NOT include any company or user filtering in your query
    - Focus only on the business logic of the query
    - Security filtering will be applied separately

    VALUE SYNONYMS TO CONSIDER (use canonical values in query):
    {value_synonyms_str}

    CRITICAL INSTRUCTIONS:
    1. Respond with ONLY valid JSON in this exact format:
    {{"collection": "{collection_name}", "query": <mongo_query>}}

    2. The <mongo_query> must be valid MongoDB query syntax
    3. For text matching, use case-insensitive regex: {{"$regex": "pattern", "$options": "i"}}
    4. For counting, use aggregation pipeline: [{{"$match": {{"business_field": "value"}}}}, {{"$group": {{"_id": null, "count": {{"$sum": 1}}}}}}]
    5. Use only standard JSON types (string, number, boolean, array, object)
    6. NO MongoDB-specific types like ObjectId() in the JSON response
    7. Use projection based on what fields the user needs, don't return everything

    VALID EXAMPLE RESPONSES:
    {{"collection": "call-interaction", "query": {{"details.type": "inbound", "status": "completed"}}}}
    {{"collection": "user", "query": [{{"$match": {{"role.name": "sales"}}}}, {{"$group": {{"_id": null, "count": {{"$sum": 1}}}}}}]}}
    {{"collection": "user", "query": [{{"$match": {{"status": "active"}}}}, {{"$project": {{"name": 1, "email": 1}}}}]}}

    Respond with ONLY the JSON object, no additional text or formatting:
    """
        
        try:
            response = self.model.generate_content(prompt, generation_config=genai.types.GenerationConfig(temperature=0.0))
            response_text = response.text.strip()
            
            # Clean up response
            response_text = self._clean_json_response(response_text)
            
            parsed_result = json.loads(response_text)
            return parsed_result
            
        except json.JSONDecodeError as e:
            return {"error": f"Failed to parse query as JSON: {str(e)}"}
        except Exception as e:
            return {"error": f"Failed to generate query: {str(e)}"}
        
    def enforce_tenant_filter(self, query: Any, collection_name: str, 
                         company_id: str, user_id: str = None) -> Any:
        """
        Enforces tenant-level security filtering at the application level.
        Applies company filtering and optionally user filtering.
        Handles both ObjectId and UUID string formats.
        """
        if collection_name not in self.company_filtered_collections:
            return query  # No security filtering needed
        
        # Convert company_id to appropriate format
        try:
            # Try to convert to ObjectId (for 24-character hex strings)
            company_filter_value = ObjectId(company_id)
        except:
            # If it fails, use as string (for UUIDs)
            company_filter_value = company_id
        
        company_field = self.company_filtered_collections[collection_name]
        
        # Build security filter
        security_filter = {company_field: company_filter_value}
        
        # Add user-specific filtering for certain collections
        user_specific_collections = ["user", "customer-details", "allocation", "time-log"]
        if user_id and collection_name in user_specific_collections:
            # Convert user_id to appropriate format
            try:
                # Try to convert to ObjectId (for 24-character hex strings)
                user_filter_value = ObjectId(user_id)
            except:
                # If it fails, use as string (for UUIDs)
                user_filter_value = user_id
            
            if collection_name == "user":
                security_filter["_id"] = user_filter_value
            elif collection_name == "customer-details":
                security_filter["asgn.toId"] = user_filter_value
            elif collection_name == "allocation":
                security_filter["assigned.toId"] = user_filter_value
            elif collection_name == "time-log":
                security_filter["user._id"] = user_filter_value
        
        # Apply security filter based on query type
        if isinstance(query, list):
            # Aggregation pipeline - prepend $match stage
            return [{"$match": security_filter}] + query
        elif isinstance(query, dict):
            # Find query - combine with $and
            if not query:  # Empty query
                return security_filter
            else:
                return {"$and": [security_filter, query]}
        
        return query
        
    def _validate_query_structure(self, query, collection_name):
    
        if isinstance(query, list):
            # This is an aggregation pipeline
            for stage in query:
                if not isinstance(stage, dict):
                    return False, "Aggregation stages must be objects"
                if len(stage) != 1:
                    return False, "Each aggregation stage should have exactly one operator"
                operator = list(stage.keys())[0]
                if not operator.startswith('$'):
                    return False, f"Invalid aggregation operator: {operator}"
            return True, None
        elif isinstance(query, dict):
            # This is a find query
            for key in query.keys():
                if key.startswith('$') and key not in ['$and', '$or', '$nor', '$not']:
                    return False, f"Find query cannot contain aggregation operator: {key}"
            return True, None
        else:
            return False, "Query must be either a dictionary (find) or list (aggregation)"
            
    def normalize_query(self, query):
        """Normalize natural language query using value synonyms"""
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized
    
    def _parse_time_with_llm_fallback(self, query_text):
        """Parse time using regex first, fallback to LLM for complex cases"""
        # First try regex patterns (fast, reliable)
        regex_result = self._parse_relative_time(query_text)
        if regex_result:
            return regex_result
        
        # Fallback to LLM for complex cases
        return self._llm_parse_time(query_text)
    
    def _parse_relative_time(self, query_text):
        """Parse relative time expressions and return Unix timestamp ranges"""
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist)
        query_lower = query_text.lower()
        
        # First, try to match dynamic patterns like "last X days/weeks/months"
        dynamic_match = self._parse_dynamic_time(query_lower, now)
        if dynamic_match:
            return dynamic_match
        
        # Fallback to predefined time ranges
        time_ranges = {
            "today": {
                "start": now.replace(hour=0, minute=0, second=0, microsecond=0),
                "end": now.replace(hour=23, minute=59, second=59, microsecond=999999)
            },
            "yesterday": {
                "start": (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0),
                "end": (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
            },
            "last week": {
                "start": now - timedelta(days=7),
                "end": now
            },
            "this week": {
                "start": now - timedelta(days=now.weekday()),
                "end": now
            },
            "last month": {
                "start": now - timedelta(days=30),
                "end": now
            },
            "this month": {
                "start": now.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
                "end": now
            },
            "last 7 days": {
                "start": now - timedelta(days=7),
                "end": now
            },
            "last 30 days": {
                "start": now - timedelta(days=30),
                "end": now
            }
        }
        
        for time_phrase, time_range in time_ranges.items():
            if time_phrase in query_lower:
                start_unix = int(time_range["start"].timestamp())
                end_unix = int(time_range["end"].timestamp())
                return {
                    "phrase": time_phrase,
                    "start_unix": start_unix,
                    "end_unix": end_unix,
                    "start_readable": time_range["start"].strftime("%Y-%m-%d %H:%M:%S"),
                    "end_readable": time_range["end"].strftime("%Y-%m-%d %H:%M:%S")
                }
        
        return None
    
    def _parse_dynamic_time(self, query_lower, now):
        """Parse dynamic time expressions like 'last 5 days', 'past 3 weeks', etc."""
        patterns = [
            (r'last\s+(\d+)\s+(day|days)', 'days'),
            (r'last\s+(\d+)\s+(week|weeks)', 'weeks'),
            (r'last\s+(\d+)\s+(month|months)', 'months'),
            (r'last\s+(\d+)\s+(year|years)', 'years'),
            (r'last\s+(\d+)\s+(hour|hours)', 'hours'),
            (r'last\s+(\d+)\s+(minute|minutes)', 'minutes'),
            (r'past\s+(\d+)\s+(day|days)', 'days'),
            (r'past\s+(\d+)\s+(week|weeks)', 'weeks'),
            (r'past\s+(\d+)\s+(month|months)', 'months'),
            (r'past\s+(\d+)\s+(year|years)', 'years'),
            (r'past\s+(\d+)\s+(hour|hours)', 'hours'),
            (r'past\s+(\d+)\s+(minute|minutes)', 'minutes'),
            (r'previous\s+(\d+)\s+(day|days)', 'days'),
            (r'previous\s+(\d+)\s+(week|weeks)', 'weeks'),
            (r'previous\s+(\d+)\s+(month|months)', 'months'),
            (r'previous\s+(\d+)\s+(year|years)', 'years'),
            (r'(\d+)\s+(day|days)\s+ago', 'days'),
            (r'(\d+)\s+(week|weeks)\s+ago', 'weeks'),
            (r'(\d+)\s+(month|months)\s+ago', 'months'),
            (r'(\d+)\s+(year|years)\s+ago', 'years'),
        ]
        
        for pattern, unit_type in patterns:
            match = re.search(pattern, query_lower)
            if match:
                number = int(match.group(1))
                return self._calculate_time_range(now, number, unit_type, query_lower)
        
        return None
    
    def _calculate_time_range(self, now, number, unit_type, original_query):
        """Calculate start and end times based on the time unit"""
        if unit_type == 'minutes':
            start_time = now - timedelta(minutes=number)
            phrase = f"last {number} minute{'s' if number != 1 else ''}"
        elif unit_type == 'hours':
            start_time = now - timedelta(hours=number)
            phrase = f"last {number} hour{'s' if number != 1 else ''}"
        elif unit_type == 'days':
            start_time = now - timedelta(days=number)
            phrase = f"last {number} day{'s' if number != 1 else ''}"
        elif unit_type == 'weeks':
            start_time = now - timedelta(weeks=number)
            phrase = f"last {number} week{'s' if number != 1 else ''}"
        elif unit_type == 'months':
            start_time = now - timedelta(days=number * 30)
            phrase = f"last {number} month{'s' if number != 1 else ''}"
        elif unit_type == 'years':
            start_time = now - timedelta(days=number * 365)
            phrase = f"last {number} year{'s' if number != 1 else ''}"
        else:
            return None
        
        # Determine the phrase based on what was actually found in the query
        if 'past' in original_query:
            phrase = phrase.replace('last', 'past')
        elif 'previous' in original_query:
            phrase = phrase.replace('last', 'previous')
        elif 'ago' in original_query:
            phrase = f"{number} {unit_type} ago"
        
        end_time = now
        
        start_unix = int(start_time.timestamp())
        end_unix = int(end_time.timestamp())
        
        return {
            "phrase": phrase,
            "start_unix": start_unix,
            "end_unix": end_unix,
            "start_readable": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_readable": end_time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def _llm_parse_time(self, query_text):
        """Use LLM to parse complex time expressions that regex couldn't handle"""
        current_time = datetime.now()
        
        prompt = f"""
You are a time expression parser. Extract time range from the given query.

CURRENT TIME: {current_time.strftime("%Y-%m-%d %H:%M:%S")} ({current_time.strftime("%A")})

QUERY: "{query_text}"

If the query contains a time expression, return a JSON object with:
{{
    "phrase": "the exact time phrase found",
    "start_unix": unix_timestamp_for_start,
    "end_unix": unix_timestamp_for_end,
    "start_readable": "YYYY-MM-DD HH:MM:SS",
    "end_readable": "YYYY-MM-DD HH:MM:SS"
}}

If no time expression is found, return: null

Examples:
- "early this morning" → time range for 6 AM to 9 AM today
- "during business hours yesterday" → 9 AM to 5 PM yesterday
- "over the weekend" → Saturday 00:00 to Sunday 23:59 of last weekend
- "the day before yesterday" → full day range 2 days ago
- "between last Tuesday and Friday" → from last Tuesday 00:00 to last Friday 23:59

RESPOND WITH ONLY THE JSON OBJECT OR null:
"""
        
        try:
            response = self.model.generate_content(prompt,generation_config=genai.types.GenerationConfig(temperature=0.0))
            response_text = response.text.strip()
            
            if response_text.lower() == 'null' or not response_text:
                return None
            
            # Clean up response
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            parsed = json.loads(response_text)
            
            # Validate the response structure
            required_fields = ["phrase", "start_unix", "end_unix", "start_readable", "end_readable"]
            if all(field in parsed for field in required_fields):
                return parsed
            else:
                print(f"LLM time parsing returned incomplete response: {parsed}")
                return None
                
        except Exception as e:
            print(f"Error in LLM time parsing: {str(e)}")
            return None
    
    def get_user_context(self, user_id: str) -> Dict[str, Any]:
        """Get user context from cache (fast!)"""
        return self.user_cache.get_user_context(user_id)
    
    def invalidate_user_cache(self, user_id: str):
        """Invalidate cache when user data changes"""
        self.user_cache.invalidate_user(user_id)
    
    
    
    
    
    
    
    
    
    
    
    def _convert_to_case_insensitive(self, query):
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
                elif isinstance(value, str) and key != "companyId":
                    # Skip regex for UUID fields (like user IDs) and other special fields
                    if key.lower().endswith('id') or key.lower().endswith('_id'):
                        new_query[key] = value  # Keep exact match for ID fields
                    else:
                        # Convert to case-insensitive regex match for non-ID fields
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

    def execute_query(self, collection_name, query, limit=50):
        """Execute MongoDB query with proper validation and error handling"""
        try:
            # VALIDATE query structure first (CRITICAL - preserves original security)
            is_valid, error_msg = self._validate_query_structure(query, collection_name)
            if not is_valid:
                return {"error": f"Invalid query structure: {error_msg}"}
            
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            results = None

            if operation_type == "aggregate":
                pipeline = query if isinstance(query, list) else []
                
                # Process pipeline for case insensitivity
                processed_pipeline = []
                for stage in pipeline:
                    if "$match" in stage:
                        stage["$match"] = self._convert_to_case_insensitive(stage["$match"])
                    processed_pipeline.append(stage)
                
                # Add limit to pipeline if not already present
                has_limit = any("$limit" in stage for stage in processed_pipeline)
                if not has_limit:
                    processed_pipeline.append({"$limit": limit})
                
                results = list(collection.aggregate(processed_pipeline, allowDiskUse=True))
                
            elif operation_type == "find":
                filter_query = query if isinstance(query, dict) else {}
                filter_query = self._convert_to_case_insensitive(filter_query)
                cursor = collection.find(filter_query).limit(limit)
                results = list(cursor)
                
            else:
                return {"error": f"Unsupported operation type: {operation_type}"}

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
        """Generate natural language explanation of query results"""
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')

        prompt = f"""
You are a helpful MongoDB assistant explaining query results in simple terms.

INSTRUCTIONS:
1. Start by rephrasing the user's question to show understanding
2. Summarize the key findings from the results
3. Highlight any important numbers or patterns
4. Explained in a natural manner, if details are asked then give proper details as a list 
5. Use natural, conversational language
6. If no results found, suggest possible reasons
7. Focus on the business meaning, not technical details

USER ORIGINAL QUESTION: "{nl_query}"

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}

SAMPLE RESULTS:
{json.dumps(sample_results, indent=2, default=str)}
""" 
        response = self.model.generate_content(prompt,generation_config=genai.types.GenerationConfig(temperature=0.2))
        return response.text

    def process_query(self, natural_language_query: str, user_id: str, 
                 include_explanation: bool = True, limit: int = 50) -> Dict[str, Any]:
        start_time = time.time()
        
        # Get user context from cache
        user_context = self.get_user_context(user_id)
        if "error" in user_context:
            return {
                "status": "error",
                "message": user_context["error"],
                "security_error": True,
                "processing_time": time.time() - start_time
            }
        
        accessible_collections = user_context.get('accessible_collections', [])
        if not accessible_collections:
            return {
                "status": "error",
                "message": "User has no access to any collections",
                "security_error": True,
                "processing_time": time.time() - start_time
            }
        
        # Select collections
        selected_collections = self._select_best_collection(natural_language_query, accessible_collections)
        if not selected_collections:
            return {
                "status": "error",
                "message": "Could not determine appropriate collection",
                "processing_time": time.time() - start_time
            }
        
        # Generate query (without security filters)
        if len(selected_collections) > 1:
            primary_collection = selected_collections[0]
            additional_collections = selected_collections[1:]
            query_result = self._generate_multi_collection_query(
                natural_language_query, primary_collection, additional_collections, user_context
            )
            complexity = "multi_collection"
        else:
            primary_collection = selected_collections[0]
            additional_collections = []
            query_result = self._generate_single_collection_query(
                natural_language_query, primary_collection, user_context
            )
            complexity = "single_collection"
        
        if "error" in query_result:
            return {
                "status": "error",
                "message": query_result["error"],
                "processing_time": time.time() - start_time
            }
        
        mongo_query = query_result["query"]
        collection_name = query_result.get("collection", primary_collection)
        
        # APPLY SECURITY FILTERING HERE (NEW)
        company_id = user_context.get('company_id')
        if company_id:
            mongo_query = self.enforce_tenant_filter(
                mongo_query, collection_name, company_id, user_id
            )
        
        # Execute query
        results = self.execute_query(collection_name, mongo_query, limit=limit)
        
        # Fallback for case sensitivity
        if complexity == "single_collection" and "error" not in results and results.get("count", 0) == 0:
            ci_query = self._convert_to_case_insensitive(mongo_query)
            results = self.execute_query(collection_name, ci_query, limit=limit)
            mongo_query = ci_query
        
        # Build response
        response = {
            "status": "success" if "error" not in results else "error",
            "collection": collection_name,
            "additional_collections": additional_collections,
            "query_complexity": complexity,
            "generated_query": self._serialize_for_json(mongo_query),
            "user_context": {
                "user_id": user_context.get('user_id'),
                "user_name": user_context.get('user_name'),
                "company_id": str(user_context.get('company_id')) if user_context.get('company_id') else None,
                "company_name": user_context.get('company_name'),
                "roles": user_context.get('roles', []),
                "cached_at": user_context.get('cached_at')
            },
            "processing_time": time.time() - start_time
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
                response["explanation"] = self._generate_results_explanation(
                    natural_language_query, mongo_query, results, collection_name
                )
        
        return response
    
    
    def _clean_json_response(self, response_text: str) -> str:
    
    # Remove markdown code blocks
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()
        
        # Remove JavaScript-style comments
        response_text = self._remove_js_comments(response_text)
        
        # Extract JSON object
        if '{' in response_text:
            response_text = response_text[response_text.find('{'):]
        if '}' in response_text:
            response_text = response_text[:response_text.rfind('}') + 1]
        
        return response_text
    
    
    def _remove_js_comments(self, json_text: str) -> str:
   
        lines = json_text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Remove single-line comments (// ...)
            if '//' in line:
                # Find the position of //
                comment_pos = line.find('//')
                # Check if // is inside a string (basic check)
                before_comment = line[:comment_pos]
                quote_count = before_comment.count('"') - before_comment.count('\\"')
                
                # If even number of quotes, // is outside a string
                if quote_count % 2 == 0:
                    line = line[:comment_pos].rstrip()
            
            # Skip empty lines after comment removal
            if line.strip():
                cleaned_lines.append(line)
    
        return '\n'.join(cleaned_lines)
    
    
    
    def _fix_json_issues(self, json_text: str) -> str:
    
        try:
            # Remove JavaScript-style comments first
            json_text = self._remove_js_comments(json_text)
            
            # Extract JSON part
            if '{' in json_text:
                json_text = json_text[json_text.find('{'):]
            if '}' in json_text:
                json_text = json_text[:json_text.rfind('}') + 1]
            
            # Fix trailing commas (simple regex approach)
            import re
            # Remove trailing commas before } or ]
            json_text = re.sub(r',(\s*[}\]])', r'\1', json_text)
            
            # Replace single quotes with double quotes (be careful with nested quotes)
            # This is a simple approach - for production, use a proper JSON fixer
            json_text = json_text.replace("'", '"')
            
            return json_text
            
        except Exception as e:
            print(f"Error fixing JSON: {e}")
            return json_text


# Usage example
def main():
    """Example usage of the enhanced system with multi-collection support"""
    system = EnhancedNLToMongoDBQuerySystem()
    
    user_id = "14f57657-7362-46c5-af7a-e96d56193786"
    

    # Test multi-collection query
    result2 = system.process_query("What are the details of the follow-ups assigned to me.", user_id)
    print(f"Multi-collection query processing time: {result2.get('processing_time', 0):.3f}s")
    print("Results:", json.dumps(result2, indent=2))
    

if __name__ == "__main__":
    main()
