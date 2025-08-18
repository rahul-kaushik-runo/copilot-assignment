import json
import google.generativeai as genai
from pymongo import MongoClient
from bson import ObjectId
import time
from datetime import datetime, timedelta
import re
from typing import Dict, List, Optional, Any
import threading
from functools import lru_cache
from difflib import get_close_matches
import pytz
import os

# Load schema from file
with open('schema.txt', 'r') as file:
    SCHEMAS_STR = file.read()

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


class OptimizedNLToMongoDBQuerySystem:
    """Optimized version with user context caching"""
    
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
        
        # Initialize user context cache
        self.user_cache = UserContextCache(self.db, cache_ttl=300)  # 5 min cache
        
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
            "customer": "companyId",
            "customer-details": "companyId",
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
            "report-history": "companyId",
            "report-agent-login": "companyId",
            "report-agent-disposition": "companyId",
            "crm-field": "companyId",
            "rechurn-log": "companyId",
            "rechurn-status": "companyId",
            "customer-assign-log": "companyId",
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
        now = datetime.now()
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
            response = self.model.generate_content(prompt)
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
    
    def apply_security_filters(self, query: Any, collection_name: str, user_context: Dict[str, Any]) -> Any:
        """Apply security filters using precomputed user context"""
        if "error" in user_context:
            return query
        
        company_id = user_context.get('company_id')
        if not company_id:
            return query
        
        # Special handling for company collection
        if collection_name == "company":
            if self._is_my_company_query(query):
                return self._convert_to_company_id_filter(query, company_id)
        
        # Apply company filtering for collections that need it
        if collection_name in self.company_filtered_collections:
            company_field = self.company_filtered_collections[collection_name]
            return self._add_company_filter_to_query(query, company_field, company_id)
        
        return query
    
    def _is_my_company_query(self, query: Any) -> bool:
        """Detect if this is a 'my company' query"""
        if isinstance(query, dict):
            filter_fields = [k for k in query.keys() if not k.startswith('$')]
            return len(filter_fields) == 0 or (len(filter_fields) == 1 and 'name' in query)
        elif isinstance(query, list):
            for stage in query:
                if '$match' in stage:
                    match_fields = [k for k in stage['$match'].keys() if not k.startswith('$')]
                    return len(match_fields) == 0
            return True
        return True
    
    def _convert_to_company_id_filter(self, query: Any, company_id: ObjectId) -> Any:
        """Convert query to filter by company _id"""
        if isinstance(query, dict):
            return {"_id": company_id}
        elif isinstance(query, list):
            new_pipeline = []
            match_added = False
            for stage in query:
                if '$match' in stage and not match_added:
                    stage['$match'] = {"_id": company_id}
                    match_added = True
                new_pipeline.append(stage)
            
            if not match_added:
                new_pipeline.insert(0, {"$match": {"_id": company_id}})
            
            return new_pipeline
        return query
    
    def _add_company_filter_to_query(self, query: Any, company_field: str, company_id: ObjectId) -> Any:
    
        if isinstance(query, dict):
            query = query.copy()
            
            # Special handling for company collection
            if company_field == "_id":
                query["_id"] = company_id
            else:
                # Handle dot notation for nested fields
                parts = company_field.split('.')
                current = query
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = company_id
                
        elif isinstance(query, list):
            query = query.copy()
            
            match_stage_found = False
            for stage in query:
                if '$match' in stage:
                    match_stage_found = True
                    if company_field == "_id":
                        stage['$match']["_id"] = company_id
                    else:
                        # Handle dot notation for nested fields
                        parts = company_field.split('.')
                        current = stage['$match']
                        for part in parts[:-1]:
                            if part not in current:
                                current[part] = {}
                            current = current[part]
                        current[parts[-1]] = company_id
                    break
            
            if not match_stage_found:
                if company_field == "_id":
                    query.insert(0, {"$match": {"_id": company_id}})
                else:
                    # Handle dot notation for nested fields
                    parts = company_field.split('.')
                    filter_obj = {}
                    current = filter_obj
                    for part in parts[:-1]:
                        current[part] = {}
                        current = current[part]
                    current[parts[-1]] = company_id
                    query.insert(0, {"$match": filter_obj})
        
        return query
    
    def normalize_query(self, query):
        """Normalize natural language query using value synonyms"""
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

    def _select_best_collection(self, query_text: str, accessible_collections: List[str]) -> Optional[str]:
        """Phase 1: Have LLM select the best collection based on summaries (filtered by user access)"""
        if not accessible_collections:
            return None
        
        normalized_query = self.normalize_query(query_text)
        
        # Filter summaries to only include accessible collections
        accessible_summaries = {
            name: summary for name, summary in self.schema_summaries.items() 
            if name in accessible_collections
        }
        
        prompt = f"""
You are a MongoDB expert assistant. Given the following collection summaries, 
select the SINGLE most appropriate collection for this query:

USER QUERY: "{normalized_query}"

AVAILABLE COLLECTIONS (filtered by user permissions):
{self._format_accessible_collection_summaries(accessible_summaries)}

INSTRUCTIONS:
1. Analyze the user's query intent
2. Compare with each collection's purpose
3. Select ONLY ONE collection name that best matches
4. Respond ONLY with the collection name in this format: "collection: <name>"

EXAMPLE RESPONSES:
- "collection: call-interaction"
- "collection: user"
"""
        response = self.model.generate_content(prompt)
        try:
            # Extract collection name from response
            if "collection:" in response.text.lower():
                selected_collection = response.text.split("collection:")[1].strip().split()[0].strip('"\'')
                # Verify the selected collection is in accessible list
                if selected_collection in accessible_collections:
                    return selected_collection
            return None
        except Exception as e:
            print(f"Error parsing collection selection: {str(e)}")
            return None
            
    def _format_accessible_collection_summaries(self, accessible_summaries):
        """Format accessible collection summaries for selection prompt"""
        return "\n".join(
            f"- {name}: {summary}" 
            for name, summary in accessible_summaries.items()
        )
    
    def _generate_query_for_collection(self, query_text: str, collection_name: str, 
                                     user_context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate MongoDB query for collection"""
        normalized_query = self.normalize_query(query_text)
        schema = self.full_schemas.get(collection_name)
        if not schema:
            return {"error": f"Collection {collection_name} not found"}
            
        # Parse time expressions using hybrid approach
        time_context = self._parse_time_with_llm_fallback(query_text)
        
        # Prepare value synonyms string
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])
        
        company_filter_note = ""
        company_id = user_context.get('company_id')
        if company_id:
            # Convert ObjectId to string for JSON compatibility in the prompt
            company_id_str = str(company_id)
            company_filter_note = f"""
    CRITICAL REQUIREMENT - COMPANY FILTERING:
    - This user can only access data from their company
    - ALWAYS include companyId filter in your query: {{"companyId": "COMPANY_ID_PLACEHOLDER"}}
    - For aggregation pipelines, add {{"$match": {{"companyId": "COMPANY_ID_PLACEHOLDER"}}}} as the first stage
    - depending on the collection, you may need to use company._id or companyId
    - For simple find queries, include "companyId": "COMPANY_ID_PLACEHOLDER" in the filter
    - NOTE: The COMPANY_ID_PLACEHOLDER will be automatically converted to ObjectId during execution
    """
        
        time_filter_note = ""
        if time_context:
            time_fields_for_collection = self.time_fields.get(collection_name, [])
            if time_fields_for_collection:
                primary_time_field = time_fields_for_collection[0]  # Use the first time field as primary
                time_filter_note = f"""
    TIME FILTERING DETECTED:
    - User asked about "{time_context['phrase']}"
    - Time range: {time_context['start_readable']} to {time_context['end_readable']}
    - Unix timestamps: {time_context['start_unix']} to {time_context['end_unix']}
    - For this collection, use time field: "{primary_time_field}"
    - Add time filter: {{"{primary_time_field}": {{"$gte": {time_context['start_unix']}, "$lte": {time_context['end_unix']}}}}}
    """
            
        prompt = f"""
    You are a MongoDB query expert. Generate a valid JSON response for this natural language query.

    USER QUERY: "{normalized_query}"

    COLLECTION SCHEMA:
    {schema}

    {company_filter_note}

    {time_filter_note}

    VALUE SYNONYMS TO CONSIDER (use canonical values in query):
    {value_synonyms_str}

    CRITICAL INSTRUCTIONS:
    1. Respond with ONLY valid JSON in this exact format:
    {{"collection": "{collection_name}", "query": <mongo_query>}}

    2. The <mongo_query> must be valid MongoDB query syntax
    3. For text matching, use case-insensitive regex: {{"$regex": "pattern", "$options": "i"}}
    4. For counting, use aggregation pipeline: [{{"$match": {{"field": "value"}}}}, {{"$group": {{"_id": null, "count": {{"$sum": 1}}}}}}]
    5. ALWAYS include companyId filter as a string (it will be converted to ObjectId later)
    6. Use only standard JSON types (string, number, boolean, array, object)
    7. NO MongoDB-specific types like ObjectId() in the JSON response
    8. Use projection based on what fields the user needs, don't return everything

    VALID EXAMPLE RESPONSES:
    {{"collection": "call-interaction", "query": {{"details.type": "inbound", "companyId": "COMPANY_ID_PLACEHOLDER"}}}}
    {{"collection": "user", "query": [{{"$match": {{"companyId": "COMPANY_ID_PLACEHOLDER"}}}}, {{"$group": {{"_id": null, "count": {{"$sum": 1}}}}}}]}}

    Respond with ONLY the JSON object, no additional text or formatting:
    """
        
        try:
            response = self.model.generate_content(prompt)
            response_text = response.text.strip()
            
            print(f"LLM Raw Response: {response_text}")  # Debug print
            
            # Clean up response - remove any markdown formatting
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            
            # Remove any leading/trailing whitespace and quotes
            response_text = response_text.strip().strip('"\'')
            
            # Additional cleanup - remove any text before the first {
            if '{' in response_text:
                response_text = response_text[response_text.find('{'):]
            
            # Remove any text after the last }
            if '}' in response_text:
                response_text = response_text[:response_text.rfind('}') + 1]
            
            print(f"Cleaned Response: {response_text}")  # Debug print
            
            # Parse JSON
            parsed_result = json.loads(response_text)
            
            # Convert companyId string to ObjectId for actual query execution
            if company_id:
                # Replace any COMPANY_ID_PLACEHOLDER with the actual company_id
                parsed_result["query"] = self._replace_company_id_placeholder(parsed_result["query"], company_id)
            
            return parsed_result
            
        except json.JSONDecodeError as e:
            return {
                "error": f"Failed to parse query as JSON: {str(e)}", 
                "raw_response": response.text,
                "cleaned_response": response_text if 'response_text' in locals() else "N/A",
                "json_error_position": f"line {e.lineno}, column {e.colno}" if hasattr(e, 'lineno') else "unknown"
            }
        except Exception as e:
            return {"error": f"Failed to generate query: {str(e)}", "raw_response": response.text}

    def _replace_company_id_placeholder(self, query, company_id):
        if isinstance(query, dict):
            new_query = {}
            for key, value in query.items():
                if key == "companyId" and value == "COMPANY_ID_PLACEHOLDER":
                    new_query[key] = company_id
                elif isinstance(value, dict):
                    new_query[key] = self._replace_company_id_placeholder(value, company_id)
                elif isinstance(value, list):
                    new_query[key] = [self._replace_company_id_placeholder(item, company_id) if isinstance(item, dict) else item for item in value]
                else:
                    new_query[key] = value
            return new_query
        elif isinstance(query, list):
            return [self._replace_company_id_placeholder(item, company_id) if isinstance(item, dict) else item for item in query]
        else:
            return query
            
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
                elif isinstance(value, str) and key != "companyId":
                    # Convert to case-insensitive regex match (but not for companyId)
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
        """
        Execute the generated MongoDB query with result limit (default 50).
        This prevents memory issues and excessive data transfer.
        """
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            results = None

            if operation_type == "aggregate":
                pipeline = query if isinstance(query, list) else query.get("aggregate", [])
                processed_pipeline = []
                for stage in pipeline:
                    if "$match" in stage:
                        stage["$match"] = self._convert_to_case_insensitive(stage["$match"])
                    processed_pipeline.append(stage)
                results = list(collection.aggregate(processed_pipeline, allowDiskUse=True))[:limit]
            elif operation_type == "find":
                filter_query = query.get("find", query)
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
        result_count = results.get('count', 0)
        sample_results = results.get('results', [])[:3]
        query_type = results.get('query_type', 'find')

        prompt = f"""
You are a helpful MongoDB assistant explaining query results in simple terms.

INSTRUCTIONS:
1. Start by rephrasing the user's question to show understanding
2. Summarize the key findings from the results
3. Highlight any important numbers or patterns
4. Keep it concise (1-2 short paragraphs max)
5. Use natural, conversational language
6. If no results found, suggest possible reasons
7. Focus on the business meaning, not technical details

USER ORIGINAL QUESTION: "{nl_query}"

DATABASE COLLECTION: {collection_name}
QUERY TYPE: {query_type}
NUMBER OF RESULTS: {result_count}

SAMPLE RESULTS (first 3):
{json.dumps(sample_results, indent=2, default=str)}
"""
        response = self.model.generate_content(prompt)
        return response.text

    def process_query(self, natural_language_query: str, user_id: str, 
                     include_explanation: bool = True, limit: int = 50) -> Dict[str, Any]:
        """
        Process natural language query with optimized user context retrieval
        """
        start_time = time.time()
        
        # Get user context from cache (fast!)
        user_context = self.get_user_context(user_id)
        if "error" in user_context:
            return {
                "status": "error",
                "message": user_context["error"],
                "security_error": True,
                "processing_time": time.time() - start_time
            }
        
        # Check if user has access to any collections
        accessible_collections = user_context.get('accessible_collections', [])
        if not accessible_collections:
            return {
                "status": "error",
                "message": "User has no access to any collections",
                "security_error": True,
                "processing_time": time.time() - start_time
            }
        
        # Phase 1: Collection selection
        collection_name = self._select_best_collection(natural_language_query, accessible_collections)
        if not collection_name:
            return {
                "status": "error",
                "message": "Could not determine appropriate collection",
                "processing_time": time.time() - start_time
            }
        
        # Phase 2: Query generation
        query_result = self._generate_query_for_collection(
            natural_language_query, 
            collection_name, 
            user_context
        )
        if "error" in query_result:
            return {
                "status": "error",
                "message": query_result["error"],
                "raw_response": query_result.get("raw_response", ""),
                "processing_time": time.time() - start_time
            }
        
        mongo_query = query_result["query"]
        
        # Phase 3: Apply security filters using precomputed context
        mongo_query = self.apply_security_filters(mongo_query, collection_name, user_context)
        
        # Phase 4: Execute query
        results = self.execute_query(collection_name, mongo_query, limit=limit)
        
        # Fallback logic for case sensitivity
        if "error" not in results and results.get("count", 0) == 0:
            ci_query = self._convert_to_case_insensitive(mongo_query)
            results = self.execute_query(collection_name, ci_query, limit=limit)
            mongo_query = ci_query
        
        # Build optimized response
        response = {
            "status": "success" if "error" not in results else "error",
            "collection": collection_name,
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
                    natural_language_query,
                    mongo_query,
                    results,
                    collection_name
                )
        
        return response


# Usage example
def main():
    """Example usage of the optimized system"""
    system = OptimizedNLToMongoDBQuerySystem()
    
    user_id = "14f57657-7362-46c5-af7a-e96d56193786"
    
    # First query - will cache user context
    result1 = system.process_query("Show me my company details", user_id)
    print(f"First query processing time: {result1.get('processing_time', 0):.3f}s")
    print("Results:", json.dumps(result1, indent=2))
    
    # Second query - will use cached context (faster!)
    result2 = system.process_query("Get all my processes", user_id)
    print(f"Second query processing time: {result2.get('processing_time', 0):.3f}s")
    print("Results:", json.dumps(result2, indent=2))
    
    # Invalidate cache when user data changes
    # system.invalidate_user_cache(user_id)

if __name__ == "__main__":
    main()