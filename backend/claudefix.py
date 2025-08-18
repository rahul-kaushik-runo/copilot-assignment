import json
import re
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
        API_KEY = "AIzaSyDeYOD3QjbczcU20MHw0GmVzkS_0-BmwxI"
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

        # Define lookup relationships between collections (now bidirectional)
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
        },
        "callInteractions": {
            "from": "call-interaction",
            "localField": "_id",
            "foreignField": "user._id",
            "as": "callInteractions"
        },
        "emailInteractions": {
            "from": "email-interaction",
            "localField": "_id",
            "foreignField": "sender._id",
            "as": "emailInteractions"
        },
        "whatsappInteractions": {
            "from": "whatsapp-interaction",
            "localField": "_id",
            "foreignField": "user._id",
            "as": "whatsappInteractions"
        },
        "smsInteractions": {
            "from": "sms-interaction",
            "localField": "_id",
            "foreignField": "user._id",
            "as": "smsInteractions"
        },
        "timeLogs": {
            "from": "time-log",
            "localField": "_id",
            "foreignField": "user._id",
            "as": "timeLogs"
        },
        "allocations": {
            "from": "allocation",
            "localField": "_id",
            "foreignField": "assigned.toId",
            "as": "allocations"
        },
        "transactions": {
            "from": "transaction",
            "localField": "_id",
            "foreignField": "userId",
            "as": "transactions"
        },
        "recurringInteractions": {
            "from": "recurring-interaction",
            "localField": "_id",
            "foreignField": "user._id",
            "as": "recurringInteractions"
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
        },
        "callInteractions": {
            "from": "call-interaction",
            "localField": "_id",
            "foreignField": "customer._id",
            "as": "callInteractions"
        },
        "emailInteractions": {
            "from": "email-interaction",
            "localField": "_id",
            "foreignField": "customer._id",
            "as": "emailInteractions"
        },
        "whatsappInteractions": {
            "from": "whatsapp-interaction",
            "localField": "_id",
            "foreignField": "customer._id",
            "as": "whatsappInteractions"
        },
        "smsInteractions": {
            "from": "sms-interaction",
            "localField": "_id",
            "foreignField": "customer._id",
            "as": "smsInteractions"
        },
        "crmInteractions": {
            "from": "crm-interaction",
            "localField": "_id",
            "foreignField": "customer._id",
            "as": "crmInteractions"
        },
        "recurringInteractions": {
            "from": "recurring-interaction",
            "localField": "_id",
            "foreignField": "customer._id",
            "as": "recurringInteractions"
        },
        "assignLogs": {
            "from": "customer-assign-log",
            "localField": "_id",
            "foreignField": "cusId",
            "as": "assignLogs"
        },
        "rechurnLogs": {
            "from": "rechurn-log",
            "localField": "_id",
            "foreignField": "cusId",
            "as": "rechurnLogs"
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
        },
        "process": {
            "from": "process",
            "localField": "user.process._id",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    # Company-related lookups
    "company": {
        "users": {
            "from": "user",
            "localField": "_id",
            "foreignField": "company._id",
            "as": "users"
        },
        "customers": {
            "from": "customer",
            "localField": "_id",
            "foreignField": "cId",
            "as": "customers"
        },
        "licenses": {
            "from": "license",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "licenses"
        },
        "apiKeys": {
            "from": "api-key",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "apiKeys"
        },
        "processes": {
            "from": "process",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "processes"
        },
        "callInteractions": {
            "from": "call-interaction",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "callInteractions"
        },
        "emailInteractions": {
            "from": "email-interaction",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "emailInteractions"
        },
        "allocations": {
            "from": "allocation",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "allocations"
        },
        "transactions": {
            "from": "transaction",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "transactions"
        },
        "virtualNumbers": {
            "from": "cloud-virtual-number",
            "localField": "_id",
            "foreignField": "companyId",
            "as": "virtualNumbers"
        }
    },
    
    # Process-related lookups
    "process": {
        "company": {
            "from": "company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "users": {
            "from": "user",
            "localField": "_id",
            "foreignField": "process._id",
            "as": "processUsers"
        },
        "allocations": {
            "from": "allocation",
            "localField": "_id",
            "foreignField": "assigned.processId",
            "as": "processAllocations"
        },
        "callInteractions": {
            "from": "call-interaction",
            "localField": "_id",
            "foreignField": "user.process._id",
            "as": "processCallInteractions"
        },
        "emailTemplates": {
            "from": "email-template",
            "localField": "_id",
            "foreignField": "processId",
            "as": "emailTemplates"
        }
    },
    
    # License-related lookups
    "license": {
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
        "transaction": {
            "from": "transaction",
            "localField": "_id",
            "foreignField": "licenseIds",
            "as": "licenseTransaction"
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
        "customer": {
            "from": "customer",
            "localField": "customer._id",
            "foreignField": "_id",
            "as": "customerDetails"
        },
        "assignedUser": {
            "from": "user",
            "localField": "assigned.toId",
            "foreignField": "_id",
            "as": "assignedUserDetails"
        },
        "assigningUser": {
            "from": "user",
            "localField": "assigned.fromId",
            "foreignField": "_id",
            "as": "assigningUserDetails"
        },
        "process": {
            "from": "process",
            "localField": "assigned.processId",
            "foreignField": "_id",
            "as": "processDetails"
        },
        "customerDetails": {
            "from": "customer-details",
            "localField": "customer._id",
            "foreignField": "cusId",
            "as": "extendedCustomerDetails"
        }
    },
    
    # Transaction-related lookups
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
            "as": "transactionLicenses"
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
        },
        "process": {
            "from": "process",
            "localField": "sender.processId",
            "foreignField": "_id",
            "as": "processDetails"
        },
        "template": {
            "from": "email-template",
            "localField": "templateId",
            "foreignField": "_id",
            "as": "emailTemplate"
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
        "assignedUser": {
            "from": "user",
            "localField": "assigned.toId",
            "foreignField": "_id",
            "as": "assignedUserDetails"
        },
        "process": {
            "from": "process",
            "localField": "assigned.processId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    # Rechurn log lookups
    "rechurn-log": {
        "company": {
            "from": "company",
            "localField": "cId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "customer": {
            "from": "customer",
            "localField": "cusId",
            "foreignField": "_id",
            "as": "customerDetails"
        },
        "rechurnedBy": {
            "from": "user",
            "localField": "rBy",
            "foreignField": "_id",
            "as": "rechurnedByDetails"
        },
        "rechurnedTo": {
            "from": "user",
            "localField": "rTo",
            "foreignField": "_id",
            "as": "rechurnedToDetails"
        },
        "rechurnStatus": {
            "from": "rechurn-status",
            "localField": "rSId",
            "foreignField": "_id",
            "as": "rechurnStatusDetails"
        },
        "process": {
            "from": "process",
            "localField": "pId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    # Add all remaining collections with their relationships
    "email-template": {
        "company": {
            "from": "company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "creator": {
            "from": "user",
            "localField": "userId",
            "foreignField": "_id",
            "as": "creatorDetails"
        },
        "process": {
            "from": "process",
            "localField": "processId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    "customer-details": {
        "customer": {
            "from": "customer",
            "localField": "cusId",
            "foreignField": "_id",
            "as": "customerCoreDetails"
        },
        "company": {
            "from": "company",
            "localField": "cId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "process": {
            "from": "process",
            "localField": "pId",
            "foreignField": "_id",
            "as": "processDetails"
        },
        "assignedUser": {
            "from": "user",
            "localField": "asgn.toId",
            "foreignField": "_id",
            "as": "assignedUserDetails"
        }
    },
    
    # Include all other collections similarly
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
        },
        "process": {
            "from": "process",
            "localField": "user.processId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
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
    
    "recurring-interaction": {
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
    
    # Include all report collections
    "report-history": {
        "company": {
            "from": "company",
            "localField": "cId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "user": {
            "from": "user",
            "localField": "userId",
            "foreignField": "_id",
            "as": "userDetails"
        },
        "process": {
            "from": "process",
            "localField": "pId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    # Add remaining collections with their relationships
    "roles": {
        "company": {
            "from": "company",
            "localField": "cId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "users": {
            "from": "user",
            "localField": "_id",
            "foreignField": "role._id",
            "as": "roleUsers"
        }
    },
    
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
    },
    
    "customer-assign-log": {
        "company": {
            "from": "company",
            "localField": "cId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "customer": {
            "from": "customer",
            "localField": "cusId",
            "foreignField": "_id",
            "as": "customerDetails"
        },
        "assignedTo": {
            "from": "user",
            "localField": "asgn.tId",
            "foreignField": "_id",
            "as": "assignedToDetails"
        },
        "assignedFrom": {
            "from": "user",
            "localField": "asgn.fId",
            "foreignField": "_id",
            "as": "assignedFromDetails"
        },
        "process": {
            "from": "process",
            "localField": "pId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    # Include all remaining collections
    "api-key": {
        "company": {
            "from": "company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "creator": {
            "from": "user",
            "localField": "creator._id",
            "foreignField": "_id",
            "as": "creatorDetails"
        }
    },
    
    "cloud-virtual-number": {
        "company": {
            "from": "company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "companyDetails"
        }
    },
    
    "crm-field": {
        "company": {
            "from": "company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "process": {
            "from": "process",
            "localField": "processId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    "rechurn-status": {
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
        "allocatedUsers": {
            "from": "user",
            "localField": "allocatedTo.id",
            "foreignField": "_id",
            "as": "allocatedUserDetails"
        }
    },
    
    "sms-template": {
        "company": {
            "from": "company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "creator": {
            "from": "user",
            "localField": "userId",
            "foreignField": "_id",
            "as": "creatorDetails"
        },
        "process": {
            "from": "process",
            "localField": "processId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    "whatsapp-template": {
        "company": {
            "from": "company",
            "localField": "companyId",
            "foreignField": "_id",
            "as": "companyDetails"
        },
        "creator": {
            "from": "user",
            "localField": "userId",
            "foreignField": "_id",
            "as": "creatorDetails"
        },
        "process": {
            "from": "process",
            "localField": "processId",
            "foreignField": "_id",
            "as": "processDetails"
        }
    },
    
    # Report collections
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
    
    
    
    def _get_relevant_relationships(self, collection_name, query_text):
        """
        Extract only the relationships relevant to the current query
        Returns a natural language description of available joins
        """
        available_joins = self.lookup_relationships.get(collection_name, {})
        if not available_joins:
            return ""
        
        # Detect which relationships might be needed
        normalized_query = query_text.lower()
        relevant_joins = []
        
        for join_name, config in available_joins.items():
            # Simple keyword matching - could be enhanced
            target_collection = config["from"]
            if target_collection in normalized_query:
                relevant_joins.append((join_name, config))
            elif join_name in normalized_query:
                relevant_joins.append((join_name, config))
        
        # If no specific joins mentioned, return all potential joins
        if not relevant_joins and " with " in normalized_query:
            relevant_joins = list(available_joins.items())
        
        # Format for LLM consumption
        if not relevant_joins:
            return ""
            
        relationship_text = f"AVAILABLE RELATIONSHIPS FOR '{collection_name}':\n"
        for join_name, config in relevant_joins:
            relationship_text += (
                f"- Can join with '{config['from']}' collection "
                f"(using {config['localField']} → {config['foreignField']}) "
                f"as '{config['as']}'\n"
            )
        
        return relationship_text

    def _identify_lookup_requirements(self, query_text):
        """Identify if the query requires lookups and determine primary collection"""
        normalized_query = query_text.lower()
        
        # Enhanced lookup indicators with primary collection hints
        lookup_indicators = {
            "user": {
                "keywords": ["user", "agent", "employee"],
                "relationships": {
                    "callInteractions": ["call interactions", "phone calls", "call history"],
                    "emailInteractions": ["email communications", "emails sent"],
                    "allocations": ["customer allocations", "assigned customers"]
                }
            },
            "call-interaction": {
                "keywords": ["call", "phone call", "call log"],
                "relationships": {
                    "user": ["user who made", "agent who called"],
                    "customer": ["customer called", "client contacted"]
                }
            },
            "customer": {
                "keywords": ["customer", "client"],
                "relationships": {
                    "callInteractions": ["call history", "phone interactions"],
                    "allocations": ["allocations", "assignments"]
                }
            }
        }
        
        # Determine primary collection and required lookups
        primary_collection = None
        required_lookups = []
        
        # First pass - find primary collection
        for collection, indicators in lookup_indicators.items():
            if any(keyword in normalized_query for keyword in indicators["keywords"]):
                primary_collection = collection
                break
        
        # Second pass - find required lookups
        if primary_collection:
            for lookup_name, phrases in lookup_indicators[primary_collection]["relationships"].items():
                if any(phrase in normalized_query for phrase in phrases):
                    required_lookups.append(lookup_name)
        
        # Special case for "with" queries (e.g., "users with their call interactions")
        if "with" in normalized_query or "along with" in normalized_query:
            parts = re.split(r"with|along with", normalized_query)
            if len(parts) > 1:
                main_entity = parts[0].strip()
                related_entity = parts[1].strip()
                
                # Simple heuristic - if main entity is plural, it's likely primary
                if main_entity.endswith('s'):
                    primary_collection = main_entity.rstrip('s').replace(" ", "-")
                    required_lookups = [related_entity.replace(" ", "")]
        
        return primary_collection, required_lookups

    def _add_lookup_stages(self, collection_name, query, required_lookups):
        """Add lookup stages to aggregation pipeline with improved relationship handling"""
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
                if lookup_type in ["user", "company", "customer", "process", "role", "assignedUser"]:
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
        """Phase 1: Select best collection with improved relationship awareness"""
        # First try to identify if this is a relationship query
        primary_collection, required_lookups = self._identify_lookup_requirements(query_text)
        
        if primary_collection and primary_collection in self.schema_summaries:
            return primary_collection
        
        # Fall back to original LLM-based selection if no clear relationship
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
4. If the query mentions multiple entities with "with" or "and" (e.g., "users with their call interactions"),
   select the collection that represents the MAIN entity (before "with").
5. Respond ONLY with the collection name in this format: "collection: <name>"

EXAMPLE RESPONSES:
- "collection: report-agent-disposition"
- "collection: user"
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
        """Enhanced version that only passes relevant relationships"""
        normalized_query = self.normalize_query(query_text)
        schema = self.full_schemas.get(collection_name)
        if not schema:
            return {"error": f"Collection {collection_name} not found"}

        # Get only relevant relationships
        relationship_info = self._get_relevant_relationships(collection_name, query_text)
        
        # Prepare value synonyms string
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" 
                                    for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])

        # Add company context if provided
        company_context = ""
        if company_id:
            company_field = self.company_field_mapping.get(collection_name, "cId")
            company_context = f"""
IMPORTANT SECURITY CONTEXT:
- User belongs to company: {company_id}
- Company field in this collection: {company_field}
- DO NOT include company filtering in your query
- The system will automatically add company filters
"""

        prompt = f"""
You are a MongoDB query expert. Given this collection schema, generate a query for:

USER QUERY: "{normalized_query}"

COLLECTION SCHEMA:
{schema}

{company_context}

{relationship_info}

VALUE SYNONYMS TO CONSIDER (use canonical values in query):
{value_synonyms_str}

Respond ONLY with a JSON object: {{
  "collection": "{collection_name}",
  "query": <mongo_query>, 
  "needed_joins": [<array_of_join_names>]  // Names from AVAILABLE RELATIONSHIPS
}}

RULES:
1. For relationship queries, use aggregation pipeline
2. Specify needed joins in 'needed_joins' array
3. Use proper field names from the schema
4. Never include company filtering
5. Keep queries efficient
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
            
            # Add actual lookup stages based on needed_joins
            if result.get("needed_joins"):
                result["query"] = self._add_lookup_stages(
                    collection_name, 
                    result["query"], 
                    result["needed_joins"]
                )
                result["has_lookups"] = True
            
            return result
        except Exception as e:
            return {"error": f"Failed to parse query: {str(e)}", "raw_response": response.text}
        
        
        
        
        
        
    def _add_lookup_stages(self, collection_name, query, needed_joins):
        """
        Enhanced version that properly handles different query types
        and adds lookups in the correct pipeline position
        """
        if not isinstance(query, list):
            # Convert find query to aggregation pipeline
            query = [{"$match": query}] if query else [{"$match": {}}]
        
        # Get available lookups for this collection
        available_lookups = self.lookup_relationships.get(collection_name, {})
        
        # Build lookup stages
        lookup_stages = []
        for join_name in needed_joins:
            if join_name in available_lookups:
                lookup_config = available_lookups[join_name]
                lookup_stage = {
                    "$lookup": {
                        "from": lookup_config["from"],
                        "localField": lookup_config["localField"],
                        "foreignField": lookup_config["foreignField"],
                        "as": lookup_config["as"]
                    }
                }
                lookup_stages.append(lookup_stage)
                
                # Add unwind for single-document relationships
                if join_name in ["user", "company", "customer", "process", "role"]:
                    lookup_stages.append({
                        "$unwind": {
                            "path": f"${lookup_config['as']}",
                            "preserveNullAndEmptyArrays": True
                        }
                    })
        
        # Insert lookups after initial match but before other stages
        if lookup_stages:
            if len(query) > 0 and "$match" in query[0]:
                # Has existing match stage - insert after it
                modified_pipeline = [query[0]] + lookup_stages + query[1:]
            else:
                # No match stage - prepend empty match
                modified_pipeline = [{"$match": {}}] + lookup_stages + query
            
            return modified_pipeline
        
        return query

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

    def process_query(self, natural_language_query, company_id=None, include_explanation=True):
        """Complete end-to-end query processing with company-based access control"""
        # Validate company_id is provided for security
        if not company_id:
            return {
                "status": "error",
                "message": "Company ID is required for data access.",
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

    # ... [keep all other existing utility methods]

# Example usage
def main():
    nl_system = NLToMongoDBQuerySystem()
    company_id = ObjectId("67c6da5aa4171809121d2990")  # Example ObjectId

    # Test relationship query
    query = "Find users alon with their call interactions"
    print(f"\nTesting Query: '{query}'")
    
    result = nl_system.process_query(query, company_id=company_id)
    
    if result['status'] == 'success':
        print(f"Collection: {result['collection']}")
        print(f"Results Count: {result['count']}")
        print("Sample Result:")
        print(result)
        print("\nGenerated Query:")
        print(result['generated_query'])
        
        print("\nExplanation:")
        print(result['explanation'])
    else:
        print("Error:", result['message'])

if __name__ == "__main__":
    main()