import json
import google.generativeai as genai
from pymongo import MongoClient
import time
from difflib import get_close_matches

class NLToMongoDBQuerySystem:
    def __init__(self):
        API_KEY = "AIzaSyCYsPGc7kXhDteLjmkn2sXrk15q7nevHeY"
        MONGODB_URI = "mongodb://localhost:27017"
        DB_NAME = "runo"

        genai.configure(api_key=API_KEY)
        self.model = genai.GenerativeModel('gemini-2.5-flash')

        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[DB_NAME]
        self.query_history = []

        # self.collection_schemas = self._sample_all_collection_schemas()

        # 🔧 Value synonyms (manually maintained or learned)
        self.value_synonyms = {
            "state": {
                "TG": ["telangana", "telengana", "tg", "t'gana"],
                "MH": ["maharashtra", "mh"],
                "KA": ["karnataka", "ka"]
            }
        }

    # def _sample_all_collection_schemas(self, sample_limit=1000):
    #     schemas = {}
    #     for collection_name in self.db.list_collection_names():
    #         collection = self.db[collection_name]
    #         all_fields = set()
    #         max_fields_count = 0
    #         sample_doc = None
    #         for doc in collection.find().limit(sample_limit):
    #             doc_fields = set(doc.keys())
    #             all_fields.update(doc_fields)
    #             if len(doc_fields) > max_fields_count:
    #                 max_fields_count = len(doc_fields)
    #                 sample_doc = doc
    #         schemas[collection_name] = {
    #             "fields": list(all_fields),
    #             "sample": sample_doc if sample_doc else {}
    #         }
            
            
    #         print(schemas[collection_name])
    #     return schemas

    # 🔧 Normalize natural language query using value synonyms
    def normalize_query(self, query):
        normalized = query.lower()
        for field, synonym_map in self.value_synonyms.items():
            for canonical, variants in synonym_map.items():
                for variant in variants:
                    if variant.lower() in normalized:
                        normalized = normalized.replace(variant.lower(), canonical.lower())
        return normalized

    # def fuzzy_match(self, input_str, valid_options, threshold=0.8):   
    #     match = get_close_matches(input_str.lower(), [v.lower() for v in valid_options], n=1, cutoff=threshold)
    #     return match[0] if match else input_str

    def natural_language_to_query(self, natural_language_query, query_type="auto"):
        normalized_query = self.normalize_query(natural_language_query)

        # 🔧 Prepare schema string
        schemas_str = '''collection: api-key
This collection stores API keys generated for companies to access the system programmatically.

_id: Unique identifier for the API key.

companyId: The ID of the company this key belongs to, referencing the company collection.

apiKey: The secret API key string.

expiresAt: Unix timestamp for when the key will expire.

access: Describes the permission level of the key (e.g., 'read-only').

creator: A dictionary containing the ID and name of the user who created the key.

createdAt: Unix timestamp of when the key was created.

updatedAt: Unix timestamp of the last update.

collection: allocation
This collection tracks the assignment of customers (leads) to users. It's similar to crm-interaction but focuses on the initial distribution or allocation.

_id: Unique identifier for the allocation record.

customer: A dictionary with details of the allocated customer.

companyId: The ID of the company this allocation belongs to.

dateTime: A timestamp for a scheduled interaction or follow-up.

priority: A number representing the priority of the lead.

process: A list of processes the user is a part of.

company: A dictionary with the user's company ID and name.

reportsTo: A list of user IDs to whom this user reports.

license: A dictionary with details about the user's assigned license.

createdAt: Unix timestamp of user creation.

updatedAt: Unix timestamp of the last update.

collection: whatsapp-interaction
notes: Text notes related to this allocation.

customFields: A list of custom data fields associated with the customer.

assigned: A dictionary containing the ID and name of the user this customer is assigned to.

isInteractionPending: A boolean flag indicating if an interaction is still required.

source: The source from which the lead was generated (e.g., 'Website', 'Bulk Upload').

createdAt: Unix timestamp of the allocation.

updatedAt: Unix timestamp of the last update.

collection: call-interaction
This collection logs every call made or received through the system.

_id: Unique identifier for the call log.

companyId: The ID of the company associated with this call.

user: A dictionary with details of the user (agent) involved in the call.

customer: A dictionary with details of the customer involved in the call.

createdAt: Unix timestamp when the call was logged.

updatedAt: Unix timestamp of the last update.

details: A dictionary containing call specifics like duration, recording URL, direction (inbound/outbound), and status (answered/missed).

isFollowedUp: Boolean indicating if a follow-up has been created for this call.

tag: A tag or disposition set for the call.

collection: cloud-virtual-number
This collection manages the virtual phone numbers assigned to companies.

_id: Unique identifier for the virtual number entry.

phoneNumber: The virtual phone number string.

companyId: The ID of the company using this number.

createdAt: Unix timestamp of when the number was assigned.

updatedAt: Unix timestamp of the last update.

collection: company
This collection stores information about the client companies using the service.

_id: Unique identifier for the company.

address: A dictionary containing the company's physical address details.

billing: A dictionary with billing information.

createdAt: Unix timestamp of when the company was registered.

name: The name of the company.

updatedAt: Unix timestamp of the last update.

industry: The industry the company operates in.

integrations: A dictionary detailing third-party service integrations.

logoUrl: A URL to the company's logo.

csts: Dictionary containing custom settings for the company.

spCode: Dictionary for service provider codes.

collection: crm-field
This collection defines the structure of custom CRM fields for different processes within a company.

_id: Unique identifier for the CRM field definition.

companyId: The ID of the company this definition belongs to.

processId: The ID of the process this field structure applies to, referencing the process collection.

nameMappings: A list that maps internal field names to display names.

defaultFieldAttributes: A list defining attributes for default fields.

crmFields: A list of objects, where each object defines a custom field (e.g., name, type, options).

createdAt: Unix timestamp of creation.

updatedAt: Unix timestamp of the last update.

collection: crm-interaction
This collection stores records of CRM interactions, which are typically scheduled tasks or follow-ups with customers.

_id: Unique identifier for the CRM interaction.

customer: A dictionary with details of the customer.

companyId: The ID of the company this interaction belongs to.

dateTime: The scheduled Unix timestamp for the interaction.

priority: A numerical priority for the task.

notes: Text notes for the interaction.

customFields: A list of custom data points for this interaction.

assigned: A dictionary with the ID and name of the user assigned to this task.

createdAt: Unix timestamp of creation.

updatedAt: Unix timestamp of the last update.

interactionStatus: A dictionary describing the current status of the interaction (e.g., 'Pending', 'Completed').

location: A dictionary containing location data relevant to the interaction.

collection: customer
This collection holds the primary, minimal details of a customer. More detailed information is in customer-details.

_id: Unique identifier for the customer.

name: The customer's name.

pNum: The customer's primary phone number.

cId: The ID of the company this customer belongs to, referencing the company collection.

cAt: Unix timestamp of when the customer was created.

uAt: Unix timestamp of the last update.

creator: A dictionary with the ID and name of the user who created the customer.

isBulk: Boolean indicating if the customer was added via a bulk upload.

collection: customer-assign-log
This collection logs the history of customer assignments to different users.

_id: Unique log entry identifier.

cId: Company ID, referencing the company collection.

pId: Process ID, referencing the process collection.

cusId: Customer ID, referencing the customer collection.

asgn: A dictionary containing details of the assignment, such as the user it was assigned to.

cAt: Unix timestamp of when the assignment was made.

uAt: Unix timestamp of the last update.

collection: customer-details
This collection stores comprehensive, process-specific details for a customer.

_id: Unique identifier for the customer detail record.

name: Customer's name.

pNum: Customer's phone number.

email: Customer's email address.

cusId: The main customer ID, referencing the customer collection.

cId: Company ID, referencing the company collection.

pId: Process ID, referencing the process collection, indicating which process this data belongs to.

cmp: A dictionary for company details of the customer.

prt: Integer representing priority.

dT: A specific date/time associated with the customer.

cFs: A list of custom fields with their values.

status: A dictionary representing the customer's current status in the process.

asgn: A dictionary with details of the assigned user.

cAt: Unix timestamp of creation.

uAt: Unix timestamp of the last update.

creator: A dictionary with the ID and name of the user who created this entry.

isBulk: Boolean indicating if created via bulk upload.

collection: email-interaction
This collection logs all email communications with customers.

_id: Unique identifier for the email log.

companyId: The ID of the company this email belongs to.

subject: The subject line of the email.

body: The content of the email.

status: The delivery status of the email (e.g., 'sent', 'failed').

customer: A dictionary with details of the customer recipient.

sender: A dictionary with details of the user who sent the email.

createdAt: Unix timestamp of when the email was sent.

updatedAt: Unix timestamp of the last update.

collection: email-template
This collection stores pre-written email templates for users to send.

_id: Unique identifier for the template.

companyId: The ID of the company this template belongs to.

processId: Optional ID of a process this template is associated with.

userId: ID of the user who created the template.

access: Defines who can use this template ('private' or 'public').

label: A name or label for the template.

subject: The default subject line for the template.

body: The content of the email template.

createdAt: Unix timestamp of creation.

updatedAt: Unix timestamp of the last update.

collection: license
This collection manages user software licenses.

_id: Unique identifier for the license.

companyId: The ID of the company this license belongs to.

type: The type of license (e.g., 'premium', 'basic').

expiryDate: Unix timestamp for when the license expires.

createdAt: Unix timestamp of creation.

updatedAt: Unix timestamp of the last update.

collection: user: A dictionary containing details of the user to whom this license is assigned.

process
This collection defines the different business processes or workflows within a company (e.g., 'Sales', 'Support').

_id: Unique identifier for the process.

companyId: The ID of the company this process belongs to.

name: The name of the process.

type: The type of workflow (e.g., 'sales_funnel', 'support_ticket').

createdAt: Unix timestamp of creation.

updatedAt: Unix timestamp of the last update.

collection: rechurn-log
This collection logs the 'rechurn' or reallocation of customers, often when they are not contacted or followed up on.

_id: Unique log identifier.

cId: Company ID.

pId: Process ID.

cusId: Customer ID.

rSId: Rechurn Status ID, referencing the rechurn-status collection.

rBy: The user the customer was taken from.

rTo: The user the customer was given to.

isCP: Boolean indicating if the customer was moved to a common pool.

cAt: Unix timestamp of the rechurn event.

uAt: Unix timestamp of the last update.

collection: recurring-interaction
Stores information about interactions that need to happen on a recurring basis.

_id: Unique identifier for the recurring interaction.

companyId: The ID of the associated company.

customer: A dictionary with customer details.

user: A dictionary with user details.

type: The type of recurring interaction (e.g., 'monthly_checkin').

notes: Any notes about the interaction.

followupAt: The Unix timestamp for the next scheduled occurrence.

createdAt: Unix timestamp of creation.

collection: roles
This collection defines user roles and their associated permissions.

_id: Unique identifier for the role.

cId: The Company ID this role belongs to.

name: The name of the role (e.g., 'Agent', 'Manager').

permissions: A dictionary detailing the specific permissions for this role.

type: An integer representing the role type.

cAt: Unix timestamp of creation.

uAt: Unix timestamp of the last update.

collection: sms-interaction
This collection logs all SMS messages sent to customers.

_id: Unique identifier for the SMS log.

companyId: The ID of the company.

message: The text content of the SMS.

status: The delivery status (e.g., 'delivered', 'failed').

customer: A dictionary with the recipient customer's details.

label: A label associated with the SMS type.

user: A dictionary with details of the user who sent the SMS.

createdAt: Unix timestamp of when the SMS was sent.

updatedAt: Unix timestamp of the last update.

collection: sms-template
This collection stores pre-written SMS templates.

_id: Unique identifier for the SMS template.

processId: Optional ID of a process this template is for.

companyId: The ID of the company.

userId: ID of the user who created the template.

access: Access level ('private' or 'public').

label: A name for the template.

message: The content of the SMS template.

createdAt: Unix timestamp of creation.

updatedAt: Unix timestamp of the last update.

collection: time-log
This collection tracks user status changes, like logging in, going on break, etc.

_id: Unique identifier for the time log entry.

companyId: The ID of the company.

user: A dictionary with details of the user.

status: The status being logged (e.g., 'LOGIN', 'BREAK_START', 'IDLE').

duration: The duration spent in the previous status.

createdAt: Unix timestamp of the status change.

updatedAt: Unix timestamp of the last update.

collection: transaction
This collection records all financial transactions, such as license purchases.

_id: Unique identifier for the transaction.

userId: The ID of the user who initiated the transaction.

companyId: The ID of the company.

status: The status of the transaction (e.g., 'success', 'failed').

type: The type of purchase (e.g., 'license', 'credits').

validityInDays: The validity period for the purchased item.

count: The quantity of items purchased.

value: The base price of the transaction.

gst: The amount of Goods and Services Tax.

amountPaid: The total amount paid after taxes and discounts.

discount: The discount amount.

currency: The currency of the transaction (e.g., 'INR').

licenseIds: A list of license IDs created or affected by this transaction.

transactionDate: Unix timestamp of the transaction.

createdAt: Unix timestamp of record creation.

updatedAt: Unix timestamp of the last update.

collection: user
This collection stores data for all users (agents, managers, admins).

_id: Unique identifier for the user (can be email or another unique string).

name: The full name of the user.

phoneNumber: The user's phone number.

email: The user's email address.

designation: A dictionary with the user's job title.

role: A dictionary containing the user's role ID and name.

process: A list of processes the user is a part of.

company: A dictionary with the user's company ID and name.

reportsTo: A list of user IDs to whom this user reports.

license: A dictionary with details about the user's assigned license.

createdAt: Unix timestamp of user creation.

updatedAt: Unix timestamp of the last update.

collection: whatsapp-interaction
This collection logs all WhatsApp message communications.

_id: Unique identifier for the WhatsApp message log.

companyId: The ID of the company.

label: A label for the message type.

message: The content of the WhatsApp message.

status: The delivery status.

createdAt: Unix timestamp of when the message was sent.

updatedAt: Unix timestamp of the last update.

user: A dictionary with details of the user who sent the message.

customer: A dictionary with details of the customer who received the message.

collection: whatsapp-template
This collection stores pre-approved WhatsApp message templates.

_id: Unique identifier for the WhatsApp template.

processId: Optional ID of a process this template is for.

companyId: The ID of the company.

userId: ID of the user who created the template.

access: Access level ('private' or 'public').

label: A name for the template.

message: The content of the template.

createdAt: Unix timestamp of creation.

updatedAt: Unix timestamp of the last update.


collection: rechurn-status
This collection tracks the status and summary of a bulk customer "rechurn" job, which is the process of reallocating inactive or unassigned customers.

_id: Unique identifier for the rechurn job status.

companyId: The ID of the company this job belongs to.

user: A dictionary containing details of the user who initiated the rechurn process.

count: A dictionary that summarizes the customer counts (e.g., total customers identified, customers successfully rechurned).
report-agent
status: An integer code representing the current state of the job (e.g., 0 for pending, 1 for completed).

isCommonPool: A boolean flag that is true if customers are being moved to a general pool instead of specific users.

startEpoch: The Unix timestamp marking the beginning of the period to check for inactive customers.

endEpoch: The Unix timestamp marking the end of the period.

createdAt: Unix timestamp of when the job was created.

updatedAt: Unix timestamp of the last update.

allocatedTo: A list of users designated to receive the rechurned customers.


collection: report-agent-disposition
This collection stores agent disposition reports that analyze call patterns and performance metrics.

_id: Unique identifier for the disposition report.

companyId: The ID of the company this report belongs to, referencing the company collection.

user: A dictionary containing details of the agent/user being reported on, including their ID and name.

callType: A string categorizing the type of calls analyzed (e.g., 'inbound', 'outbound', 'all').

callCounts: A dictionary containing various call metrics (e.g., 'totalCalls', 'answered', 'missed', 'averageDuration').

customFields: A list of additional custom data points specific to the company's reporting needs.

dateRange: A dictionary with 'start' and 'end' Unix timestamps defining the reporting period.

createdAt: Unix timestamp of when the report was generated.

updatedAt: Unix timestamp of the last update to the report.


collection: report-agent-login  
This collection tracks agent login sessions, call activity, and time distribution for performance reporting.  

_id: Unique identifier for the login report.  

companyId: The ID of the company this report belongs to, referencing the company collection.  

user: A dictionary containing details of the agent/user, including their ID and name.  

dateRange: A dictionary with 'start' and 'end' Unix timestamps defining the reporting period.  

calls: A dictionary summarizing call activity (e.g., 'totalCalls', 'answered', 'missed', 'averageHandleTime').  

loginDuration: Total time (in seconds) the agent was logged into the system.  

idleTime: Total time (in seconds) the agent spent in an idle state.  

wrapupTime: Total time (in seconds) spent in post-call wrap-up activities.  

breakTime: Total time (in seconds) the agent spent on breaks.  

createdAt: Unix timestamp of when the report was generated.  

updatedAt: Unix timestamp of the last update to the report.  


collection: report-history  
This collection stores historical records of generated reports, including their parameters and status.  

_id: Unique identifier for the report history entry.  

cId: The ID of the company associated with the report, referencing the company collection.  

userId: The ID of the user who requested or generated the report.  

pId: The ID of the process (if applicable) related to the report, referencing the process collection.  

fromEpoch: The starting Unix timestamp (integer or float) for the report's data range.  

toEpoch: The ending Unix timestamp (integer or float) for the report's data range.  

type: A string indicating the type of report (e.g., 'agent-performance', 'call-summary', 'customer-activity').  

ids: A string or comma-separated list of IDs relevant to the report (e.g., user IDs, customer IDs).  

query: A dictionary containing the filters or query parameters used to generate the report.  

status: The current status of the report (e.g., 'pending', 'completed', 'failed').  

updatedAt: Unix timestamp of the last update to the report history entry.  

createdAt: Unix timestamp of when the report history entry was created.  '''


        # 🔧 Prompt history
        history_examples = ""
        if self.query_history:
            history_examples = "Recent successful queries:\n"
            for nl_query, mongo_query in self.query_history[-3:]:
                history_examples += f"- \"{nl_query}\" → {json.dumps(mongo_query)}\n"

        # 🔧 Collection names
        # collections_list = ", ".join(self.collection_schemas.keys())

        # 🔧 Entity synonyms
        entity_synonyms = {
            "user": ["users", "user", "account", "profile"],
            "order": ["orders", "order", "purchase", "transaction"],
            "product": ["products", "product", "item", "goods"],
            "log": ["logs", "log", "event", "activity"]
        }
        entity_synonyms_str = "\n".join([f"{k}: {v}" for k, v in entity_synonyms.items()])

        # 🔧 Value synonyms string for Gemini
        value_synonyms_str = "\n".join([
            f"{field}: " + ", ".join([f"{canonical} → {variants}" for canonical, variants in syns.items()])
            for field, syns in self.value_synonyms.items()
        ])

        prompt = f"""
You are an extremely smart and helpful MongoDB expert. Given the schemas below, select the best collection for the user's query and generate a valid MongoDB query for it.
Respond ONLY with a JSON object: {{ "collection": <collection_name>, "query": <mongo_query> }}

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

Schemas:
{schemas_str}

Entity synonyms:
{entity_synonyms_str}

Value synonyms to consider (use canonical values in query):
{value_synonyms_str}


User query: "{normalized_query}"

STRICT INSTRUCTIONS:
- Match query terms to the best matching collection using schemas and synonyms
- Use case-insensitive regex for text matching: {{ "field": {{ "$regex": "value", "$options": "i" }} }}
- For state queries, check both full name and abbreviation using $or
- NEVER use $count, $sum, $avg as top-level operators
- For counting, always use aggregation pipeline with $group
- Generate syntactically correct MongoDB queries only
- Test your query mentally before responding
- Look at the query carefully and run it carefully after seeing the schema as well, you are very smart. 
- You can have access to multiple collections, but focus on the most relevant one
- If you are unsure, ask for clarification instead of guessing


"""

        response = self.model.generate_content(prompt)
        print(response.text)
        try:
            response_text = response.text
            if "```json" in response_text:
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                response_text = response_text.split("```")[1].split("```")[0].strip()
            response_text = response_text.strip().strip('"\'')
            result_json = json.loads(response_text)
            if "error" in result_json:
                return result_json
            self.query_history.append((natural_language_query, result_json))
            if len(self.query_history) > 10:
                self.query_history.pop(0)
            return result_json
        except Exception as e:
            return {"error": f"Failed to parse Gemini response: {str(e)}", "raw_response": response.text}
        
            
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
                elif isinstance(value, str):
                    # Convert to case-insensitive exact match
                    new_query["$expr"] = {
                        "$eq": [
                            {"$toLower": f"${key}"},
                            {"$toLower": value}
                        ]
                    }
                else:
                    new_query[key] = value
        return new_query

    def execute_query(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            operation_type = self._get_operation_type(query)
            results = None
            
            if operation_type == "aggregate":
                pipeline = query if isinstance(query, list) else query.get("aggregate", [])
                # Process each $match stage for case-insensitive exact matching
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
        
        
    def _get_operation_type(self, query):
        if isinstance(query, list):
            return "aggregate"
        for op_type in ["find", "insertOne", "insertMany", "updateOne", "updateMany", 
                        "deleteOne", "deleteMany", "countDocuments", "distinct"]:
            if op_type in query:
                return op_type
        return "find"
    
    
    def _lowercase_query_values(self, query):
        if isinstance(query, dict):
            return {
                k: self._lowercase_query_values(v) 
                for k, v in query.items()
            }
        elif isinstance(query, list):
            return [self._lowercase_query_values(item) for item in query]
        elif isinstance(query, str):
            return query.lower()
        else:
            return query


    def process_query(self, natural_language_query, include_explanation=True):
        result = self.natural_language_to_query(natural_language_query)
        if "error" in result:
            return {
                "status": "error",
                "message": result["error"],
                "raw_response": result.get("raw_response", "")
            }

        collection_name = result["collection"]
        mongo_query = result["query"]
        print("collection used by ai: ", collection_name)

        # First attempt (raw query)
        results = self.execute_query(collection_name, mongo_query)

        # If no results and no error, try fallback
        if "error" not in results and results.get("count", 0) == 0:
            print("No results found. Retrying with case-insensitive query...")
            ci_query = self._convert_to_case_insensitive(mongo_query)
            results = self.execute_query(collection_name, ci_query)
            mongo_query = ci_query  # update returned query

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
                explanation = self.generate_query_explanation(
                    natural_language_query,
                    mongo_query,
                    results
                )
                response["explanation"] = explanation

        return response


    def generate_query_explanation(self, natural_language_query, mongo_query, results):
        result_count = len(results["results"]) if "results" in results else 0
        result_sample = results["results"][:6] if "results" in results else []
        operation_type = results.get("query_type", "find")
        prompt = f"""
You are a helpful MongoDB Copilot assistant who works as a whatsapp chatbot. Explain the results of a database operation in a conversational, helpful manner.

Original natural language query: "{natural_language_query}"

MongoDB operation type: {operation_type}

MongoDB query executed: {json.dumps(mongo_query, indent=2)}

Number of results: {result_count}

Sample results (up to 3): {json.dumps(result_sample, indent=2, default=str)}
"""
        response = self.model.generate_content(prompt)
        return response.text
 