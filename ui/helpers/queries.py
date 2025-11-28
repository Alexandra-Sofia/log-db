QUERY_DICTIONARY = {
    "01": {
        "title": "Find the total logs per type that were created within a specified time range in descending order",
        "htmlInputs": {
            "time_range": "required"
        },
        "storedProcedure": "storedProcedure01"
    },
    "02": {
        "title": "Find the total logs per day for a specific action type and time range",
        "htmlInputs": {
            "action_type": "required",
            "time_range": "required"
        },
        "storedProcedure": "storedProcedure02",
    },
    "03": {
        "title": "Find the most common log per source IP for a specific day",
        "htmlInputs": {
            "day": "required"
        },
        "storedProcedure": "storedProcedure03"
    },
    "04": {
        "title": "Find the top-5 Block IDs with regards to total number of actions per day for a specific day range",
        "htmlInputs": {
            "dayRange": "required"
        },
        "storedProcedure": "storedProcedure04"
    },
    "05": {
        "title": "Find the referrers (if any) that have led to more than one resources",
        "htmlInputs": {},
        "storedProcedure": "storedProcedure05"
    },
    "06": {
        "title": "Find the 2nd most common resource requested",
        "htmlInputs": {},
        "storedProcedure": "storedProcedure06"
    },
    "07": {
        "title": "Find the access log (all fields) where the size is less than a specified number",
        "htmlInputs": {
            "size_bytes": "required"
        },
        "storedProcedure": "storedProcedure07"
    },
    "08": {
        "title": "Find the blocks that have been replicated the same day that they have also been served",
        "htmlInputs": {},
        "storedProcedure": "storedProcedure08"
    },
    "09": {
        "title": "Find the blocks that hae been replicated the same day and hour that they have also been served",
        "htmlInputs": {},
        "storedProcedure": "storedProcedure09"
    },
    "10": {
        "title": "Find access logs that specified a particular version of Firefox as their browser",
        "htmlInputs": {
            "version": "required"
        },
        "storedProcedure": "storedProcedure10"
    },
    "11": {
        "title": "Find IPs that have issued a particular HTTP method on a particular time range",
        "htmlInputs": {
            "action_type": "required",
            "time_range": "required"
        },
        "storedProcedure": "storedProcedure11"
    },
    "12": {
        "title": "Find IPs that have issued two particular HTTP methods on a particular time range",
        "htmlInputs": {
            "action_type": "required",
            "action_type_2": "required",
            "time_range": "required"
        },
        "storedProcedure": "storedProcedure12"
    },
    "13": {
        "title": "Find IPs that have issued any four distinct HTTP methods on a particular time range",
        "htmlInputs": {
            "time_range": "required"
        },
        "storedProcedure": "storedProcedure13"
    },
    "14": {
        "title": "Input a log entry with following data",
        "htmlInputs": {
            "log_type": "required",
            "action_type": "required",
            "timestamp": "required",
            "source_ip": "required",
            "destination_ip": "required",
            "block_id": "required",
            "size_bytes": "required",

            "optional": "optional",
            "remote_name": "optional",
            "auth_user": "optional",
            "resource": "optional",
            "http_status": "optional",
            "referrer": "optional",
            "user_agent": "optional"
        },
        "storedProcedure": "fn_insert_log"
    },
    "15": {
        "title": "Find all the user queries in descending order",
        "storedProcedure": "get_user_query_log"
    }
}

def hasQuery(request): 
    return request.POST and request.POST.get('query') != None and QUERY_DICTIONARY[request.POST.get('query')] != None

def getQuery(request):
    if(hasQuery(request)):
        return QUERY_DICTIONARY[request.POST.get('query')]
    return None

def getQueriesDictionary():
    return {
        "queries": QUERY_DICTIONARY            
    }


