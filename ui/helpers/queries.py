QUERY_DICTIONARY = {
    "01": {
        "title": "Find the total logs per type that were created within a specified time range in descending order",
        "params": {
            "timeRange": "timeRange"
        },
        "template": "template01",
        "description": "description"
    },
    "02": {
        "title": "Find the total logs per day for a specific action type and time range",
        "params": {
            "actionType": "string",
            "timeRange": "timeRange"
        },
        "template": "template02",
        "description": "description"
    },
    "03": {
        "title": "Find the most common log per source IP for a specific day",
        "params": {
            "day": "date"
        },
        "template": "template03",
        "description": "description"
    },
    "04": {
        "title": "Find the top-5 Block IDs with regards to total number of actions per day for a specific day range",
        "params": {
            "dayRange": "dayRange"
        },
        "template": "template04",
        "description": "description"
    },
    "05": {
        "title": "Find the referrers (if any) that have led to more than one resources",
        "params": {},
        "template": "template05",
        "description": "description"
    },
    "06": {
        "title": "Find the 2nd most common resource requested",
        "params": {},
        "template": "template06",
        "description": "description"
    },
    "07": {
        "title": "Find the access log (all fields) where the size is less than a specified number",
        "params": {
            "size_bytes": "number"
        },
        "template": "template07",
        "description": "description"
    },
    "08": {
        "title": "Find the blocks that have been replicated the same day that they have also been served",
        "params": {},
        "template": "template08",
        "description": "description"
    },
    "09": {
        "title": "Find the blocks that hae been replicated the same day and hour that they have also been served",
        "params": {},
        "template": "template09",
        "description": "description"
    },
    "10": {
        "title": "Find access logs that specified a particular version of Firefox as their browser",
        "params": {},
        "template": "template10",
        "description": "description"
    },
    "11": {
        "title": "Find IPs that have issued a particular HTTP method on a particular time range",
        "params": {
            "timeRange": "timeRange"
        },
        "template": "template11",
        "description": "description"
    },
    "12": {
        "title": "Find IPs that have issued two particular HTTP methods on a particular time range",
        "params": {
            "timeRange": "timeRange"
        },
        "template": "template12",
        "description": "description"
    },
    "13": {
        "title": "Find IPs that have issued any four distinct HTTP methods on a particular time range",
        "params": {
            "timeRange": "timeRange"
        },
        "template": "template13",
        "description": "description"
    },
    "14": {
        "title": "Input a log entry with following data",
        "params": {
            "log_type": "string",
            "action_type": "string",
            "timestamp": "timestamp",
            "source_ip": "string",
            "destination_ip": "string",
            "block_id": "number",
            "size_bytes": "number",
        },
        "template": "template14",
        "query": "SELECT * FROM log_types",
        "description": "description"
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


