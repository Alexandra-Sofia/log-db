from .queries import hasQuery, getQuery
from ..models import LogType # Assuming you have a model named LogEntry

def executeQueryAndGetResults(request):
    result = {}
    if(hasQuery(request)):
        result['selectedQuery'] = getQuery(request)
        result['queryParams'] = request.POST
        result['results'] = getResults(request.POST)
    return result

def getParams(request):
    return None

def getResults(payload): 
    result = {
        "data": {}
    }
    for i in range(20):
        result["data"][i] = {
            "title": "title",
            "description": "description",
            "logTypes": retrieve_logs_as_models()
        }
    return result

def retrieve_logs_as_models():
    # NOTE: The SELECT fields MUST match the fields defined on the LogEntry model.
    query = """
    SELECT id, name
    FROM log_type
    """
    # Use parameters just like with cursor.execute()
    log_entries = LogType.objects.raw(query)
    result = {}
    for type in log_entries:
        result[type.id] = type.name
    # log_entries is a RawQuerySet, which can be iterated over like a normal QuerySet
    return result