from .queries import getQueriesDictionary, hasQuery, getQuery

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
    for i in range(2):
        result["data"][i] = {
            "title": "title",
            "description": "description"
        }

    return result;