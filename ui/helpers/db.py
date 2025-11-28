import time
from .queries import hasQuery, getQuery
from django.db import connection, DatabaseError
OMMITED_PARAMS = ['csrfmiddlewaretoken', 'query']

STORED_PROCEDURES = {
    'storedProcedure01': {
        'sql': 'select * from fn_total_logs_per_action_type(%s, %s)',
        'parameters': ['start_time', 'end_time']
    },
    'storedProcedure02': {
        'sql': 'select * from fn_logs_per_day_for_action(%s, %s, %s)',
        'parameters': ['action_type', 'start_time', 'end_time']
    },
    'storedProcedure03': {
        'sql': 'select * from fn_most_common_action_per_source_ip(%s)',
        'parameters': ['single_day']
    },
    'storedProcedure04': {
        'sql': 'select * from fn_top_blocks_by_actions_per_day(%s, %s)',
        'parameters': ['start_day', 'end_day']
    },
    'storedProcedure05': {
        'sql': 'select * from fn_referrers_multiple_resources()',
        'parameters': []
    },
    'storedProcedure06': {
        'sql': 'select * from fn_second_most_common_resource()',
        'parameters': []
    },
    'storedProcedure07': {
        'sql': 'select * from fn_access_logs_below_size(%s)',
        'parameters': ['size_bytes']
    },
    'storedProcedure08': {
        'sql': 'select * from fn_blocks_rep_and_serv_same_day()',
        'parameters': []
    },
    'storedProcedure09': {
        'sql': 'select * from fn_blocks_rep_and_serv_same_day_hour()',
        'parameters': []
    },
    'storedProcedure10': {
        'sql': 'select * from fn_access_logs_by_user_agent_version(%s)',
        'parameters': ['version']
    },
    'storedProcedure11': {
        'sql': "select * from fn_ips_with_method_in_range('ACCESS', %s, %s, %s)",
        'parameters': ['action_type', 'start_time', 'end_time']
    },
    'storedProcedure12': {
        'sql': "select * from fn_ips_with_two_methods_in_range('ACCESS', %s, %s, %s, %s)",
        'parameters': ['action_type', 'action_type_2', 'start_time', 'end_time']
    },
    'storedProcedure13': {
        'sql': "select * from fn_ips_with_n_methods_in_range('ACCESS', 4, %s, %s)",
        'parameters': ['start_time', 'end_time']
    },
    'fn_insert_log': {
        'sql': 'SELECT fn_insert_new_log(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
        'parameters': [
            'log_type',
            'action_type',
            'specific_timestamp',
            'source_ip',
            'destination_ip',
            'block_id',
            'size_bytes',
            'remote_name',
            'auth_user',
            'resource',
            'http_status',
            'referrer',
            'user_agent'
        ]
    },
    'log_user_query': {
        'admin': True,
        'sql': 'log_user_query(%s, %s, %s)',
    }
}

def getStoredProcedure(key):
    sp = STORED_PROCEDURES[key]
    if(sp.get('admin') == True):
        return ''
    return STORED_PROCEDURES.get(key).get('sql')

def getStoredProcedureParameters(key):
    return STORED_PROCEDURES.get(key).get('parameters')


def executeQueryAndGetResults(request):
    result = {}
    if(hasQuery(request)):
        result['selectedQuery'] = getQuery(request)
        result['queryParams'] = getParams(request)
        result['results'] = getResults(request)
    return result

def getParams(request):
    params = {}
    for key in request.POST:
        if(key not in OMMITED_PARAMS):
            params[key] = request.POST.get(key)
    return params

def getResults(request): 
    query = getQuery(request)
    params = getParams(request)
    result = run_log_analyzer(query, params)
    return result

def run_log_analyzer(query, params):
    if query != None:
        sql = getStoredProcedure(query.get('storedProcedure'))
        parameters = []
        for key in getStoredProcedureParameters(query.get('storedProcedure')):
            parameters.append(params.get(key, 'null'))
    else:
        raise ValueError(f"Unknown query method: {query}")
    # remove to make the call
    # return {
    #     'data': [{'sql': sql, 'parameters': parameters}],
    #     'executionTimeInMs': 12314,
    # }
    try:
        # 1. Obtain a cursor and execute the raw SQL
        with connection.cursor() as cursor:
            
            # log user query
            # cursor.execute(getStoredProcedure('log_user_query'), [1, sql, parameters])
            
            # IMPORTANT: Use placeholders (%s) and pass parameters separately 
            # to prevent SQL Injection.
            start_time = time.time()
            cursor.execute(sql, parameters)
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
            

            # 2. Fetch column names and results
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                data = cursor.fetchall()
                
                # 3. Map results to a list of dictionaries
                results = [dict(zip(columns, row)) for row in data]
                return {
                    'data': results,
                    'executionTimeInMs': duration_ms
                }
            else:
                return [] # No results returned

    except DatabaseError as e:
        print(f"Database Error during SP execution: {e}")
        raise # Re-raise the error to be handled by the calling view