import json
import boto3
from datetime import timedelta, date

DATABASE_NAME = 'spotlake'
result = ""
id = 1


def run_query(TABLE_NAME, features, start, end):
    session = boto3.Session()
    query_client = session.client('timestream-query')

    global result
    
    if start == '' or end == '':
        result = "[]"
        return
    
    query_string = ""
    
    if TABLE_NAME == 'aws':
        instance_type = features['InstanceType']
        region = features['Region']
        az = features['AZ']
        
        # instance type should be specified

        region_condition = ""
        az_condition = ""

        if region != '*':
            region_condition = f"AND Region = '{region}'"    
        if az != '*':
            az_condition = f"AND AZ = '{az}'"
        
        query_string = f"""
        (SELECT ALL.* FROM (
                SELECT InstanceType, Region, AZ, MAX(time) as time 
                FROM "spotlake"."aws" 
                WHERE time < from_iso8601_date('{start}')
                AND InstanceType = '{instance_type}' {region_condition} {az_condition}
                GROUP BY InstanceType, Region, AZ
                )LATEST, "spotlake"."aws" ALL 
        WHERE LATEST.InstanceType = ALL.InstanceType 
        AND LATEST.Region = ALL.Region 
        AND LATEST.AZ = ALL.AZ 
        AND LATEST.time = ALL.time)
        UNION 
        (SELECT * 
        FROM "spotlake"."aws" 
        WHERE time BETWEEN from_iso8601_date('{start}') and from_iso8601_date('{end}') 
        AND InstanceType = '{instance_type}' {region_condition} {az_condition})"""
    
    elif TABLE_NAME == 'azure':
        instance_type = features['InstanceType']
        region = features['Region']
        instance_tier = features['InstanceTier']
        
        # instance type should be specified

        region_condition = ""

        if region != '*':
            region_condition = f"AND Region = '{region}'"
        
        if instance_tier != '*':
            tier_condition = f"AND InstanceTier = '{instance_tier}'"
        
        query_string = f"""
        (SELECT ALL.* FROM (
                SELECT InstanceType, Region, MAX(time) as time 
                FROM "spotlake"."azure" 
                WHERE time < from_iso8601_date('{start}')
                AND InstanceType = '{instance_type}' {region_condition} {tier_condition}
                GROUP BY InstanceType, Region
                )LATEST, "spotlake"."azure" ALL 
        WHERE LATEST.InstanceType = ALL.InstanceType 
        AND LATEST.Region = ALL.Region
        AND LATEST.time = ALL.time)
        UNION 
        (SELECT * 
        FROM "spotlake"."azure" 
        WHERE time BETWEEN from_iso8601_date('{start}') and from_iso8601_date('{end}') 
        AND InstanceType = '{instance_type}' {region_condition} {tier_condition})"""
    
    elif TABLE_NAME == 'gcp':
        instance_type = features['InstanceType']
        region = features['Region']
        
        # instance type should be specified

        region_condition = ""
        tier_condition = ""

        if region != '*':
            region_condition = f"AND Region = '{region}'"
        
        query_string = f"""
        (SELECT ALL.* FROM (
                SELECT InstanceType, Region, MAX(time) as time 
                FROM "spotlake"."gcp" 
                WHERE time < from_iso8601_date('{start}')
                AND InstanceType = '{instance_type}' {region_condition}
                GROUP BY InstanceType, Region
                )LATEST, "spotlake"."gcp" ALL 
        WHERE LATEST.InstanceType = ALL.InstanceType 
        AND LATEST.Region = ALL.Region
        AND LATEST.time = ALL.time)
        UNION 
        (SELECT * 
        FROM "spotlake"."gcp" 
        WHERE time BETWEEN from_iso8601_date('{start}') and from_iso8601_date('{end}') 
        AND InstanceType = '{instance_type}' {region_condition})"""
    
    paginator = query_client.get_paginator('query')
    page_iterator = paginator.paginate(QueryString=query_string)
    result = "["
    for page in page_iterator:
        _parse_query_result(page)
    result = result[:len(result)-1] + "]"
    if result == "]":
        result = "[]"

def _parse_query_result(query_result):
    column_info = query_result['ColumnInfo']
    global result
    global id
    for row in query_result['Rows']:
        tmp = _parse_row(column_info, row)
        tmp = tmp[:1] + '"id":"' + str(id) + '",' + tmp[1:]
        id += 1
        result = result + tmp + ","
        print(tmp)

def _parse_row(column_info, row):
    data = row['Data']
    row_output = []
    for j in range(len(data)):
        info = column_info[j]
        datum = data[j]
        returnedData = _parse_datum(info, datum)
        if returnedData != "":
            row_output.append(returnedData)
    return "{%s}" % ",".join(row_output)

def _parse_datum(info, datum):
    if datum.get('NullValue', False):
        return "%s:NULL" % info['Name']
    column_type = info['Type']

    # If the column is of TimeSeries Type
    if 'TimeSeriesMeasureValueColumnInfo' in column_type:
        return _parse_time_series(info, datum)
    # If the column is of Array Type
    elif 'ArrayColumnInfo' in column_type:
        array_values = datum['ArrayValue']
        return "'%s':'%s'" % (info['Name'], _parse_array(info['Type']['ArrayColumnInfo'], array_values))
    # If the column is of Row Type
    elif 'RowColumnInfo' in column_type:
        row_column_info = info['Type']['RowColumnInfo']
        row_values = datum['RowValue']
        return _parse_row(row_column_info, row_values)
    # If the column is of Scalar Type
    # If the column is Time
    elif 'ScalarType' in column_type and column_type['ScalarType'] == 'TIMESTAMP':
        return _parse_column_name(info) + datum['ScalarValue'].split(".")[0] + '"'
    # If the column is AZ
    elif 'Name' in info and info['Name'] == 'AZ':
        return _parse_column_name(info) + datum['ScalarValue'].split("-az")[-1] + '"'
    elif 'Name' in info and info['Name'] == 'measure_name':
        return ""
    # The others
    else:
        return _parse_column_name(info) + datum['ScalarValue'] + '"'

def _parse_time_series(info, datum):
    time_series_output = []
    for data_point in datum['TimeSeriesValue']:
        time_series_output.append("{time=%s, value=%s}"
                                  % (data_point['Time'],
                                     _parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],
                                                       data_point['Value'])))
    return "[%s]" % str(time_series_output)

def _parse_array(array_column_info, array_values):
    array_output = []
    for datum in array_values:
        array_output.append(_parse_datum(array_column_info, datum))
    return "[%s]" % str(array_output)

def run_query_with_multiple_pages(limit):
    query_with_limit = SELECT_ALL + " LIMIT " + str(limit)
    print("Starting query with multiple pages : " + query_with_limit)
    run_query(query_with_limit)

def cancel_query():
    print("Starting query: " + SELECT_ALL)
    result = client.query(QueryString=SELECT_ALL)
    print("Cancelling query: " + SELECT_ALL)
    try:
        client.cancel_query(QueryId=result['QueryId'])
        print("Query has been successfully cancelled")
    except Exception as err:
        print("Cancelling query failed:", err)

def _parse_column_name(info):
    if 'Name' in info:
        return '"' + info['Name'] + '"' + ':' + '"'
    else:
        return ""

def lambda_handler(event, context):
    operation = event['requestContext']['http']['method']
    '''
    
    
    
    
    
    
    
    <HIDDEN>
    Filtering Code
    CORS, httpMethod, etc...
    
    
    
    
    
    
    
    
    '''
    global id
    id = 1
    info = event['queryStringParameters']
    table_name = info['TableName']
    start = info['Start']
    end = info['End']
    end = (date.fromisoformat(end) + timedelta(days=1)).isoformat()
    
    run_query(table_name, info, start, end)
    global result
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': 'https://spotlake.ddps.cloud',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': json.dumps(result)
    }
