import json
import boto3

DATABASE_NAME = 'spotrank-timestream'
TABLE_NAME = 'spot-table'
result = ""


def run_query(instance_type, region, az, start, end):
    session = boto3.Session()
    query_client = session.client('timestream-query')
    
    # if region is not null, add region constraint string to query string
    # else region is null, don't add region constraint to query string
    
    global result
    
    if start == '' or end == '':
        result = "[]"
        return
    
    instances = []
    for instance in instance_type.split(","):
        instances.append("'" + instance.strip() + "'")
    queryInstanceType = "\nAND InstanceType = ANY (VALUES " + ", ".join(instances) + ")" 
    
    if instance_type == "*":
        queryInstanceType = ""
        
    Regions = []
    for reg in region.split(","):
        Regions.append("'" + reg.strip() + "'")
    queryRegion = "\nAND Region = ANY (VALUES " + ", ".join(Regions) + ") "
    
    if region == "*":
        queryRegion = ""
    
    azs = []
    azs_string = []
    queryAZ = "\nAND ("
    for azz in az.split(","):
        azs.append("'" + azz.strip() + "'")
        azs_string.append("substr(AZ, length(AZ)-" + str(len(azz.strip()) - 1) + ") = " + azz.strip())
    queryAZ_withAlphabet = "\nAND AZ = ANY (VALUES " + ", ".join(azs) + ") "
    queryAZ = "\nAND (substr(AZ, length(AZ)) = ANY (VALUES " + ", ".join(azs) + ") or substr(AZ, length(AZ)-1) = ANY (VALUES " + ", ".join(azs) + ")) "
    
    if az == "*":
        queryAZ_withAlphabet = ""
        queryAZ = ""
    
    query_string = f"""
    SELECT SpotPrice, Savings, SPS, AZ, Region, InstanceType, IF, time FROM "{DATABASE_NAME}"."{TABLE_NAME}" 
    WHERE time between from_iso8601_date('{start}') and from_iso8601_date('{end}')""" + queryInstanceType + queryRegion + queryAZ_withAlphabet + f"""
    ORDER BY time DESC limit 20000"""
    
    query_string_second = f"""
    SELECT SpotPrice, Savings, SPS, AZ, Region, InstanceType, IF, time FROM "{DATABASE_NAME}"."{TABLE_NAME}" 
    WHERE time between from_iso8601_date('{start}') and from_iso8601_date('{end}')""" + queryInstanceType + queryRegion + queryAZ + f"""
    ORDER BY time DESC limit 20000"""
    
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
    print("Metadata: %s" % column_info)
    print("Data: ")
    id = 1
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
        row_output.append(_parse_datum(info, datum))
    return "{%s}" % ",".join(row_output)

def _parse_datum(info, datum):
    if datum.get('NullValue', False):
        return "%s=NULL" % info['Name'],
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
    elif 'Name' in info and info['Name'] == 'IF':
        if datum['ScalarValue'] == '<5%':
            return _parse_column_name(info) + "1" + '"'
        elif datum['ScalarValue'] == '5-10%':
            return _parse_column_name(info) + "1.5" + '"'
        elif datum['ScalarValue'] == '10-15%':
            return _parse_column_name(info) + "2" + '"'
        elif datum['ScalarValue'] == '15-20%':
            return _parse_column_name(info) + "2.5" + '"'
        else:
            return _parse_column_name(info) + "3" + '"'
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
    operation = event['httpMethod']
    if operation != 'GET':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Headers': '*',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,GET'
            },
            'body': json.dumps("[]")
        }
    info = event['queryStringParameters']
    instance_type = info['InstanceType']
    region = info['Region']
    az = info['AZ']
    start = info['Start']
    end = info['End']
    
    run_query(instance_type, region, az, start, end)
    global result
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': json.dumps(result)
    }
