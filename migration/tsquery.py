import boto3
import pandas as pd


PROFILE_NAME = "source-profile"
REGION_NAME = "us-west-2"


def run_query(query_string):
    try:
        session = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION_NAME)
        query_client = session.client('timestream-query')
        paginator = query_client.get_paginator('query')
        page_iterator = paginator.paginate(QueryString=query_string)
        for page in page_iterator:
            _parse_query_result(page)
    except Exception as err:
        print("Exception while running query:", err)

        
def _parse_query_result(query_result):
    query_status = query_result["QueryStatus"]
    column_info = query_result['ColumnInfo']
    for row in query_result['Rows']:
        _parse_row(column_info, row)

        
def _parse_row(column_info, row):
    data = row['Data']
    row_output = []
    for j in range(len(data)):
        info = column_info[j]
        datum = data[j]
        row_output.append(_parse_datum(info, datum))
    return "{%s}" % str(row_output)


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
        return "%s=%s" % (info['Name'], _parse_array(info['Type']['ArrayColumnInfo'], array_values))

    # If the column is of Row Type
    elif 'RowColumnInfo' in column_type:
        row_column_info = info['Type']['RowColumnInfo']
        row_values = datum['RowValue']
        return _parse_row(row_column_info, row_values)

    # If the column is of Scalar Type
    else:
        global timestream_data
        if info['Name'] == "time":
            timestream_data[info['Name']].append(datum['ScalarValue'].split('.')[0]+"+00:00")
        elif info['Name'] != "measure_name" and info['Name'] != "measure_value::double":
            timestream_data[info['Name']].append(datum['ScalarValue'])
        return _parse_column_name(info) + datum['ScalarValue']

    
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


def _parse_column_name(info):
    if 'Name' in info:
        return info['Name'] + "="
    else:
        return ""

    
def get_timestream(start_date, end_date):
    global timestream_data
    timestream_data = {"SpotPrice" : [], "Savings" : [], "SPS" : [], "AZ" : [], "Region" : [], "InstanceType" : [], "IF" : [], "time" : []}
    
    print(f"Start query ({start_date}~{end_date})")
    query_string = f"""SELECT * FROM "spotrank-timestream"."spot-table" WHERE time between from_iso8601_timestamp('{start_date}') and from_iso8601_timestamp('{end_date}') ORDER BY time"""
    run_query(query_string)
    print(start_date + "~" + end_date + " is end")
    timestream_df = pd.DataFrame(timestream_data)
    timestream_df.drop_duplicates(inplace=True)
    return timestream_df


def get_timestamps(start_date, end_date):
    global timestream_data
    timestream_data = {"time" : []}

    print(f"Start query ({start_date}~{end_date})")
    query_string = f"""SELECT DISTINCT time FROM "spotrank-timestream"."spot-table" WHERE time between from_iso8601_date('{start_date}') and from_iso8601_date('{end_date}') ORDER BY time"""
    run_query(query_string)
    print(start_date + "~" + end_date + " is end")
    return timestream_data['time']
