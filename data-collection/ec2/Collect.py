import boto3
import time
import pickle
import datetime
import json
import subprocess
import io
import logging
import pandas as pd
from datetime import datetime, timedelta
from ec2_package import get_regions, store_spot_price
from update_base import update_base
from botocore.config import Config
from botocore.exceptions import ClientError

# SNS Configures

email = 'spotrank@kookmin.ac.kr'
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
class SnsWrapper:
    """Encapsulates Amazon SNS topic and subscription functions."""
    def __init__(self, sns_resource):
        """
        :param sns_resource: A Boto3 Amazon SNS resource.
        """
        self.sns_resource = sns_resource

    def create_topic(self, name):
        """
        Creates a notification topic.

        :param name: The name of the topic to create.
        :return: The newly created topic.
        """
        try:
            topic = self.sns_resource.create_topic(Name=name)
            logger.info("Created topic %s with ARN %s.", name, topic.arn)
        except ClientError:
            logger.exception("Couldn't create topic %s.", name)
            raise
        else:
            return topic

    def list_topics(self):
        """
        Lists topics for the current account.

        :return: An iterator that yields the topics.
        """
        try:
            topics_iter = self.sns_resource.topics.all()
            logger.info("Got topics.")
        except ClientError:
            logger.exception("Couldn't get topics.")
            raise
        else:
            return topics_iter

    @staticmethod
    def delete_topic(topic):
        """
        Deletes a topic. All subscriptions to the topic are also deleted.
        """
        try:
            topic.delete()
            logger.info("Deleted topic %s.", topic.arn)
        except ClientError:
            logger.exception("Couldn't delete topic %s.", topic.arn)
            raise

    @staticmethod
    def subscribe(topic, protocol, endpoint):
        """
        Subscribes an endpoint to the topic. Some endpoint types, such as email,
        must be confirmed before their subscriptions are active. When a subscription
        is not confirmed, its Amazon Resource Number (ARN) is set to
        'PendingConfirmation'.

        :param topic: The topic to subscribe to.
        :param protocol: The protocol of the endpoint, such as 'sms' or 'email'.
        :param endpoint: The endpoint that receives messages, such as a phone number
                         (in E.164 format) for SMS messages, or an email address for
                         email messages.
        :return: The newly added subscription.
        """
        try:
            subscription = topic.subscribe(
                Protocol=protocol, Endpoint=endpoint, ReturnSubscriptionArn=True)
            logger.info("Subscribed %s %s to topic %s.", protocol, endpoint, topic.arn)
        except ClientError:
            logger.exception(
                "Couldn't subscribe %s %s to topic %s.", protocol, endpoint, topic.arn)
            raise
        else:
            return subscription

    def list_subscriptions(self, topic=None):
        """
        Lists subscriptions for the current account, optionally limited to a
        specific topic.

        :param topic: When specified, only subscriptions to this topic are returned.
        :return: An iterator that yields the subscriptions.
        """
        try:
            if topic is None:
                subs_iter = self.sns_resource.subscriptions.all()
            else:
                subs_iter = topic.subscriptions.all()
            logger.info("Got subscriptions.")
        except ClientError:
            logger.exception("Couldn't get subscriptions.")
            raise
        else:
            return subs_iter

    @staticmethod
    def add_subscription_filter(subscription, attributes):
        """
        Adds a filter policy to a subscription. A filter policy is a key and a
        list of values that are allowed. When a message is published, it must have an
        attribute that passes the filter or it will not be sent to the subscription.

        :param subscription: The subscription the filter policy is attached to.
        :param attributes: A dictionary of key-value pairs that define the filter.
        """
        try:
            att_policy = {key: [value] for key, value in attributes.items()}
            subscription.set_attributes(
                AttributeName='FilterPolicy', AttributeValue=json.dumps(att_policy))
            logger.info("Added filter to subscription %s.", subscription.arn)
        except ClientError:
            logger.exception(
                "Couldn't add filter to subscription %s.", subscription.arn)
            raise

    @staticmethod
    def delete_subscription(subscription):
        """
        Unsubscribes and deletes a subscription.
        """
        try:
            subscription.delete()
            logger.info("Deleted subscription %s.", subscription.arn)
        except ClientError:
            logger.exception("Couldn't delete subscription %s.", subscription.arn)
            raise

    def publish_text_message(self, phone_number, message):
        """
        Publishes a text message directly to a phone number without need for a
        subscription.

        :param phone_number: The phone number that receives the message. This must be
                             in E.164 format. For example, a United States phone
                             number might be +12065550101.
        :param message: The message to send.
        :return: The ID of the message.
        """
        try:
            response = self.sns_resource.meta.client.publish(
                PhoneNumber=phone_number, Message=message)
            message_id = response['MessageId']
            logger.info("Published message to %s.", phone_number)
        except ClientError:
            logger.exception("Couldn't publish message to %s.", phone_number)
            raise
        else:
            return message_id

    @staticmethod
    def publish_message(topic, message, attributes):
        """
        Publishes a message, with attributes, to a topic. Subscriptions can be filtered
        based on message attributes so that a subscription receives messages only
        when specified attributes are present.

        :param topic: The topic to publish to.
        :param message: The message to publish.
        :param attributes: The key-value attributes to attach to the message. Values
                           must be either `str` or `bytes`.
        :return: The ID of the message.
        """
        try:
            att_dict = {}
            for key, value in attributes.items():
                if isinstance(value, str):
                    att_dict[key] = {'DataType': 'String', 'StringValue': value}
                elif isinstance(value, bytes):
                    att_dict[key] = {'DataType': 'Binary', 'BinaryValue': value}
            response = topic.publish(Message=message, MessageAttributes=att_dict)
            message_id = response['MessageId']
            logger.info(
                "Published message with attributes %s to topic %s.", attributes,
                topic.arn)
        except ClientError:
            logger.exception("Couldn't publish message to topic %s.", topic.arn)
            raise
        else:
            return message_id

    @staticmethod
    def publish_multi_message(
            topic, subject, default_message, sms_message, email_message):
        """
        Publishes a multi-format message to a topic. A multi-format message takes
        different forms based on the protocol of the subscriber. For example,
        an SMS subscriber might receive a short, text-only version of the message
        while an email subscriber could receive an HTML version of the message.

        :param topic: The topic to publish to.
        :param subject: The subject of the message.
        :param default_message: The default version of the message. This version is
                                sent to subscribers that have protocols that are not
                                otherwise specified in the structured message.
        :param sms_message: The version of the message sent to SMS subscribers.
        :param email_message: The version of the message sent to email subscribers.
        :return: The ID of the message.
        """
        try:
            message = {
                'default': default_message,
                'sms': sms_message,
                'email': email_message
            }
            response = topic.publish(
                Message=json.dumps(message), Subject=subject, MessageStructure='json')
            message_id = response['MessageId']
            logger.info("Published multi-format message to topic %s.", topic.arn)
        except ClientError:
            logger.exception("Couldn't publish message to topic %s.", topic.arn)
            raise
        else:
            return message_id

sns_wrapper = SnsWrapper(boto3.resource('sns', region_name='us-west-2'))
topic_name = f'sps-timestream-upload-error-topic'

# S3 Configures

BUCKET_NAME = 'sungjae-sps-data'
SAVE_BUCKET_NAME = 'spotrank-latest'

s3 = boto3.resource('s3')
credentials = pickle.load(open('/home/ec2-user/SpotInfo/pkls/user_cred_df.pkl', 'rb'))
user_workload = pickle.load(open('/home/ec2-user/SpotInfo/pkls/bin_packed_workload.pickle', 'rb'))

# Timestream Configures

DATABASE_NAME = "spotrank-timestream"
TABLE_NAME = "spot-table"
KeyId = "d9264200-41b0-4872-8506-105562ec3b26"
HT_TTL_HOURS = 4392
CT_TTL_DAYS = 365

session = boto3.session.Session(profile_name='jaeil', region_name='us-west-2')
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))
query_client = session.client('timestream-query')

# Subprocess Command To Get SpotInfo Data

COMMAND = ['wget https://github.com/alexei-led/spotinfo/releases/download/1.0.7/spotinfo_linux_amd64 -O spotinfo', 'chmod +x spotinfo', './spotinfo --output csv --region all']

# Collect SpotInfo Data From ./spotinfo

def get_spotinfo(command = COMMAND):
    process1 = subprocess.Popen(command[0].split(' '), stdout=subprocess.PIPE)
    process1.communicate()
    process2 = subprocess.Popen(command[1].split(' '), stdout=subprocess.PIPE)
    process2.communicate()
    process3 = subprocess.Popen(command[2].split(' '), stdout=subprocess.PIPE)

    stdout, stderr = process3.communicate()
    spotinfo_string = stdout.decode('utf-8')
    spotinfo_list = [row.split(',') for row in spotinfo_string.split('\n')]
    return spotinfo_list

# Collect SPS Data From EC2

def get_sps(cred=credentials, workload=user_workload):
    results_list = []
    for i in range(len(workload)):
        user = cred.iloc[i]
        access_id = user['AccessKeyId']
        secret_key = user['SecretAccessKey']
        user_num = cred.index[i] + 2

        session = boto3.session.Session(
                aws_access_key_id=access_id,
                aws_secret_access_key=secret_key)
        ec2 = session.client('ec2', region_name='us-west-2')
        
        for query in workload[i]:
            response = ec2.get_spot_placement_scores(
                    InstanceTypes=[query[0]],
                    TargetCapacity=1,
                    SingleAvailabilityZone=True,
                    RegionNames=query[1])
            scores = response['SpotPlacementScores']
            results = ({'user_num': user_num}, query[0], query[1], scores)
            results_list.append(results)

    return results_list

# Collect SpotPrice by AZ

def get_az_price():
    session = boto3.session.Session()
    
    regions = get_regions(session)
    
    end_date = datetime.utcnow().replace(microsecond=0)
    start_date = end_date - timedelta(minutes=10)
    result = dict()
    
    buffers = []
    args = [(session, region, start_date, end_date) for region in regions]

    az_map = dict()
    for region in regions:
        print(region)
        ec2 = session.client('ec2', region_name=region)
        response = ec2.describe_availability_zones()

        for val in response['AvailabilityZones']:
            if val['RegionName'] in az_map:
                az_map[val['RegionName']][val['ZoneName']] = val['ZoneId']
            else:
                az_map[val['RegionName']] = {val['ZoneName'] : val['ZoneId']}
        time.sleep(1)
    print(az_map)

    for arg in args:
        buffer = store_spot_price(arg)
        for it in buffer.keys():
            if not (it in result):
                result[it] = {}
            for az, value in buffer[it].items():
                result[it][az_map[arg[1]][az]] = value
    
    return result

# Integrate Collected Spotinfo And SPS Data as DataFrame

def combine(now):
    now_time = time.strftime('%Y-%m-%d %H:%M:%S+00:00', now)
    spotinfo_list = get_spotinfo()
    sps_list = get_sps()
    az_price = get_az_price()

    sps_dict = {'InstanceType' : [],
            'Region' : [],
            'AvailabilityZoneId': [],
            'SPS' : [],
            'TimeStamp' : []}
    for sps in sps_list:
        for info in sps[3]:
            sps_dict['TimeStamp'].append(now_time)
            sps_dict['InstanceType'].append(sps[1])
            sps_dict['Region'].append(info['Region'])
            sps_dict['AvailabilityZoneId'].append(info['AvailabilityZoneId'])
            sps_dict['SPS'].append(info['Score'])

    spotinfo_dict = {'Region' : [],
            'InstanceType' : [],
            'vCPU' : [],
            'Memory GiB' : [],
            'Savings' : [],
            'IF' : [],
            'SpotPrice' : [],
            'TimeStamp' : []}
    for spotinfo in spotinfo_list[2:-1]:
        spotinfo_dict['TimeStamp'].append(now_time)
        spotinfo_dict['Region'].append(spotinfo[0])
        spotinfo_dict['InstanceType'].append(spotinfo[1])
        spotinfo_dict['vCPU'].append(spotinfo[2])
        spotinfo_dict['Memory GiB'].append(spotinfo[3])
        spotinfo_dict['Savings'].append(spotinfo[4])
        spotinfo_dict['IF'].append(spotinfo[5])
        spotinfo_dict['SpotPrice'].append(spotinfo[6])

    sps_df = pd.DataFrame(sps_dict)
    spotinfo_df = pd.DataFrame(spotinfo_dict)

    spotrank_df = sps_df.merge(spotinfo_df, how='inner', on=['InstanceType', 'Region'], suffixes=('_sps', '_spotinfo'))

    changed = 0
    for i in range(len(spotrank_df)):
        it = spotrank_df.iloc[i]['InstanceType']
        region = spotrank_df.iloc[i]['Region']
        az = spotrank_df.iloc[i]['AvailabilityZoneId']

        if i == 0:
            print(it, region, az)
        if it in az_price:
            if az in az_price[it]:
                spotrank_df.at[i, 'SpotPrice'] = az_price[it][az]['Linux/UNIX']['price']
                changed += 1
    print(changed)

    return spotrank_df

# Submit Batch To Timestream

def submit_batch(records, counter, recursive):
    if recursive == 10:
        sns_wrapper.publish_multi_message(topic, 'Timestream Upload error SNS',
                str(records), str(records), str(records))
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName = TABLE_NAME, Records=records, CommonAttributes={})
        print("Processed [%d] records. WriteRecords Status: [%s]" % (counter, result['ResponseMetadata']['HTTPStatusCode']))
    except write_client.exceptions.RejectedRecordsException as err:
        print("-------------------------------------------------------")
        print("RejectedRecords:", err)
        re_records = []
        for rr in err.response["RejectedRecords"]:
            print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
            re_records.append(records[rr["RecordIndex"]])
        print("Other records were written successfully")
        print("Re-Write failed records")
        submit_batch(re_records, counter, recursive + 1)
        print("-------------------------------------------------------")
    except Exception as err:
        print("Error:", err)
        exit()

# Check Database And Table Are Exist and Upload Data to Timestream

def upload_to_timestream(data):
    print("Creating Database")
    try:
        write_client.create_database(DatabaseName=DATABASE_NAME)
        print("Database [%s] created successfully." % DATABASE_NAME)
    except write_client.exceptions.ConflictException:
        print("Database [%s] exists. Skipping database creation" % DATABASE_NAME)
    except Exception as err:
        print("Create database failed:", err)

    print("Describing database")
    try:
        result = write_client.describe_database(DatabaseName=DATABASE_NAME)
        print("Database [%s] has id [%s]" % (DATABASE_NAME, result['Database']['Arn']))
    except write_client.exceptions.ResourceNotFoundException:
        print("Database doesn't exist")
    except Exception as err:
        print("Describe database failed:", err)

    # print("Updating database")
    # try:
    #     result = write_client.update_database(DatabaseName=DATABASE_NAME, KmsKeyId=KeyId)
    #     print("Database [%s] was updated to use kms [%s] successfully" % (DATABASE_NAME, result['Database']['KmsKeyId']))
    # except write_client.exceptions.ResourceNotFoundException:
    #     print("Database doesn't exist")
    # except Exception as err:
    #     print("Update database failed:", err)

    print("Creating table")
    retention_properties = {
            'MemoryStoreRetentionPeriodInHours': HT_TTL_HOURS,
            'MagneticStoreRetentionPeriodInDays': CT_TTL_DAYS
    }
    try:
        write_client.create_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, RetentionProperties=retention_properties)
        print("Table [%s] successfully created." % TABLE_NAME)
    except write_client.exceptions.ConflictException:
        print("Table [%s] exists on database [%s]. Skipping table creation" % (TABLE_NAME, DATABASE_NAME))
    except Exception as err:
        print("Create table failed:", err)

    print("Describing table")
    try:
        result = write_client.describe_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME)
        print("Table [%s] has id [%s]" % (TABLE_NAME, result['Table']['Arn']))
    except write_client.exceptions.ResourceNotFoundException:
        print("Table doesn't exist")
    except Exception as err:
        print("Describe table failed:", err)

    # print("Updating table")
    # retention_properties = {
    #     'MemoryStoreRetentionPeriodInHours': HT_TTL_HOURS,
    #     'MagneticStoreRetentionPeriodInDays': CT_TTL_DAYS
    # }
    # try:
    #     write_client.update_table(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, RetentionProperties=retention_properties)
    #     print("Table updated")
    # except Exception as err:
    #     print("Update table failed:", err)

    data = data[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS', 'IF', 'SpotPrice', 'Savings', 'TimeStamp_spotinfo']]
    data = data.rename({'AvailabilityZoneId': 'AZ', 'TimeStamp_spotinfo': 'TimeStamp'}, axis=1)

    print("Writing records")
    start_time = time.time()
    success_count = 0

    records = []
    counter = 0
    for idx, row in data.iterrows():
        time_value = str(row['TimeStamp']).split('+')[0]
        time_value = time.strptime(time_value, '%Y-%m-%d %H:%M:%S')
        time_value = time.mktime(time_value)
        time_value = str(int(round(time_value * 1000)))

        dimensions = []
        for column in ['InstanceType', 'Region', 'AZ', 'SPS', 'IF', 'SpotPrice', 'Savings']:
            dimensions.append({'Name':column, 'Value': str(row[column])})

        submit_data = {
                'Dimensions': dimensions,
                'MeasureName': 'SpotPrice',
                'MeasureValue': str(row['SpotPrice']),
                'MeasureValueType': 'DOUBLE',
                'Time': time_value
        }
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)

def update_latest(data):
    data_to_json = "["
    id_count = 0
    for idx, row in data.iterrows():
        id_count += 1
        data_to_json += '{'
        data_to_json += '\"id\":\"'+str(id_count)+'\",'
        data_to_json += '\"SpotPrice\":\"'+str(row['SpotPrice'])+'\",'
        data_to_json += '\"Savings\":\"'+str(row['Savings'])+'\",'
        data_to_json += '\"SPS\":\"'+str(row['SPS'])+'\",'
        data_to_json += '\"AZ\":\"'+str(row['AvailabilityZoneId'])+'\",'
        data_to_json += '\"Region\":\"'+str(row['Region'])+'\",'
        data_to_json += '\"InstanceType\":\"'+str(row['InstanceType'])+'\",'
        save_latest_if = 0
        if row['IF'] == '<5%':
            save_latest_if = 3.0
        elif row['IF'] == '5-10%':
            save_latest_if = 2.5
        elif row['IF'] == '10-15%':
            save_latest_if = 2.0
        elif row['IF'] == '15-20%':
            save_latest_if = 1.5
        else:
            save_latest_if = 1.0
        data_to_json += '\"IF\":\"'+str(save_latest_if)+'\",'
        data_to_json += '\"time\":\"'+str(row['TimeStamp_spotinfo'].split('+')[0])+'\"}'
        data_to_json += ','

    if data_to_json[-1] == ',':
        data_to_json = data_to_json[:len(data_to_json)-1] + ']'
    elif data_to_json[-1] == '[':
        data_to_json += ']'
    result = json.dumps(data_to_json)
    filename = 'latest_spot_data.json'
    s3_path = f'latest_data/{filename}'
    s3.Object(SAVE_BUCKET_NAME, s3_path).put(Body=result)
    object_acl = s3.ObjectAcl(SAVE_BUCKET_NAME, s3_path)
    response = object_acl.put(ACL='public-read')

if __name__ == "__main__":
    start = time.time()
    topic = sns_wrapper.create_topic(topic_name)
    # email_sub = sns_wrapper.subscribe(topic, 'email', email)

    now = time.gmtime(start)
    spotrank_df = combine(now)

    half = time.time()
    print(spotrank_df.iloc[:10])
    print("...")
    print("Data Collecting Time(s):", (half - start))

    upload_to_timestream(spotrank_df)

    update_latest(spotrank_df)

    end = time.time()
    print("\nTotal Time(s):", (end-start))

