import boto3
import pandas
import pickle


# get spot placement score
def get_sps():
    # need to change file location
    credentials = pickle.load(open('./user_cred_df.pkl', 'rb'))
    workload = pickle.load(open('./bin_packed_workload.pickle', 'rb'))
    sps_list = []

    for i in range(len(workload)):
        user = credentials.iloc[i]
        access_id = user['AccessKeyId']
        secret_key = user['SecretAccessKey']
        # credentials start with spotrank+002@kookmin.ac.kr at index 0
        user_num = credentials.index[i] + 2

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
            sps_list.append(results)

    sps_dict = {'InstanceType' : [],
                'Region' : [],
                'AvailabilityZoneId': [],
                'SPS' : []}

    for sps in sps_list:
        for info in sps[3]:
            sps_dict['InstanceType'].append(sps[1])
            sps_dict['Region'].append(info['Region'])
            sps_dict['AvailabilityZoneId'].append(info['AvailabilityZoneId'])
            sps_dict['SPS'].append(info['Score'])

    sps_df = pd.DataFrame(sps_dict)

    return sps_df
