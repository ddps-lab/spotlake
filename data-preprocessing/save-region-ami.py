import boto3
import pickle

region_list_default = ['us-east-2', 'ap-south-1', 'us-west-2', 'ap-northeast-3', 'ap-southeast-1', 'ap-northeast-2', 'ca-central-1', 'eu-central-1', 'eu-west-1', 'eu-west-2', 'eu-west-3', 'eu-north-1', 'sa-east-1', 'ap-northeast-1', 'ap-southeast-2', 'us-east-1', 'us-west-1']
region_ami_dict = {}
region_ami_x86 = {}
region_ami_arm = {}

for region in region_list_default:
    print(region, end=' ')
    session = boto3.session.Session(profile_name='dev-session')
    client = session.client('ec2', region_name=region)

    response = client.describe_images(Owners=['amazon'],
                                      Filters=[{
                                          'Name': 'name',
                                          'Values': ['amzn*']}])
    
    sorted_images = sorted(response['Images'], key=lambda i: i['CreationDate'], reverse=True)
    for ami in sorted_images:
        try:
            desc = ami['Description']
            ami_id = ami['ImageId']
            if ('Amazon Linux 2 Kernel 5.10 AMI' in desc) and ('x86_64 HVM gp2' in desc):
                region_ami_x86[region] = (ami_id, ami)
                break
        except:
            continue
    for ami in sorted_images:
        try:
            desc = ami['Description']
            ami_id = ami['ImageId']
            if ('Amazon Linux 2 LTS Arm64 Kernel' in desc) and ('arm64 HVM gp2' in desc):
                region_ami_arm[region] = (ami_id, ami)
                break
        except:
            continue

region_ami_dict['x86'] = region_ami_x86
region_ami_dict['arm'] = region_ami_arm

print('Done!')
pickle.dump(region_ami_dict, open('region_ami_dict.pkl', 'wb'))
