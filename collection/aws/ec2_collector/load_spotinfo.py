import pandas as pd
import os
import pickle
import subprocess


LOCAL_PATH = '/home/ubuntu/spot-score/collection/aws/ec2_collector'


# get info of interrupt frequency at spotinfo
def get_spotinfo():
    # need to change file location
    if 'spotinfo' in os.listdir(f'{LOCAL_PATH}/'):
        command = ['', '', f'{LOCAL_PATH}/spotinfo --output csv --region all']
    else:
        command = [f'wget https://github.com/alexei-led/spotinfo/releases/download/1.0.7/spotinfo_linux_amd64 -O {LOCAL_PATH}/spotinfo', f'chmod +x {LOCAL_PATH}/spotinfo', f'{LOCAL_PATH}/spotinfo --output csv --region all']
    
        process1 = subprocess.Popen(command[0].split(' '), stdout=subprocess.PIPE)
        process1.communicate()
        process2 = subprocess.Popen(command[1].split(' '), stdout=subprocess.PIPE)
        process2.communicate()
    
    process3 = subprocess.Popen(command[2].split(' '), stdout=subprocess.PIPE)

    stdout, stderr = process3.communicate()
    spotinfo_string = stdout.decode('utf-8')
    spotinfo_list = [row.split(',') for row in spotinfo_string.split('\n')]

    spotinfo_dict = {'Region' : [],
                     'InstanceType' : [],
                     'IF' : []}
    
    # remove column name from data using indexing
    for spotinfo in spotinfo_list[2:-1]:
        spotinfo_dict['Region'].append(spotinfo[0])
        spotinfo_dict['InstanceType'].append(spotinfo[1])
        spotinfo_dict['IF'].append(spotinfo[5])
    
    spotinfo_df = pd.DataFrame(spotinfo_dict)

    frequency_map = {'<5%': 3.0, '5-10%': 2.5, '10-15%': 2.0, '15-20%': 1.5, '>20%': 1.0}
    spotinfo_df = spotinfo_df.replace({'IF': frequency_map})

    return spotinfo_df
    