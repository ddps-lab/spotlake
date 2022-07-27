import pandas as pd
import os
import pickle
import subprocess


# get info of interrupt frequency at spotinfo
def get_spotinfo():
    # need to change file location
    if 'spotinfo' in os.listdir('./'):
        command = ['', '', './spotinfo --output csv --region all']
    else:
        command = ['wget https://github.com/alexei-led/spotinfo/releases/download/1.0.7/spotinfo_linux_amd64 -O spotinfo', 'chmod +x spotinfo', './spotinfo --output csv --region all']
    
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
                     'vCPU' : [],
                     'Memory GiB' : [],
                     'Savings' : [],
                     'IF' : [],
                     'SpotPrice' : []}
    
    # remove column name from data using indexing
    for spotinfo in spotinfo_list[2:-1]:
        spotinfo_dict['Region'].append(spotinfo[0])
        spotinfo_dict['InstanceType'].append(spotinfo[1])
        spotinfo_dict['vCPU'].append(spotinfo[2])
        spotinfo_dict['Memory GiB'].append(spotinfo[3])
        spotinfo_dict['Savings'].append(spotinfo[4])
        spotinfo_dict['IF'].append(spotinfo[5])
        spotinfo_dict['SpotPrice'].append(spotinfo[6])
    
    spotinfo_df = pd.DataFrame(spotinfo_dict)

    return spotinfo_df
    