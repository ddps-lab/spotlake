import pandas as pd
import numpy as np
import os

dir_path = "./"

for (root, directories, files) in os.walk(dir_path):
    for file in files:
        # 현재 위치에서 하위 디렉토리의 csv.gz의 파일에 경우
        if '.csv.gz' in file:
            file_path = os.path.join(root, file)
            # 압축을 하제하고 df으로 만든다.
            df = pd.read_csv(f'{file_path}', compression='gzip')
            # spotprice가 없는 행은 drop하고
            df = df.dropna(subset=['spotPrice'])
            # 사용하지 않는 vendor행을 drop한다.
            df = df.drop(columns=['vendor'], axis=1)

            # 반복문으로 instanceTier에 instanceType에 있는경우 nan값으로 바꾸어주고 instanceType에 값을 옮긴다.
            for index, row in df.iterrows():
                if not(row['instanceTier'] == 'Standard' or row['instanceTier'] == 'Basic'):
                    df.loc[index, 'instanceType'] = df.loc[index, 'instanceTier']
                    df.loc[index, 'instanceTier'] = np.nan
            # 다시 압축하여 저장한다.
            df.to_csv(f'{file_path}', compression='gzip')
