import pandas as pd

def compare(pervious_df, current_df):
    changed_list = []
    for idx, row in current_df.iterrows():
        cmp = pervious_df[((row['InstanceType']==pervious_df['InstanceType']) & 
                         (row['Region']==pervious_df['Region']) & 
                         (row['AZ']==pervious_df['AZ']))]
        if len(cmp) == 0:
            changed_list.append(dict(row))
            continue
        cmp = cmp.iloc[0]
        if cmp['SPS']!=row['SPS'] or cmp['IF']!=row['IF'] or cmp['SpotPrice']!=row['SpotPrice']:
            changed_list.append(dict(row))
    changed_df = pd.DataFrame(changed_list)
    return changed_df
    