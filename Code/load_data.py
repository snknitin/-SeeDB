import pandas as pd
import os
import numpy as np
import time

DATA_PATH=os.path.join(os.path.dirname(os.getcwd()),"Data/splits/")
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)


class Solution(object):
    def __init__(self,splits):

        self.dataframe_all=pd.read_csv(os.path.join(os.path.dirname(os.getcwd()) ,"Data/processed.txt"), sep=",")
        # self.dataframe_all.columns = ["age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation", "relationship",
        #     "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country", "economic_indicator"]
        self.splits= splits

    def split_data(self):
        ''' Partitions the data into splits and saves them as csv files '''
        t0 = time.time()

        df_split= np.array_split(self.dataframe_all,self.splits)
        for i in range(1,len(df_split)+1):
            df_split[i-1].to_csv(os.path.join(DATA_PATH ,"test_{}.csv".format(i)),encoding='utf-8', index=False)

        t1 = time.time()
        print('Time taken by this process: %0.2f'%(t1 - t0))



if __name__=="__main__":
    s=Solution(10)
    s.split_data()

