import geopandas
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

pd.set_option('display.expand_frame_repr', False)


def data_together(filepath):
    """
    :param filepath: all the csv files with raw data
    :return:
    """
    csvs = []
    dfs = []

    for subdir, dirs, files in os.walk(filepath):
        for file in files:
            filepath = subdir + os.sep + file
            if filepath.endswith(".csv"):
                csvs.append(filepath)

    for f in csvs:
        temp = pd.read_csv(f)

        # temp['datetime'] = pd.to_datetime(temp['datetime'], dayfirst=True)
        # temp.set_index('datetime', inplace=True)

        temp.index = pd.to_datetime(temp['datetime'], dayfirst=True, utc=True).dt.strftime('%Y-%m-%d %H:%M')
        temp.drop('datetime', axis=1, inplace=True)
        dfs.append(temp)

    return dfs, csvs



def save_chosen_station(df_list, path_list, stationid):

    for single_df, single_path in zip(df_list, path_list):
        df_chosen = single_df[single_df.site_uid == stationid].copy()
        # df_chosen['datetime'] = pd.to_datetime(df_chosen['datetime'])
        # df_chosen.set_index('datetime', inplace=True)
        df_chosen.drop('site_uid', axis=1, inplace=True)
        df_chosen.drop('packet_uid', axis=1, inplace=True)
        filename, file_extension = os.path.splitext(single_path)
        outputcsv = os.path.join(filename + '_' + stationid + '.csv')
        df_chosen.to_csv(outputcsv)


def mergeSameStationList(dflist,filepath):

    newone = pd.merge(dflist[0], dflist[1], left_index=True,right_index=True, how='inner')

    remainlist = []
    for df in dflist[2:]:
        remainlist.append(df)

    for i in range(0,len(remainlist)):
        newone = pd.merge(newone, remainlist[i], left_index=True,right_index=True, how='inner')

    # newone['Stationname'] = filedir[-1]
    # newone['datetime'] = pd.to_datetime(newone['datetime'], dayfirst=True)
    # newone.set_index('datetime', inplace=True)


    # output CSV
    filename, file_extension = os.path.splitext(filepath)
    outputcsv = os.path.join(filename + '_joined' + '.csv')
    newone.to_csv(outputcsv, date_format='%Y-%m-%dT%H:%M')


def resampleCSV(df,filepath):
    # df['datetime'] = pd.to_datetime(df['datetime'], dayfirst=True)   ##use day first for GOV download csv
    # df.set_index('datetime',inplace=True)

    df.index = pd.to_datetime(df.index, dayfirst=True)
    df[df < 0] = 0
    df.replace(0, np.nan, inplace=True)


    newcsv = df.resample('60Min').mean()
    # newcsv = newcsv.interpolate(method='linear', axis=0).bfill()
    newcsv.replace(0, np.nan, inplace=True)
    

    newcsv.drop(['ph'], axis=1, inplace=True)
    newcsv.drop(['diss_oxy_con'], axis=1, inplace=True)
    newcsv.drop(['chloro_con'], axis=1, inplace=True)

    newcsv['temp_water'] = newcsv['temp_water'].rolling(window=12).mean()
    # newcsv['ph'] = newcsv['ph'].rolling(window=12).mean()
    newcsv['spec_cond'] = newcsv['spec_cond'].rolling(window=12).mean()
    # newcsv['diss_oxy_con'] = newcsv['diss_oxy_con'].rolling(window=12).mean()
    # newcsv['chloro_con'] = newcsv['chloro_con'].rolling(window=12).mean()
    newcsv['nitrate_con'] = newcsv['nitrate_con'].rolling(window=12).mean()


    print(newcsv.describe())
    # filedir, name = os.path.split(filepath)
    filename, file_extension = os.path.splitext(filepath)
    # outputcsv = os.path.join(filedir, name + '_resample' + '.csv')
    outputcsv = os.path.join(filename + '_resample' + '.csv')
    newcsv.to_csv(outputcsv,date_format='%Y-%m-%dT%H:%M')



if __name__ == '__main__':
    # # datapath = r'C:\Users\ZHA244\Downloads\test'
    # datapath = r'C:\Users\ZHA244\Downloads\all'
    # alldata,allpaths = data_together(datapath)
    # station_id = 'WQS0004'
    # save_chosen_station(alldata,allpaths, station_id)

    # datapath = r'C:\Users\ZHA244\Downloads\chosen_data\WQS0004\5'
    # alldata,allpaths = data_together(datapath)
    # mergeSameStationList(alldata,datapath)
    #
    datapath = 'data/USA'
    alldata,allpaths = data_together(datapath)

    for i,j in zip(alldata,allpaths):
        resampleCSV(i,j)
    #
    # datapath = r'C:\Users\ZHA244\Downloads\chosen_data\join_two_stations\8'
    # alldata,allpaths = data_together(datapath)
    # mergeSameStationList(alldata,datapath)


    # statistics

    # datapath = r'C:\Users\ZHA244\Coding\Pytorch_based\Dual-Head-SSIM\data'
    # alldata,allpaths = data_together(datapath)
    #
    # df_all = pd.concat([alldata[0],alldata[1],alldata[2],alldata[3],alldata[4],], axis=0)
    #
    # print(df_all.describe())

    # # statistics single location
    # datapath = r'C:\Users\ZHA244\Coding\Pytorch_based\Bias_Attention\data\iowa'
    # alldata,allpaths = data_together(datapath)
    #
    # df_all = pd.concat([alldata[0],alldata[1]], axis=0)
    #
    # print(df_all.describe())












### geopandas
# path = r'C:\Users\ZHA244\Downloads\shapefile_site_2018'
# df = geopandas.read_file(path)
# print(df)
# ax = df.plot(figsize=(10, 10), alpha=0.5, edgecolor='k')
# plt.show()

#
# folder_path = r'C:\Users\ZHA244\Downloads\test'


# ## combin and regroup IOWA water quality data
#
# df_all, _ = data_together(folder_path)
#
# print(len(df_all))
#
#
# # try 1
#
# print(df_all[0].describe())
#
# df_new = df_all[0][df_all[0].site_uid=='WQS0002']
#
# print(df_new.describe())
#
#
#
#
#
#
#
# for i in df_all:
#     print(i.head(1))







