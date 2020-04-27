import os
import datetime
import sys, getopt

import pandas as pd
#from pandas import datetime as pddatetime
import numpy as np
import warnings

import csv

from statsmodels.tsa.ar_model import AR
from statsmodels.tsa.arima_model import ARMA,ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX

from queue import Queue
import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from ThreadSafeWriter import ThreadSafeWriter

warnings.simplefilter('ignore')

#Constants
CHUNK_SIZE = 5000000
THREAD_NUMBER = 75
ROWS_PER_RUN = 100


#Global variables
mainDf=None
dummyDf=None


def log(msg):
    print('[{}]: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),msg))


def parser(x):
    return datetime.datetime.strptime(x,'%Y%m')
    

def reduce_mem_usage(df):

    for col in df.columns:
        col_type = df[col].dtype
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            elif 'datetime64' in str(col_type):
                pass
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
    return df


def normalizeResult(yt,printIt=False):
    res=yt[0].tolist()
    for i in yt[2]:
        res=res+i.tolist()

    if printIt:
        print('---------------')
        tps=['Month 1','Month 2','Month 3','CI1M1','CI2M1','CI1M2','CI2M2','CI1M3','CI2M3']
        for i in range(0,len(tps)):
            print(tps[i],res[i])
    return res


def forecast(datafrm, dummydf,csvwriter):

    row=list(datafrm.index[0])
    
    res=datafrm.merge(dummydf,on='year_and_month',how='right')#.sort_values('year_and_month')

    res=res.sort_values(['year_and_month'])
    res['clicks']=res.clicks_x+res.clicks_y
    res['clicks'].fillna(0.0,inplace=True)

    res['queries']=res.queries_x+res.queries_y
    res['queries'].fillna(0.0,inplace=True)

    res['advertiser_spent']=res.advertiser_spent_x+res.advertiser_spent_y
    res['advertiser_spent'].fillna(0.0,inplace=True)

    del res['advertiser_spent_x']
    del res['advertiser_spent_y']
    del res['queries_x']
    del res['queries_y']
    del res['clicks_x']
    del res['clicks_y']

    res.reset_index(inplace=True)
    
    for valuetype in ['clicks','queries','advertiser_spent']:
        data = res[valuetype].values

        try:
            #print('------ ARMA ------')
            yhatARMA = ARMA(data, order=(1, 0)).fit(disp=False).forecast(3)
            row = row + normalizeResult(yhatARMA,False)
        except Exception as e:
            #print(valuetype,'arma',e)
            row = row + [0.0 for x in range(0,9)]

        try:
            #print('------ ARIMA ------')
            yhatARIMA=ARIMA(data, order=(1,0,0)).fit(disp=False).forecast(3)
            row = row + normalizeResult(yhatARIMA,False)
        except Exception as e:
            #print(valuetype,'ARIMA',e)
            row = row + [0.0 for x in range(0,9)]
        
        try:
            #print('------ SARIMAX ------')
            yhatSARIMAX=SARIMAX(data, order=(1,0,1), seasonal_order=(1,0,0,12)).fit(disp=False,maxiter=200, method='nm').forecast(3)
            row = row + yhatSARIMAX
        except Exception as e:
            #print(valuetype,'SARIMAX',e)
            row = row + [0.0 for x in range(0,3)]

    #print(row)
    #print(row[0:3])
    csvwriter.writerow(row)
    return row[0:4]


def startProcessing(filename,productCategory,startDate,endDate):
    global mainDf
    global dummyDf

    log('Opening file {} with chunk size = {}'.format(filename,CHUNK_SIZE))
    #df_chunk = pd.read_csv(filename,index_col=[0,1,2,3],parse_dates=[4], date_parser=parser, dtype={'zipcode': object}, chunksize=5000000)
    df_chunk = pd.read_csv(filename,index_col=[0,1,2,3],parse_dates=[4], date_parser=parser, chunksize=CHUNK_SIZE)
    chunk_list = []

    for dfc in df_chunk:
        df=dfc
        log('{} chunk size: {}'.format(CHUNK_SIZE,df.memory_usage().sum() / 1024**2))
        df=reduce_mem_usage(df)
        log('{} chunk size reduced: {}'.format(CHUNK_SIZE,df.memory_usage().sum() / 1024**2))
        chunk_list.append(df)

    mainDf=pd.concat(chunk_list)
    log('{} rows read'.format(mainDf.shape[0]))

    dummyDf=pd.DataFrame(pd.period_range(startDate, endDate, freq='M').strftime('%Y-%m'),columns=['year_and_month'])
    dummyDf.year_and_month=dummyDf.year_and_month.astype('datetime64[ns]',copy=False)
    dummyDf['clicks']=0.0
    dummyDf['queries']=0.0
    dummyDf['advertiser_spent']=0.0
    dummyDf.set_index('year_and_month',inplace=True)

    log('Dummy dataframe created')



    outputFilename='forecast_'+productCategory+'_predict.csv'

    flagWriteHeader=True

    if os.path.isfile(outputFilename):
        log('Output file already exists, continuing with previous job')
        log('Loading output file {}'.format(outputFilename))

        outputdf_chunk = pd.read_csv(outputFilename,index_col=[0,1,2,3],usecols=[0,1,2,3], chunksize=CHUNK_SIZE)
        outputchunk_list = []
        outputdf=None
        for dfc in outputdf_chunk:
            outputdf=dfc    

            log('{} chunk size: {}'.format(CHUNK_SIZE,outputdf.memory_usage().sum() / 1024**2))
            outputdf=reduce_mem_usage(outputdf)
            log('{} chunk size reduced: {}'.format(CHUNK_SIZE,outputdf.memory_usage().sum() / 1024**2))

            outputchunk_list.append(outputdf)
            
        outputdf=pd.concat(outputchunk_list)
        log('{} rows read from output file'.format(outputdf.shape[0]))
        log('Discarding already generated forecast from source dataframe')
        mainDf.drop(outputdf.index,inplace=True)
        log('Discard complete, main dataframe now has {} rows to be processed'.format(mainDf.shape[0]))
        flagWriteHeader=False


    log('Saving results in file {}'.format(outputFilename))
    csvFile = open(outputFilename, "at", newline="", encoding="utf-8")

    writer = ThreadSafeWriter(csvFile, quoting=csv.QUOTE_ALL)

    if flagWriteHeader:
        writer.writerow(['aw_publisher', 'zipcode', 'make_code', 'model_code',
                     'clicks_arma_m1','clicks_arma_m2','clicks_arma_m3',
                     'clicks_arma_lcim1','clicks_arma_ucim1',
                     'clicks_arma_lcim2','clicks_arma_ucim2',
                     'clicks_arma_lcim3','clicks_arma_ucim3',
                     'clicks_arima_m1','clicks_arima_m2','clicks_arima_m3',
                     'clicks_arima_lcim1','clicks_arima_ucim1',
                     'clicks_arima_lcim2','clicks_arima_ucim2',
                     'clicks_arima_lcim3','clicks_arima_ucim3',
                     'clicks_sarimax_m1','clicks_sarimax_m2','clicks_sarimax_m3',
                     'queries_arma_m1','queries_arma_m2','queries_arma_m3',
                     'queries_arma_lcim1','queries_arma_ucim1',
                     'queries_arma_lcim2','queries_arma_ucim2',
                     'queries_arma_lcim3','queries_arma_ucim3',
                     'queries_arima_m1','queries_arima_m2','queries_arima_m3',
                     'queries_arima_lcim1','queries_arima_ucim1',
                     'queries_arima_lcim2','queries_arima_ucim2',
                     'queries_arima_lcim3','queries_arima_ucim3',
                     'queries_sarimax_m1','queries_sarimax_m2','queries_sarimax_m3',
                     'advertiser_spent_arma_m1','advertiser_spent_arma_m2','advertiser_spent_arma_m3',
                     'advertiser_spent_arma_lcim1','advertiser_spent_arma_ucim1',
                     'advertiser_spent_arma_lcim2','advertiser_spent_arma_ucim2',
                     'advertiser_spent_arma_lcim3','advertiser_spent_arma_ucim3',
                     'advertiser_spent_arima_m1','advertiser_spent_arima_m2','advertiser_spent_arima_m3',
                     'advertiser_spent_arima_lcim1','advertiser_spent_arima_ucim1',
                     'advertiser_spent_arima_lcim2','advertiser_spent_arima_ucim2',
                     'advertiser_spent_arima_lcim3','advertiser_spent_arima_ucim3',
                     'advertiser_spent_sarimax_m1','advertiser_spent_sarimax_m2','advertiser_spent_sarimax_m3'])
        log('CSV headers written')

    log('Starting to schedule a maximum of {} forecast tasks, using {} threads/workers'.format(ROWS_PER_RUN,THREAD_NUMBER))
    with ThreadPoolExecutor(max_workers=THREAD_NUMBER) as executor:
        tasks = []
        for idx in mainDf.index:
            future = executor.submit(forecast,mainDf.loc[idx], dummyDf, writer)
            tasks.append(future)
            print('[{}]: {} tasks scheduled        \r'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),len(tasks)),end='')
            if len(tasks)%ROWS_PER_RUN==0:
                break
        print()
        
        log('Starting task execution')
        
        cont=1
        tasks_len=len(tasks)
        for task in as_completed(tasks):
            try:
                print('[{}]: Task {} of {}: {}        \r'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),cont,tasks_len,task.result()),end='')
                #log('{}: {}'.format(cont,task.result()))
                cont+=1
            except Exception as e:
                #raise 
                log(e)
                raise e
        print()
        csvFile.close()
        log('Forecast complete')



def main(argv):
    global CHUNK_SIZE
    global THREAD_NUMBER
    global ROWS_PER_RUN


    help_message='Usage: python3 clickForecasting.py [help] [--threads=n] [--chunksize=n] [--maxrows=n] <input gz file>'

    try:
        opts, args = getopt.getopt(argv, "--", ["help", "threads=", "maxrows=", "chunksize="])
        for opt, arg in opts:
            print(opt,arg)
            if opt == "--help":
                print(help_message)
                sys.exit()
            elif opt == "--threads":
                if not str.isnumeric(arg):
                    print('threads must be an integer greater than 0')
                    sys.exit()
                try:
                    THREAD_NUMBER = int(arg)
                    if THREAD_NUMBER<1:
                        print('threads must be an integer greater than 0')
                        sys.exit() 
                except Exception:
                    print('threads must be an integer greater than 0')
                    sys.exit()
            elif opt == "--maxrows":
                if not str.isnumeric(arg):
                    print('maxrows must be an integer greater than 0')
                    sys.exit()
                try:
                    ROWS_PER_RUN = int(arg)
                    if ROWS_PER_RUN<1:
                        print('maxrows must be an integer greater than 0')
                        sys.exit() 
                except Exception:
                    print('maxrows must be an integer greater than 0')
                    sys.exit()
            elif opt == "--chunksize":
                if not str.isnumeric(arg):
                    print('chunksize must be an integer greater than 0')
                    sys.exit()
                try:
                    CHUNK_SIZE = int(arg)
                    if CHUNK_SIZE<1:
                        print('chunksize must be an integer greater than 0')
                        sys.exit() 
                except Exception:
                    print('chunksize must be an integer greater than 0')
                    sys.exit()
    except getopt.GetoptError:
        print(help_message)
        sys.exit(2)
    
    if len(argv) < 1:
        print(help_message)
        sys.exit(2)

    if argv[0].lower()=='help':
        print(help_message)
        sys.exit(0)

    inputFile=argv[len(argv)-1]
    if not os.path.isfile(inputFile):
        print(inputFile,'is not a file!')
        sys.exit(2)

    filenameNoPath=os.path.basename(inputFile)
    filenameSplitted=filenameNoPath.split('_')
    if len(filenameSplitted)!=3:
        print(inputFile,'is not a valid filename!')
        sys.exit(2)

    if not str.isnumeric(filenameSplitted[1]):
        print(inputFile,'is not a valid filename!')
        sys.exit(2)

    aw_product_category=filenameSplitted[1]



    today=datetime.date.today()

    endDate=str(datetime.date(today.year,today.month,1)-datetime.timedelta(days=1))[:7]
    initDate=str(datetime.date(today.year-1,today.month,1))[:7]

    log('Started')
    print('\tParameters:')
    print('\t===========')
    print('\tFilename:\t\t',inputFile)
    print('\tProduct category:\t',aw_product_category)
    print('\tStart date:\t\t',initDate)
    print('\tEnd date:\t\t',endDate)
    print('\tThreads:\t\t',THREAD_NUMBER)
    print('\tMax Rows:\t\t',ROWS_PER_RUN)
    print('\tChunk Size:\t\t',CHUNK_SIZE)

    startProcessing(inputFile,aw_product_category,initDate,endDate)







if __name__ == "__main__":
	main(sys.argv[1:])
    





