import pandas as pd
import numpy as np

import pymysql

conn = pymysql.connect(host='host.com', port=3306, user='user', passwd='bla', db='dtb')

df = pd.read_sql_query("""
select j.contractor_id, j.task_id, j.address_id, j.id job_id,
       (p2.rowAddedDate) after_time, (p1.rowAddedDate) before_time
from taskeasy.Job j
join taskeasy.Photo p1 on j.id = p1.job_id and p1.photoType='JOB_BEFORE'
join taskeasy.Photo p2 on j.id = p2.job_id and p2.photoType='JOB_AFTER'
group by j.contractor_id, j.task_id, j.address_id, j.id
""",conn)

df













import json
import boto3
import csv, io
from datetime import datetime, timedelta, date
import random
import time
from apiclient.errors import HttpError

VIEW_ID = '51865733'

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'ga-te-test-dd2557ae0564.json'
VIEW_ID = '51865733'

credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
analytics = build('analyticsreporting', 'v4', credentials=credentials)
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

def get_batch(reportbody):
  return analytics.reports().batchGet(
      body=reportbody
  ).execute()

def log(msg):
    print(datetime.today().strftime('[%Y/%m/%d %H:%M:%S]: '+msg))

def get_report(reportName, body, endDate):
    log('{0} starting'.format(reportName))
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)

    #rowCount nextPageToken
    header=[]
    header.clear()
    
    
    for n in range(0, 10):
        try:
        
            flag_repeat = True
            while flag_repeat:

                resp = get_batch(body)

                if len(header) == 0:
                    for dim in resp["reports"][0]['columnHeader']["dimensions"]:
                        header.append(dim[3:])

                    for mhdr in resp["reports"][0]['columnHeader']["metricHeader"]["metricHeaderEntries"]:
                        header.append(mhdr["name"][3:])

                    writer.writerow(header)

                row=[]
                if 'rows' in resp["reports"][0]["data"]:
                    for rw in resp["reports"][0]["data"]["rows"]:
                        row.clear()
                        for dm in rw['dimensions']:
                            row.append(dm)

                        for mt in rw['metrics'][0]['values']:
                            row.append(mt)
                        writer.writerow(row)
                    if 'nextPageToken' in resp['reports'][0]:
                        log('Read {nextPageToken} rows of {rowCount}'.format(nextPageToken=resp['reports'][0]['nextPageToken'],rowCount=resp["reports"][0]["data"]['rowCount']))
                        body['reportRequests'][0]['pageToken']=resp['reports'][0]['nextPageToken']
                    else:
                        log('Read the last of {rowCount} rows'.format(rowCount=resp["reports"][0]["data"]['rowCount']))
                        flag_repeat = False
                        if 'pageToken' in body['reportRequests'][0]:
                            del body['reportRequests'][0]['pageToken']
                    #
                    #time.sleep(1)

            if 'rows' in resp["reports"][0]["data"]:
                filename = 'google_analytics/'+reportName+"/"+reportName + endDate.strftime('_%Y%m%d')+'.csv'
                log('Uploading to S3 as {filename}'.format(filename=filename))
                s3resp=s3.Object('google-analytics-bucket', filename).put(Body=output.getvalue())
                log('Uploaded to S3 with status code {HTTPStatusCode} and {RetryAttempts} retry attempts'.format(HTTPStatusCode=s3resp['ResponseMetadata']['HTTPStatusCode'],RetryAttempts=s3resp['ResponseMetadata']['RetryAttempts']))
                log('{0} ended'.format(reportName))
            else:
                log('No data for {0} on {1}, skipping file upload.'.format(reportName,endDate.strftime('_%Y%m%d')))
            return True

        except (HttpError) as error:
            log('Error in {nn} try {error}'.format(nn=n,error=error))
            log('Reason is: {reason}'.format(reason=error.resp.reason))
            #if error.resp.reason in ['userRateLimitExceeded', 'quotaExceeded','internalServerError', 'backendError']:
            if n<9:
                time.sleep((3 ** n) + random.random())
            else:
                log("There has been an error, the request never succeeded")
                break
        else:
            break
























a = date(2018,11,1)
#a = date.today()
b = date(2018,10,19)

totaldays=(a-b).days

c=(date.today()-b).days


data = json.load(open('requests.json'))


for request in data['requests']:
    request['request']['reportRequests'][0]['viewId']=VIEW_ID
    
for i in range(-c,(-c+totaldays)):

    
    startDate=datetime.today()-timedelta(days=-1*i)
    endDate=datetime.today()-timedelta(days=-1*i)
    
    log('-- Getting data for {date} --'.format(date=startDate.strftime('%Y-%m-%d')))
    
    for request in data['requests']:
        request['request']['reportRequests'][0]['dateRanges']=[{'startDate': startDate.strftime('%Y-%m-%d'), 'endDate': endDate.strftime('%Y-%m-%d')}]
        #if request['name'] == 'ecommerce':
        jsonresp = get_report(request['name'],request['request'],endDate)

    log('-- Process finished for {date} --'.format(date=endDate.strftime('%Y-%m-%d')))
        
        
