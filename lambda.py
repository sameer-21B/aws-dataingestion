from datetime import datetime as d
from datetime import date
import boto3 as b3
from io import StringIO
import pandas as pd
import numpy as np
import pickle
import json

def lambda_handler(event, context):
    '''This function retreives files from s3 and dumps date wise files to s3'''
    #TODO implement
    s3_client=b3.client('s3')
    S3_BUCKET_NAME='aws-test01-bucket'
    OBJECT_KEY='ecom_data.csv'
    file_content=s3_client.get_object(Bucket=S3_BUCKET_NAME,Key=OBJECT_KEY)['Body'].read().decode('utf-8')
    fc='Idx'+file_content
    testdata=StringIO(fc)
    data=pd.read_csv(testdata,sep=",")
    data.reset_index(drop=True,inplace=True)
    data.InvoiceDate=pd.to_datetime(data.InvoiceDate)
    data.InvoiceDate=data.InvoiceDate.apply(lambda x:x.date())
    data=data[data.InvoiceDate == d.strptime(str(event['year'])+'-'+str(event['month'])+'-'+str(date.today().strftime(r"%d")),
    r"%Y-%m-%d").date()]
    csv_buffer = StringIO()
    data.to_csv(csv_buffer)
    s3_resource = b3.resource('s3')
    s3_resource.Object(S3_BUCKET_NAME, 'ecom_data_daily/ecom_data_'+str(event['year'])+'_'+str(event['month'])+'_'+date.today().strftime(r"%d")+'.csv').put(Body=csv_buffer.getvalue())
    return 'Lambda function completed.'