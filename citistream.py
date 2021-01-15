# #! /bin/python


# ''' ******************************************************************************
        # PROGRAM_NAME:  citistrean.py
        # AUTHOR          :       Hem Lohani
        # DESCRIPTION     :       This lambda script  gets data from API and push to the kinesis data stream
        # ARGUMENTS:
                # -e | --
               
               
        # REVISION HISTORY:
        # DATE            AUTHOR                          REASON FOR CHANGE
        # ----------------------------------------------------------------------------
        # 20/12/2020      Hem Lohani                   US: Created
		# 
        # ----------------------------------------------------------------------------

import boto3
import json
from datetime import datetime
import calendar
import random
import time
import requests

citidata = ''
params = {
    'access_key': '006a0d9ea7917b3671d3802602ae3bf8'

      }

api_result = requests.get('http://api.aviationstack.com/v1/cities', params)
api_statuscode =  api_result.status_code 
api_response = api_result.json()
citidata = api_response['data']
v_streamname = 'citistream'

kinesis_client = boto3.client('kinesis', region_name='us-east-2')
def put_to_stream(thing_id, property_value, property_timestamp):
    payload = {
                'prop': citidata,
                'timestamp': str(property_timestamp),
                'thing_id': thing_id
              }

    print ('Data Loaded from the Source')

    put_response = kinesis_client.put_record(
                        StreamName=v_streamname,
                        Data=json.dumps(payload),
                        PartitionKey=thing_id)
def lambda_handler(event, context):  
       
    property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    thing_id = 'aa-bb'
    put_to_stream(thing_id,citidata,property_timestamp)
    return_response = 'Citis API Status Code: ',api_statuscode 
    return  {
             'statuscode': 200,
            'body': json.dumps(return_response)
        }


