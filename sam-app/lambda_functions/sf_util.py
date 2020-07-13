"""
You must have an AWS account to use the Amazon Connect CTI Adapter.
Downloading and/or using the Amazon Connect CTI Adapter is subject to the terms of the AWS Customer Agreement,
AWS Service Terms, and AWS Privacy Notice.

© 2017, Amazon Web Services, Inc. or its affiliates. All rights reserved.

NOTE:  Other license terms may apply to certain, identified software components
contained within or distributed with the Amazon Connect CTI Adapter if such terms are
included in the LibPhoneNumber-js and Salesforce Open CTI. For such identified components,
such other license terms will then apply in lieu of the terms above.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
from datetime import datetime, timedelta
import base64
import json
import boto3
import os

logger = logging.getLogger()

def parse_date(value, date=datetime.now()):
    if "|" not in value:
        return value

    value_raw = value.split("|")
    delta_raw = value_raw[0].strip()
    format_raw = value_raw[1].strip()
    delta = timedelta()

    if format_raw not in formats:
      msg = "Supported formats are 'date', 'time' and 'datetime', example '2h|date'"
      logger.error(msg)
      raise Exception(msg)

    if len(delta_raw) > 0:
      delta_value = delta_raw[:-1] 
      delta_type = delta_raw[-1].lower()
      
      if delta_type not in timedeltas:
        msg = "Supported delta types are 'd' for days, 'h' for hours and 'm' for minutes, example '2h|date'"
        logger.error(msg)
        raise Exception(msg)
      
      delta = timedeltas[delta_type](delta_value)

    return (date+delta).strftime(formats[format_raw])

def split_bucket_key(location):
  bucketIndex = location.index('/')
  bucket = location[:bucketIndex]
  key = location[bucketIndex+1:]
  return (bucket, key)

def get_arg(kwargs, name):
  if name not in kwargs:
    msg = "'%s' enviroment variable is missing"
    logger.error(msg)
    raise Exception(msg)
  return kwargs[name]

formats = {
    "datetime":"%Y-%m-%dT%H:%M:%SZ",
    "date":"%Y-%m-%d",
    "time":"%H:%M:%S"
}

timedeltas = {
    "w":lambda v: timedelta(weeks=int(v)),
    "d":lambda v: timedelta(days=int(v)),
    "h":lambda v: timedelta(hours=int(v)),
    "m":lambda v: timedelta(minutes=int(v))
}

def split_s3_bucket_key(s3_path):
    """Split s3 path into bucket and key prefix.
    This will also handle the s3:// prefix.
    :return: Tuple of ('bucketname', 'keyname')
    """
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    return find_bucket_key(s3_path)

def find_bucket_key(s3_path):
    """
    This is a helper function that given an s3 path such that the path is of
    the form: bucket/key
    It will return the bucket and the key represented by the s3 path
    """
    s3_components = s3_path.split('/')
    bucket = s3_components[0]
    s3_key = ""
    if len(s3_components) > 1:
        s3_key = '/'.join(s3_components[1:])
    return bucket, s3_key

def getBase64fileFromS3(path):
    s3 = boto3.client('s3')
    bucket_name, key_name = split_s3_bucket_key(path)
    response = s3.get_object(Bucket=bucket_name, Key=key_name)
    recObj = response['Body'].read()
    encodedBytes = base64.b64encode(recObj)
    encodedStr = str(encodedBytes, "utf-8")
    logger.info('S3 file retrieved')
    return encodedStr

def getS3FileMetadata(Bucket, ContactId):
    oMetadata = {}
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=Bucket, Key='locks/' + ContactId + '.lock')
    logger.info('GetS3FileMetadata Response: %s ' % response)
    if 'Metadata' in response:
        oMetadata = response['Metadata']
    return oMetadata

def getS3FileJSONObject(bucket, key):
    s3=boto3.resource('s3')
    fileObj = s3.Object(bucket, key)
    fileBody = fileObj.get()['Body'].read()
    return json.loads(fileBody)
  
def attachFileSaleforceObject(objName, objContentType, objDescription, objParentId, objBody):

    sfRequest = {'Details' : {'Parameters':{}}}
    sfRequest['Details']['Parameters']['sf_operation'] = 'create'
    sfRequest['Details']['Parameters']['sf_object'] = 'Attachment'
    sfRequest['Details']['Parameters']['ContentType'] = objContentType
    sfRequest['Details']['Parameters']['Description'] = objDescription
    sfRequest['Details']['Parameters']['Name'] = objName
    sfRequest['Details']['Parameters']['ParentId'] = objParentId
    sfRequest['Details']['Parameters']['Body'] = objBody

    return invokeSfAPI(sfRequest)

def invokeSfAPI(sfRequest):
    sfLambdaClient = boto3.client('lambda')

    sfLambdaResponse = sfLambdaClient.invoke(FunctionName = os.environ['SFDC_INVOKE_API_LAMBDA'], InvocationType='RequestResponse', Payload=json.dumps(sfRequest))
    if(sfLambdaResponse['StatusCode']==200):
        responsePayload = sfLambdaResponse['Payload'].read()
        responsePayload = responsePayload.decode('utf8')
        logger.info('SF API Lambda Response %s' %responsePayload)
        responsePayload = json.loads(responsePayload)
        if 'errorMessage' in responsePayload:
            raise ValueError('Error SFDC Lambda: ' + json.dumps(responsePayload))
        return(responsePayload)
    else:
        raise ValueError('Error SFDC Lambda: ' + str(sfLambdaResponse['StatusCode']))

def getBase64String(iObject):
    sObject = iObject
    if not isinstance(iObject, str):
        sObject = json.dumps(iObject)
    encodedBytes = base64.b64encode(sObject.encode("utf-8"))
    encodedStr = str(encodedBytes, "utf-8")
    return encodedStr