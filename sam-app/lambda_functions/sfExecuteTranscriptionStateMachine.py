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

import boto3
import botocore
import os
import json
import base64
import uuid
from sf_util import split_s3_bucket_key, invokeSfAPI
import logging
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))


def process_record(record):
    #for record in records:
    decoded_payload = base64.b64decode(record).decode('utf-8')
    recordObj = json.loads(decoded_payload)
    logger.info('DecodedPayload: {}'.format(recordObj))

    # ignore all Kinesis events that don't contain a contact
    if 'ContactId' not in recordObj:
        logger.info('No contact in record; returning')
        return
    
    #check if CTR already locked, and proceed if not locked
    if not checkLockCTR(recordObj['ContactId']):
        if lockCTR(recordObj['ContactId'], recordObj["Attributes"]):
            if('Attributes' in recordObj and 'Recording' in recordObj and recordObj["Recording"]!=None and 'Location' in recordObj["Recording"] and recordObj["Recording"]["Status"]=='AVAILABLE' and  recordObj["Recording"]["Type"]=='AUDIO'):
                #check if postcallRecordingImportEnabled then import call recording file into Salesforce
                if('postcallRecordingImportEnabled' in recordObj["Attributes"] and recordObj["Attributes"]["postcallRecordingImportEnabled"]=='true'):
                    logger.info('postcallRecordingImportEnabled = true')
                    createACContactChannelAnalyticsSalesforceObject(recordObj['ContactId'], recordObj['Recording']['Location'])
                #check if postcallTranscribeEnabled then start the transcribing process
                if('postcallTranscribeEnabled' in recordObj["Attributes"] and recordObj["Attributes"]["postcallTranscribeEnabled"]=='true' and "postcallTranscribeLanguage" in recordObj["Attributes"]):
                    executeStateMachine(recordObj['Recording']['Location'], recordObj['ContactId'], recordObj["Attributes"]["postcallTranscribeLanguage"])



def executeStateMachine(s3_object, contactId, languageCode):
    try:
        execution_input = {
          "jobName": contactId +"_"+uuid.uuid4().hex, #RANDOMNESS
          "mediaFormat": os.environ["MEDIA_FORMAT"],
          "fileUri": "https://s3.amazonaws.com/"+s3_object,
          "languageCode": languageCode,
          "transcriptDestination": os.environ["TRANSCRIPTS_DESTINATION"],
          "outputEncryptionKMSKeyId": os.environ["TRANSCRIPTS_DESTINATION_KMS"],
          "wait_time": os.environ["WAIT_TIME"],
          "settings" : {"ChannelIdentification" : True}
        }
        client = boto3.client('stepfunctions')
        logger.info('Starting Transcribe State Machine: %s' % execution_input)
        response = client.start_execution(
            stateMachineArn=os.environ['TRANSCRIBE_STATE_MACHINE_ARN'],
            input=json.dumps(execution_input)
        )
        logger.info('Transcribe State Machine Response: {}'.format(response))
    except Exception as e:
        logger.error('Error: {}'.format(e))
        logger.error('Current data: {}'.format(execution_input))


def lockCTR(ContactId, Attributes):
    try:
        s3 = boto3.resource('s3')
        oMetadata = {}
        
        if 'postcallTranscribeComprehendAnalysis' in Attributes:
            oMetadata['postcallTranscribeComprehendAnalysis'] = Attributes['postcallTranscribeComprehendAnalysis']
        logger.info('Locking CTR: {}'.format(ContactId))
        s3.Object(os.environ["TRANSCRIPTS_DESTINATION"], 'locks/' + ContactId + '.lock').put(Body='IN_PROGRESS', Metadata=oMetadata)
        logger.info('CTR Locked: {}'.format(ContactId))
        return True
    except Exception as e:
        logger.error('Error lock: {}'.format(e))
        logger.error('Current data: {}'.format(ContactId))
        raise e

def checkLockCTR(ContactId):
    try:
        s3 = boto3.resource('s3')
        logger.info('Checking if CTR locked: {}'.format(ContactId))
        s3Object = s3.Object(os.environ["TRANSCRIPTS_DESTINATION"], 'locks/' + ContactId + '.lock').load()
    except botocore.exceptions.ClientError as e:
        if int(e.response['Error']['Code']=='404'):
            logger.info('CTR not locked: {}'.format(ContactId))
            return False
        else:
            logger.error('Error checkLock: {}'.format(e))
            logger.error('Current data: {}'.format(ContactId))
            return False
    logger.warning('CTR already locked: {}'.format(ContactId))
    return True

def updateLockMetadata(ContactId, nMetadata):
    try:
        s3 = boto3.resource('s3')

        logger.info('Load existing metadata from lock object: %s' % ContactId)
        s3Object = s3.Object(os.environ["TRANSCRIPTS_DESTINATION"], 'locks/' + ContactId + '.lock')

        oMetadata = s3Object.metadata
        logger.info('Existing lock object metadata: %s' % oMetadata)

        fMetadata = {**nMetadata, **oMetadata}
        logger.info('Updating lock object metadata: %s' % fMetadata)

        s3.Object(os.environ["TRANSCRIPTS_DESTINATION"], 'locks/' + ContactId + '.lock').put(Body='IN_PROGRESS', Metadata=fMetadata)
        logger.info('Lock object metadata updated: %s' % ContactId)
        return True
    except Exception as e:
        logger.error('Error updateLockMetadata: {}'.format(e))
        logger.error('Current data: {}'.format(ContactId))
        raise e
    

def lambda_handler(event, context):
    try:
        logger.info('Event: {}'.format(event))

        process_record(event['record'])
        return "Done"
        
    except Exception as e:
        raise e

def createACContactChannelAnalyticsSalesforceObject(contactId, recordingPath):
    pnamespace = os.environ['SF_ADAPTER_NAMESPACE']
    if not pnamespace or pnamespace == '-':
        logger.info("SF_ADAPTER_NAMESPACE is empty")
        pnamespace = ''
    else:
        pnamespace = pnamespace + "__"

    sfRequest = {'Details' : {'Parameters':{}}}
    sfRequest['Details']['Parameters']['sf_operation'] = 'create'
    sfRequest['Details']['Parameters']['sf_object'] = pnamespace + 'AC_ContactChannelAnalytics__c'
    sfRequest['Details']['Parameters'][pnamespace + 'ContactId__c'] = contactId
    sfRequest['Details']['Parameters'][pnamespace + 'RecordingPath__c'] = recordingPath

    ACContactChannelAnalyticsId = invokeSfAPI(sfRequest)['Id']
    logger.info('SF Object Created, with ID: %s' % ACContactChannelAnalyticsId)
    
    #add ACContactChannelAnalyticsId to lock file metadata
    oMetadata = {}
    oMetadata['ACContactChannelAnalyticsId'] = ACContactChannelAnalyticsId
    updateLockMetadata(contactId, oMetadata)
    return
