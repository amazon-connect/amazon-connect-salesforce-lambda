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

import json
import boto3
import botocore
import os
import base64
import logging
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))
from sf_util import getS3FileMetadata, getS3FileJSONObject, getBase64String, attachFileSaleforceObject, invokeSfAPI, split_s3_bucket_key
from sfContactLensUtil import processContactLensTranscript, processContactLensConversationCharacteristics

def lambda_handler(event, context):
    try:
        logger.info('Received event: %s' % event)
        clFileUri = event["ContactLensFileUri"]
        
        bucket, key = split_s3_bucket_key(clFileUri)

        logger.info('Retrieving ContactLens file: %s', key)
        contactLensObj = getS3FileJSONObject(bucket, key)
        logger.info('Retrieved ContactLens file: %s', key)
        
        contactId = contactLensObj['CustomerMetadata']['ContactId']
        languageCode = contactLensObj['LanguageCode']

        logger.info('Getting lock file metadata: %s ' % contactId)
        oMetadata = getS3FileMetadata(os.environ['TRANSCRIPTS_DESTINATION'], contactId)

        mACContactChannelAnalyticsId = None
        if 'ACContactChannelAnalyticsId'.lower() in oMetadata:
            mACContactChannelAnalyticsId = oMetadata['ACContactChannelAnalyticsId'.lower()]

        logger.info('Processing ContactLens transcript')
        ContactLensTranscripts = processContactLensTranscript(contactLensObj['Transcript'])
        
        customerTranscripts = ContactLensTranscripts['customerTranscripts']
        logger.info('Customer transcript: %s' % customerTranscripts)
        agentTranscripts = ContactLensTranscripts['agentTranscripts']
        logger.info('Agent transcript: %s' % agentTranscripts)
        
        contactLensConversationCharacteristics = processContactLensConversationCharacteristics(contactLensObj)
        logger.info('Conversation Characteristics: %s' % contactLensConversationCharacteristics)

        createSalesforceObject(contactId, customerTranscripts, agentTranscripts, contactLensConversationCharacteristics, mACContactChannelAnalyticsId)

        updateLock(os.environ['TRANSCRIPTS_DESTINATION'], contactId, oMetadata)

        logger.info('Done')
        return {"Done": True}
    except Exception as e:
        raise e

def createSalesforceObject(contactId, customerTranscripts, agentTranscripts, contactLensConversationCharacteristics, mACContactChannelAnalyticsId):
    
    pnamespace = os.environ['SF_ADAPTER_NAMESPACE']
    if not pnamespace or pnamespace == '-':
        logger.info("SF_ADAPTER_NAMESPACE is empty")
        pnamespace = ''
    else:
        pnamespace = pnamespace + "__"

    sfRequest = {'Details' : {'Parameters':{}}}
    if mACContactChannelAnalyticsId is not None:
        logger.info('SF Object Already Created, with ID: %s' % mACContactChannelAnalyticsId)
        sfRequest['Details']['Parameters']['sf_operation'] = 'update'
        sfRequest['Details']['Parameters']['sf_id'] = mACContactChannelAnalyticsId
    else:
        sfRequest['Details']['Parameters']['sf_operation'] = 'create'

    sfRequest['Details']['Parameters']['sf_object'] = pnamespace + 'AC_ContactChannelAnalytics__c'
    sfRequest['Details']['Parameters'][pnamespace + 'ContactId__c'] = contactId
    
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensCustomerSentiment__c'] = contactLensConversationCharacteristics['contactLensCustomerSentiment']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensAgentSentiment__c'] = contactLensConversationCharacteristics['contactLensAgentSentiment']

    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensInterruptions__c'] = contactLensConversationCharacteristics['contactLensInterruptions']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensNonTalkTime__c'] = contactLensConversationCharacteristics['contactLensNonTalkTime']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkSpeedCustomer__c'] = contactLensConversationCharacteristics['contactLensTalkSpeedCustomer']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkSpeedAgent__c'] = contactLensConversationCharacteristics['contactLensTalkSpeedAgent']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkTimeTotal__c'] = contactLensConversationCharacteristics['contactLensTalkTimeTotal']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkTimeCustomer__c'] = contactLensConversationCharacteristics['contactLensTalkTimeCustomer']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkTimeAgent__c'] = contactLensConversationCharacteristics['contactLensTalkTimeAgent']


    ACContactChannelAnalyticsId = mACContactChannelAnalyticsId
    if mACContactChannelAnalyticsId is not None:
        logger.info("Updating the SF Object: %s" % sfRequest)
        invokeSfAPI(sfRequest)
    else:
        logger.info('SF Object does not exist, creating a new one: %s' % sfRequest)
        ACContactChannelAnalyticsId = invokeSfAPI(sfRequest)['Id']
        logger.info('SF Object Created, with ID: %s' % ACContactChannelAnalyticsId)

    if len(customerTranscripts) > 0:
        logger.info('Attaching SF Transcript - Customer Side')
        attachFileSaleforceObject('CustomerTranscripts.json', 'application/json', 'Call Recording Transcription - Customer Side', ACContactChannelAnalyticsId, getBase64String(customerTranscripts))
        logger.info('SF Transcript Attached - Customer Side')

    if len(agentTranscripts) > 0:
        logger.info('Attaching SF Transcript - Agent Side')
        attachFileSaleforceObject('AgentTranscripts.json', 'application/json', 'Call Recording Transcription - Agent Side', ACContactChannelAnalyticsId, getBase64String(agentTranscripts))
        logger.info('SF Transcript Attached - Agent Side')


def updateLock(Bucket, ContactId, oMetadata):
    try:
        s3r = boto3.resource('s3')
        logger.info('Updating lock file: %s' % ContactId)
        s3r.Object(Bucket, 'locks/' + ContactId + '.lock').put(Body='COMPLETED', Metadata=oMetadata)
        logger.info('Lock file updated: %s' % ContactId)
        return True
    except Exception as e:
        logger.error('Error lock: {}'.format(e))
        logger.error('Current data: {}'.format(ContactId))
        raise e