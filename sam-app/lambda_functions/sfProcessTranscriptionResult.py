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
from sf_util import getS3FileMetadata, getS3FileJSONObject, getBase64String, attachFileSaleforceObject, invokeSfAPI
from sfComprehendUtil import StartComprehendAnalysis, GetFormattedSentiment, GetFormattedKeywords, GetFormattedDominantLanguage, GetFormattedNamedEntities, GetFormattedSyntax, processTranscript

def lambda_handler(event, context):
    try:
        logger.info('Received event: %s' % event)
        transcriptFileUri = event["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
        languageCode = event["TranscriptionJob"]["LanguageCode"].split('-')[0]
        bucket = transcriptFileUri.split('/')[3]
        key = transcriptFileUri.split('/')[4]
        contactId = event["TranscriptionJob"]["TranscriptionJobName"].split('_')[0]

        logger.info('Getting lock file metadata: %s ' % contactId)
        oMetadata = getS3FileMetadata(bucket, key.split('_')[0])

        postcallTranscribeComprehendAnalysis = []
        if 'postcallTranscribeComprehendAnalysis'.lower() in oMetadata:
            postcallTranscribeComprehendAnalysis = oMetadata['postcallTranscribeComprehendAnalysis'.lower()].split(',')

        mACContactChannelAnalyticsId = None
        if 'ACContactChannelAnalyticsId'.lower() in oMetadata:
            mACContactChannelAnalyticsId = oMetadata['ACContactChannelAnalyticsId'.lower()]

        logger.info('Retrieving transcription file: %s', key)
        transcriptObj = getS3FileJSONObject(bucket, key)
        logger.info('Retrieved transcription file: %s', key)

        logger.info('Processing Customer transcript')
        customerTranscripts = processTranscript(transcriptObj['results']['channel_labels']['channels'][0]['items'])
        logger.info('Customer transcript: %s' % customerTranscripts)
        logger.info('Processing Agent transcript')
        agentTranscripts = processTranscript(transcriptObj['results']['channel_labels']['channels'][1]['items'])
        logger.info('Agent transcript: %s' % agentTranscripts)

        comprehendResults = {}
        #Test Comprehend
        if len(customerTranscripts) > 0:
            for ca in postcallTranscribeComprehendAnalysis:
                if ca == 'snt':
                    comprehendResults['FormattedSentiment'] = GetFormattedSentiment(StartComprehendAnalysis(customerTranscripts, 'snt', languageCode, False))
                elif ca=='kw':
                    comprehendResults['FormattedKeywords'] = GetFormattedKeywords(StartComprehendAnalysis(customerTranscripts, 'kw', languageCode, False))
                elif ca=='dl':
                    comprehendResults['FormattedDominantLanguage'] = GetFormattedDominantLanguage(StartComprehendAnalysis(customerTranscripts, 'dl', languageCode, False))
                elif ca=='ne':
                    comprehendResults['FormattedNamedEntities'] = GetFormattedNamedEntities(StartComprehendAnalysis(customerTranscripts, 'ne', languageCode, False))
                elif ca=='syn':
                    comprehendResults['FormattedSyntax'] = GetFormattedSyntax(StartComprehendAnalysis(customerTranscripts, 'syn', languageCode, False))
        #End Test Comprehend
        
        createSalesforceObject(contactId, customerTranscripts, agentTranscripts, comprehendResults, mACContactChannelAnalyticsId)

        updateLock(bucket, key.split('_')[0], oMetadata)

        logger.info('Done')
        return {"Done": True}
    except Exception as e:
        raise e

def createSalesforceObject(contactId, customerTranscripts, agentTranscripts, comprehendResults, mACContactChannelAnalyticsId):
    
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
    sfRequest['Details']['Parameters'][pnamespace + 'Sentiment__c'] = comprehendResults['FormattedSentiment'] if 'FormattedSentiment' in comprehendResults else ''
    sfRequest['Details']['Parameters'][pnamespace + 'Keywords__c'] = comprehendResults['FormattedKeywords'] if 'FormattedKeywords' in comprehendResults else ''
    sfRequest['Details']['Parameters'][pnamespace + 'DominantLanguage__c'] = comprehendResults['FormattedDominantLanguage'] if 'FormattedDominantLanguage' in comprehendResults else ''
    sfRequest['Details']['Parameters'][pnamespace + 'NamedEntities__c'] = comprehendResults['FormattedNamedEntities'] if 'FormattedNamedEntities' in comprehendResults else ''

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

    if 'FormattedSyntax' in comprehendResults:
        logger.info('Attaching Comprehend Syntax')
        attachFileSaleforceObject('ComprehendSyntax.json', 'application/json', 'Comprehend Syntax', ACContactChannelAnalyticsId, getBase64String(comprehendResults['FormattedSyntax']))
        logger.info('SF Comprehend Syntax Attached')

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
    