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

import json, csv, urllib.parse, logging, os
import boto3
import botocore
import base64
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))
from sf_util import getS3FileMetadata, getS3FileJSONObject, getBase64String, attachFileSaleforceObject, invokeSfAPI, split_s3_bucket_key
from sfContactLensUtil import processContactLensTranscript, processContactLensConversationCharacteristics, getDataSource, getContactAttributes

def lambda_handler(event, context):
    # Check if Contact Lens is enabled at application level
    if os.environ["CONTACT_LENS_IMPORT_ENABLED"] != 'true':
        logger.warning('Contact Lens Import is disabled')
        return {"Done": False}

    try:
        logger.info('Received event: %s' % json.dumps(event))

        event_record = event['Records'][0]
        bucket = event_record['s3']['bucket']['name']
        logger.info("ContactLens file bucket: %s" % bucket)

        key = urllib.parse.unquote(event_record['s3']['object']['key'])
        logger.info("ContactLens file key: %s" % key)

        logger.info('Retrieving ContactLens file: %s', key)
        contactLensObj = getS3FileJSONObject(bucket, key)
        logger.info('Retrieved ContactLens file: %s', key)

        contactId = contactLensObj['CustomerMetadata']['ContactId']

        # Check contact attributes
        contactAttributes = getContactAttributes(contactLensObj)
        if ("contactLensImportEnabled" not in contactAttributes or "contactLensImportEnabled" in contactAttributes and contactAttributes["contactLensImportEnabled"] != 'true'):
            logger.warning("Contact Lens import not enabled!")
            return {"Done": False}

        # Check if Connect instanceId in contact lens object matches env variable
        if not isValidContactLensData(contactLensObj):
            logger.warning('Wrong Contact Lens data for Amazon Connect instance %s', os.environ["AMAZON_CONNECT_INSTANCE_ID"])
            return {"Done": False}

        logger.info('Getting lock file metadata: %s ' % contactId)
        oMetadata = getS3FileMetadata(os.environ['TRANSCRIPTS_DESTINATION'], contactId)

        mACContactChannelAnalyticsId = None
        if 'ACContactChannelAnalyticsId'.lower() in oMetadata:
            mACContactChannelAnalyticsId = oMetadata['ACContactChannelAnalyticsId'.lower()]

        logger.info('Processing ContactLens transcript')
        participants = contactLensObj['Participants']
        ContactLensTranscripts = processContactLensTranscript(contactLensObj['Transcript'], participants)
        
        # customerTranscripts = ','.join(str(transcript) for transcript in ContactLensTranscripts['customerTranscripts'])
        # agentTranscripts = ','.join(str(transcript) for transcript in ContactLensTranscripts['agentTranscripts'])
        contactLensTranscripts = ContactLensTranscripts['finalTranscripts']
        
        logger.info('Processing Conversation Characteristics')
        contactLensConversationCharacteristics = processContactLensConversationCharacteristics(contactLensObj, bucket, contactLensTranscripts)

        createSalesforceObject(contactId, contactLensTranscripts, contactLensConversationCharacteristics, mACContactChannelAnalyticsId)

        logger.info('Updating s3 metadata')
        updateLock(os.environ['TRANSCRIPTS_DESTINATION'], contactId, oMetadata)

        logger.info('Done')
        return {"Done": True}
    except Exception as e:
        raise e

def createSalesforceObject(contactId, contactLensTranscripts, contactLensConversationCharacteristics, mACContactChannelAnalyticsId):
    
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
    
    if contactLensConversationCharacteristics['contactLensCustomerOverallSentiment']:
        sfRequest['Details']['Parameters'][pnamespace + 'ContactLensCustomerSentiment__c'] = contactLensConversationCharacteristics['contactLensCustomerOverallSentiment']
    if contactLensConversationCharacteristics['contactLensAgentOverallSentiment']:
        sfRequest['Details']['Parameters'][pnamespace + 'ContactLensAgentSentiment__c'] = contactLensConversationCharacteristics['contactLensAgentOverallSentiment']

    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensInterruptions__c'] = contactLensConversationCharacteristics['contactLensInterruptions']
    if contactLensConversationCharacteristics['contactLensAgentInterruptions']:
        sfRequest['Details']['Parameters'][pnamespace + 'ContactLensAgentInterruptions__c'] = contactLensConversationCharacteristics['contactLensAgentInterruptions']
    if contactLensConversationCharacteristics['contactLensCustomerInterruptions']:
        sfRequest['Details']['Parameters'][pnamespace + 'ContactLensCustomerInterruptions__c'] = contactLensConversationCharacteristics['contactLensCustomerInterruptions']

    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensNonTalkTime__c'] = contactLensConversationCharacteristics['contactLensNonTalkTime']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkSpeedCustomer__c'] = contactLensConversationCharacteristics['contactLensTalkSpeedCustomer']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkSpeedAgent__c'] = contactLensConversationCharacteristics['contactLensTalkSpeedAgent']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkTimeTotal__c'] = contactLensConversationCharacteristics['contactLensTalkTimeTotal']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkTimeCustomer__c'] = contactLensConversationCharacteristics['contactLensTalkTimeCustomer']
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTalkTimeAgent__c'] = contactLensConversationCharacteristics['contactLensTalkTimeAgent']
    if contactLensConversationCharacteristics['recordingPath'] is not None:
        sfRequest['Details']['Parameters'][pnamespace + 'RecordingPath__c'] = contactLensConversationCharacteristics['recordingPath']
    if contactLensConversationCharacteristics['contactLensMatchedCategories']:
        sfRequest['Details']['Parameters'][pnamespace + 'ContactLensMatchedCategories__c'] = contactLensConversationCharacteristics['contactLensMatchedCategories']
    
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensMatchedDetails__c'] = contactLensConversationCharacteristics['contactLensMatchedDetails']
    if contactLensConversationCharacteristics['contactLensCustomerSentimentCurve']:
        sfRequest['Details']['Parameters'][pnamespace + 'ContactLensCustomerSentimentCurve__c'] = contactLensConversationCharacteristics['contactLensCustomerSentimentCurve']

    sfRequest['Details']['Parameters'][pnamespace + 'DataSource__c'] = getDataSource()
    sfRequest['Details']['Parameters'][pnamespace + 'ContactLensTranscriptsFullText__c'] = contactLensConversationCharacteristics['contactLensTranscriptsFullText']
    # if len(customerTranscripts) > 0:
    #     sfRequest['Details']['Parameters'][pnamespace + 'ContactLensCustomerTranscripts__c'] = customerTranscripts
    # if len(agentTranscripts) > 0:
    #     sfRequest['Details']['Parameters'][pnamespace + 'ContactLensAgentTranscripts__c'] = agentTranscripts
    

    ACContactChannelAnalyticsId = mACContactChannelAnalyticsId
    if mACContactChannelAnalyticsId is not None:
        logger.info("Updating the SF Object: %s" % sfRequest)
        invokeSfAPI(sfRequest)
    else:
        logger.info('SF Object does not exist, creating a new one: %s' % sfRequest)
        ACContactChannelAnalyticsId = invokeSfAPI(sfRequest)['Id']
        logger.info('SF Object Created, with ID: %s' % ACContactChannelAnalyticsId)

    if len(contactLensTranscripts) > 0:
        logger.info('Attaching SF Transcript - Contact Lens')
        attachFileSaleforceObject('ContactLensTranscripts.json', 'application/json', 'Contact Lens Transcripts', ACContactChannelAnalyticsId, getBase64String(contactLensTranscripts))
        logger.info('SF Transcript Attached - Contact Lens')
        
        
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

def isValidContactLensData(contactLensObj):
    instanceId = contactLensObj['CustomerMetadata']['InstanceId']
    return os.environ["AMAZON_CONNECT_INSTANCE_ID"] == instanceId
