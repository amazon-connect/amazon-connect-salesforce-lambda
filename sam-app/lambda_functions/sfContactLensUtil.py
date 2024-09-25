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
import os, json, boto3
from log_util import logger

def getDataSource():
    return 'Contact_Lens'

def processContactLensTranscript(iItems, participants):
    customerTranscripts = []
    agentTranscripts = []
    finalTranscripts = []

    for iTranscript in iItems:
        transcript = {}
        transcript['id'] = iTranscript['Id']
        transcript['participantId'] = iTranscript['ParticipantId'] # For now it's either AGENT or CUSTOMER
        transcript['beginOffsetMillis'] = iTranscript['BeginOffsetMillis']
        transcript['endOffsetMillis'] = iTranscript['EndOffsetMillis']
        transcript['content'] = iTranscript['Content']
        transcript['sentiment'] = iTranscript['Sentiment']
        transcript['loudness_score'] = iTranscript['LoudnessScore']  # array
        if 'IssuesDetected' in iTranscript:
            transcript['issues_detected'] = iTranscript['IssuesDetected']
        if 'Redaction' in iTranscript:
            transcript['redaction'] = iTranscript['Redaction']

        finalTranscripts.append(transcript)
        if iTranscript['ParticipantId'] == 'AGENT':
            transcript['participantRole'] = getParticipantRole('AGENT', participants)
            agentTranscripts.append(transcript)
        elif iTranscript['ParticipantId'] == 'CUSTOMER':
            transcript['participantRole'] = getParticipantRole('CUSTOMER', participants)
            customerTranscripts.append(transcript)

        
    return {'customerTranscripts' : customerTranscripts, 'agentTranscripts' : agentTranscripts, 'finalTranscripts': finalTranscripts}


def processContactLensConversationCharacteristics(contactLensObj, connectBucket, transcripts):
    resultSet = {}

    # Overall Sentiment
    resultSet['contactLensAgentOverallSentiment'] = contactLensObj['ConversationCharacteristics']['Sentiment']['OverallSentiment']['AGENT'] if 'AGENT' in contactLensObj['ConversationCharacteristics']['Sentiment']['OverallSentiment'] else None
    resultSet['contactLensCustomerOverallSentiment'] = contactLensObj['ConversationCharacteristics']['Sentiment']['OverallSentiment']['CUSTOMER'] if 'CUSTOMER' in contactLensObj['ConversationCharacteristics']['Sentiment']['OverallSentiment'] else None

    # Sentiment By Period
    if 'CUSTOMER' in contactLensObj['ConversationCharacteristics']['Sentiment']['SentimentByPeriod']['QUARTER']:
        customerSentimentCurve = []
        customerSentimentCurveLabel = 'Other'
        for quarter in contactLensObj['ConversationCharacteristics']['Sentiment']['SentimentByPeriod']['QUARTER']['CUSTOMER']:
            customerSentimentCurve.append(quarter['Score'])
        customerSentimentCurve[1:3] = [sum(customerSentimentCurve[1:3]) / 2]
        if (customerSentimentCurve[0] <= customerSentimentCurve[1] - 1) & (customerSentimentCurve[1] < customerSentimentCurve[2]):
            customerSentimentCurveLabel = 'S'
        elif (customerSentimentCurve[0] >= customerSentimentCurve[1] + 1) & (customerSentimentCurve[1] > customerSentimentCurve[2]):
            customerSentimentCurveLabel = 'Z'
        resultSet['contactLensCustomerSentimentCurve'] = customerSentimentCurveLabel
    else:
        resultSet['contactLensCustomerSentimentCurve'] = None

    # Interruptions Total Count
    resultSet['contactLensInterruptions'] = contactLensObj['ConversationCharacteristics']['Interruptions']['TotalCount']
    resultSet['contactLensAgentInterruptions'] = json.dumps(contactLensObj['ConversationCharacteristics']['Interruptions']['InterruptionsByInterrupter']['AGENT']) if 'AGENT' in contactLensObj['ConversationCharacteristics']['Interruptions']['InterruptionsByInterrupter'] else None
    resultSet['contactLensCustomerInterruptions'] = json.dumps(contactLensObj['ConversationCharacteristics']['Interruptions']['InterruptionsByInterrupter']['CUSTOMER']) if 'CUSTOMER' in contactLensObj['ConversationCharacteristics']['Interruptions']['InterruptionsByInterrupter'] else None
    

    # Non Talk Time 
    resultSet['contactLensNonTalkTime'] = contactLensObj['ConversationCharacteristics']['NonTalkTime']['TotalTimeMillis']

    # Talk Speed
    resultSet['contactLensTalkSpeedCustomer'] = contactLensObj['ConversationCharacteristics']['TalkSpeed']['DetailsByParticipant']['CUSTOMER']['AverageWordsPerMinute']
    resultSet['contactLensTalkSpeedAgent'] = contactLensObj['ConversationCharacteristics']['TalkSpeed']['DetailsByParticipant']['AGENT']['AverageWordsPerMinute']

    # Talk time
    resultSet['contactLensTalkTimeTotal'] = contactLensObj['ConversationCharacteristics']['TalkTime']['TotalTimeMillis']
    resultSet['contactLensTalkTimeCustomer'] = contactLensObj['ConversationCharacteristics']['TalkTime']['DetailsByParticipant']['CUSTOMER']['TotalTimeMillis']
    resultSet['contactLensTalkTimeAgent'] = contactLensObj['ConversationCharacteristics']['TalkTime']['DetailsByParticipant']['AGENT']['TotalTimeMillis']

    # Categories
    resultSet['contactLensMatchedCategories'] = '|'.join(contactLensObj['Categories']['MatchedCategories']) if len(contactLensObj['Categories']['MatchedCategories']) > 0 else None
    resultSet['contactLensMatchedDetails'] = json.dumps(contactLensObj['Categories']['MatchedDetails'])

    # Recording Path
    contactAttributes = getContactAttributes(contactLensObj)
    contactId = contactLensObj['CustomerMetadata']['ContactId']
    if ('postcallRedactedRecordingImportEnabled' in contactAttributes and contactAttributes['postcallRedactedRecordingImportEnabled'] == 'true'):
        logger.info('Redacted recording import is enabled')
        redactedRecordingLocation = getRedactedRecordingLocation(contactId, connectBucket)
        resultSet['recordingPath'] = redactedRecordingLocation
    else:
        resultSet['recordingPath'] = None

    # Transcript Full Text
    transcriptsText = []
    if len(transcripts) > 0:
        for transcript in transcripts:
            transcriptsText.append(transcript["content"])
    resultSet['contactLensTranscriptsFullText'] = ' '.join(transcriptsText)

    return resultSet

def getParticipantRole(participantId, participants):
    for participant in participants:
        if participant['ParticipantId'] == participantId:
            return participant['ParticipantRole']
    logger.warning('Participant Role not found for participant id: %s' % participantId)
    return ''

def getRedactedRecordingLocation(contactId, connectBucket):
    logger.info('Retrieving Redacted Recording S3 Location, contact ID is: %s', contactId)
    redactedRecordingKey = contactId + '_call_recording_redacted_'

    # Using paginator because S3 only returns up to 1000 objects from list_objects_v2() method
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=connectBucket, Prefix='Analysis/Voice/Redacted')
    for page in pages:
      for obj in page['Contents']:
        if redactedRecordingKey in obj['Key'] and obj['Key'].endswith('.wav'):
          redactedRecordingLocation = connectBucket + '/' + obj['Key']
          return redactedRecordingLocation
    logger.warn('Redacted Recording Not Found!')
    return ''
    
def getContactAttributes(contactLensObj):
    client = boto3.client('connect')
    try: 
        connect_response = client.get_contact_attributes(
            InstanceId=contactLensObj['CustomerMetadata']['InstanceId'],
            InitialContactId=contactLensObj['CustomerMetadata']['ContactId']
        )
        return connect_response["Attributes"]
    except Exception as e:
        logger.error('Error when retrieving contact attribute: {}'.format(e))