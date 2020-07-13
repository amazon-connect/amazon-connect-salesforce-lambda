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
import boto3
import json

logger = logging.getLogger()


def processContactLensTranscript(iItems):
    customerTranscripts = []
    agentTranscripts = []

    for iTranscript in iItems:
        transcript = {}
        transcript['start_time'] = round((float(iTranscript['BeginOffsetMillis'])/1000),2)
        transcript['end_time'] = round((float(iTranscript['EndOffsetMillis'])/1000),2)
        transcript['content'] = iTranscript['Content']
        transcript['sentiment'] = iTranscript['Sentiment']
        if iTranscript['ParticipantId'] == 'AGENT':
            agentTranscripts.append(transcript)
        elif iTranscript['ParticipantId'] == 'CUSTOMER':
            customerTranscripts.append(transcript)
        
    return {'customerTranscripts' : customerTranscripts, 'agentTranscripts' : agentTranscripts}


def processContactLensConversationCharacteristics(contactLensObj):
    resultSet = {}

    resultSet['contactLensAgentSentiment'] = contactLensObj['ConversationCharacteristics']['Sentiment']['OverallSentiment']['AGENT']
    resultSet['contactLensCustomerSentiment'] = contactLensObj['ConversationCharacteristics']['Sentiment']['OverallSentiment']['CUSTOMER']

    resultSet['contactLensInterruptions'] = contactLensObj['ConversationCharacteristics']['Interruptions']['TotalCount']

    resultSet['contactLensNonTalkTime'] = round((float(contactLensObj['ConversationCharacteristics']['NonTalkTime']['TotalTimeMillis'])/1000),2)

    resultSet['contactLensTalkSpeedCustomer'] = contactLensObj['ConversationCharacteristics']['TalkSpeed']['DetailsByParticipant']['CUSTOMER']['AverageWordsPerMinute']
    resultSet['contactLensTalkSpeedAgent'] = contactLensObj['ConversationCharacteristics']['TalkSpeed']['DetailsByParticipant']['AGENT']['AverageWordsPerMinute']

    resultSet['contactLensTalkTimeTotal'] = round((float(contactLensObj['ConversationCharacteristics']['TalkTime']['TotalTimeMillis'])/1000),2)
    resultSet['contactLensTalkTimeCustomer'] = round((float(contactLensObj['ConversationCharacteristics']['TalkTime']['DetailsByParticipant']['CUSTOMER']['TotalTimeMillis'])/1000),2)
    resultSet['contactLensTalkTimeAgent'] = round((float(contactLensObj['ConversationCharacteristics']['TalkTime']['DetailsByParticipant']['AGENT']['TotalTimeMillis'])/1000),2)

    return resultSet
    
        

