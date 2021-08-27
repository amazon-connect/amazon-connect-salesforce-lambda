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

import os
import json
import base64
import logging
from salesforce import Salesforce

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))

def lambda_handler(event, context):
    try:
        logger.info('Start CTR Sync Lambda')
        logger.info('Event: {}'.format(event))

        process_ctr_record(event['record'])

        return "Done"

    except Exception as e:
        raise e


def process_ctr_record(record):
    decoded_payload = base64.b64decode(record).decode('utf-8')
    record_obj = json.loads(decoded_payload)
    logger.info('DecodedPayload: {}'.format(record_obj))

    if 'Attributes' in record_obj and 'postcallCTRImportEnabled' in record_obj["Attributes"] and record_obj["Attributes"]["postcallCTRImportEnabled"] == 'true':
        logger.info('postcallCTRImportEnabled = true')
        create_ctr_record(record_obj)


def create_ctr_record(ctr):
    objectnamespace = os.environ['SF_ADAPTER_NAMESPACE']

    if not objectnamespace or objectnamespace == '-':
        logger.info("SF_ADAPTER_NAMESPACE is empty")
        objectnamespace = ''
    else:
        objectnamespace = objectnamespace + "__"

    sf_request = {}

    sf_request[objectnamespace + 'AWSAccountId__c'] = ctr['AWSAccountId']

    if ctr['Agent']:
        sf_request[objectnamespace + 'AfterContactWorkDuration__c'] = ctr['Agent']['AfterContactWorkDuration']
        sf_request[objectnamespace + 'AfterContactWorkEndTimestamp__c'] = ctr['Agent']['AfterContactWorkEndTimestamp']
        sf_request[objectnamespace + 'AfterContactWorkStartTimestamp__c'] = ctr['Agent']['AfterContactWorkStartTimestamp']
        sf_request[objectnamespace + 'AgentConnectedToAgentTimestamp__c'] = ctr['Agent']['ConnectedToAgentTimestamp']
        sf_request[objectnamespace + 'AgentInteractionDuration__c'] = ctr['Agent']['AgentInteractionDuration']
        sf_request[objectnamespace + 'AgentCustomerHoldDuration__c'] = ctr['Agent']['CustomerHoldDuration']
        sf_request[objectnamespace + 'AgentHierarchyGroup__c'] = json.dumps(ctr['Agent']['HierarchyGroups'])
        sf_request[objectnamespace + 'AgentLongestHoldDuration__c'] = ctr['Agent']['LongestHoldDuration']
        sf_request[objectnamespace + 'AgentNumberOfHolds__c'] = ctr['Agent']['NumberOfHolds']
        sf_request[objectnamespace + 'AgentUsername__c'] = ctr['Agent']['Username']

        if ctr['Agent']['RoutingProfile']:
            sf_request[objectnamespace + 'AgentRoutingProfileARN__c'] = ctr['Agent']['RoutingProfile']['ARN']
            sf_request[objectnamespace + 'AgentRoutingProfileName__c'] = ctr['Agent']['RoutingProfile']['Name']

    sf_request[objectnamespace + 'AgentConnectionAttempts__c'] = ctr['AgentConnectionAttempts']
    sf_request[objectnamespace + 'Attributes__c'] = json.dumps(ctr['Attributes'])
    sf_request[objectnamespace + 'Channel__c'] = ctr['Channel']
    sf_request[objectnamespace + 'ConnectedToSystemTimestamp__c'] = ctr['ConnectedToSystemTimestamp']

    # Customer Data
    if ctr['CustomerEndpoint']:
        sf_request[objectnamespace + 'CustomerEndpointAddress__c'] = ctr['CustomerEndpoint']['Address']
    if ctr['ContactDetails']:
        sf_request[objectnamespace + 'ContactDetails__c'] = json.dumps(ctr['ContactDetails'])

    sf_request[objectnamespace + 'InitiationTimestamp__c'] = ctr['InitiationTimestamp']
    sf_request[objectnamespace + 'InitialContactId__c'] = ctr['InitialContactId']
    sf_request[objectnamespace + 'Initiation_Method__c'] = ctr['InitiationMethod']
    sf_request[objectnamespace + 'InitiationTimestamp__c'] = ctr['InitiationTimestamp']
    sf_request[objectnamespace + 'InstanceARN__c'] = ctr['InstanceARN']
    sf_request[objectnamespace + 'LastUpdateTimestamp__c'] = ctr['LastUpdateTimestamp']
    sf_request[objectnamespace + 'NextContactId__c'] = ctr['NextContactId']
    sf_request[objectnamespace + 'PreviousContactId__c'] = ctr['PreviousContactId']
    sf_request[objectnamespace + 'DisconnectReason__c'] = ctr['DisconnectReason']
    sf_request[objectnamespace + 'DisconnectTimestamp__c'] = ctr['DisconnectTimestamp']

    # Queue
    if ctr['Queue']:
        sf_request[objectnamespace + 'QueueARN__c'] = ctr['Queue']['ARN']
        sf_request[objectnamespace + 'QueueDequeueTimestamp__c'] = ctr['Queue']['DequeueTimestamp']
        sf_request[objectnamespace + 'QueueDuration__c'] = ctr['Queue']['Duration']
        sf_request[objectnamespace + 'QueueEnqueueTimestamp__c'] = ctr['Queue']['EnqueueTimestamp']
        sf_request[objectnamespace + 'QueueName__c'] = ctr['Queue']['Name']

    # Recording
    if ctr['Recording']:
        sf_request[objectnamespace + 'RecordingLocation__c'] = ctr['Recording']['Location']
        sf_request[objectnamespace + 'RecordingStatus__c'] = ctr['Recording']['Status']
        sf_request[objectnamespace + 'RecordingDeletionReason__c'] = ctr['Recording']['DeletionReason']
    
    # Reference
    if ctr['References']:
        sf_request[objectnamespace + 'References__c'] = json.dumps(ctr['References'])

    # System End Data
    if ctr['SystemEndpoint']:
        sf_request[objectnamespace + 'SystemEndpointAddress__c'] = ctr['SystemEndpoint']['Address']

    # Transfer Data
    if ctr['TransferredToEndpoint']:
        sf_request[objectnamespace + 'TransferredToEndpoint__c'] = ctr['TransferredToEndpoint']['Address']
        
    if ctr['TransferCompletedTimestamp']:
        sf_request[objectnamespace + 'TransferCompletedTimestamp__c'] = ctr['TransferCompletedTimestamp']

    logger.info(f'Record : {sf_request}')

    sf = Salesforce()
    sf.update_by_external(objectnamespace + "AC_ContactTraceRecord__c", objectnamespace + 'ContactId__c', ctr['ContactId'], sf_request)

    logger.info(f'Record Created Successfully')