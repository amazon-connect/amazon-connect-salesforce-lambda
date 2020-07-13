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
import json
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))


def lambda_handler(event, context):
    try:
        logger.info('Event: {}'.format(event))

        invoke_sfExec_async(event, context)
        
    except Exception as e:
        raise e

def invoke_sfExec_async(event, context):

    event_template = {'async' : True, 'record':{}}

    def send_data(data_to_send):
        event_to_send = event_template.copy()
        event_to_send['record'] = data_to_send
        
        if os.environ["POSTCALL_RECORDING_IMPORT_ENABLED"] == 'true' or os.environ["POSTCALL_TRANSCRIBE_ENABLED"] == 'true' or os.environ["CONTACT_LENS_IMPORT_ENABLED"] == 'true':
            logger.info('Invoke  EXECUTE_TRANSCRIPTION_STATE_MACHINE_LAMBDA')
            boto3.client('lambda').invoke(FunctionName=os.environ["EXECUTE_TRANSCRIPTION_STATE_MACHINE_LAMBDA"], InvocationType='Event', Payload=json.dumps(event_to_send))
        
        if os.environ["POSTCALL_CTR_IMPORT_ENABLED"] == 'true':
            logger.info('Invoke  EXECUTE_CTR_IMPORT_LAMBDA')
            boto3.client('lambda').invoke(FunctionName=os.environ["EXECUTE_CTR_IMPORT_LAMBDA"], InvocationType='Event', Payload=json.dumps(event_to_send))


    #for each record in Kinesis records, invoke a new Lambda function to process it async
    for record in event['Records']:
        payload = record['kinesis']['data']
        send_data(payload)