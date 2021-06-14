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
import datetime
import logging
import os
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))
client = boto3.client('transcribe')


def lambda_handler(event, context):
    try:
        response = client.get_transcription_job(
            TranscriptionJobName=event["TranscriptionJobName"]
        )
        # BELOW IS THE CODE TO FIX SERIALIZATION ON DATETIME OBJECTS
        if "CreationTime" in response["TranscriptionJob"]:
            val = response["TranscriptionJob"]["CreationTime"]
            response["TranscriptionJob"]["CreationTime"] = val.strftime("%Y-%m-%dT%H:%M:%S.%f%z") if isinstance(val, datetime.datetime) else str(val)
        if "StartTime" in response["TranscriptionJob"]:
            val = response["TranscriptionJob"]["StartTime"]
            response["TranscriptionJob"]["StartTime"] = val.strftime("%Y-%m-%dT%H:%M:%S.%f%z") if isinstance(val, datetime.datetime) else str(val)
        if "CompletionTime" in response["TranscriptionJob"]:
            val = response["TranscriptionJob"]["CompletionTime"]
            response["TranscriptionJob"]["CompletionTime"] = val.strftime("%Y-%m-%dT%H:%M:%S.%f%z") if isinstance(val, datetime.datetime) else str(val)
        logger.info(response)
        return response["TranscriptionJob"]
    except Exception as e:
        raise e