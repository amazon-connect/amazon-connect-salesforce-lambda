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

    http:#www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import boto3
import botocore
import logging
import os
import json
import datetime
import uuid
from time import sleep

logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))

def lambda_handler(event, context):
    logger.info("event: %s" % json.dumps(event))
    method = event["method"]
    params = event["params"]

    try:
        result = ""
        if method == "connect_create_instance":
            result = connect_create_instance(**params)
        elif method == "kinesis_create_stream":
            result = kinesis_create_stream(**params)
        elif method == "s3_create_bucket":
            result = s3_create_bucket(**params)
        elif method == "kinesis_describe_stream":
            result = kinesis_describe_stream(**params)
        elif method == "connect_associate_instance_storage_config":
            result = connect_associate_instance_storage_config(**params)
        elif method == "connect_associate_approved_origin":
            result = connect_associate_approved_origin(**params)
        elif method == "retrieve_lambda_parameters":
            result = retrieve_lambda_parameters(**params)
        elif method == "setup_audio_recording":
            result = setup_audio_recording(**params)
        elif method == "get_aws_region":
            result = get_aws_region()
        elif method == "generate_audio_recording_url":
            result = generate_audio_recording_url(params)
        else:
            raise Exception("Invalid method: " + method)
        return { 
            "success": True, 
            "body": json.dumps(result)
        }
    except Exception as e:
        logger.error("error: %s" % e)
        return {
            "success": False,
            "body": str(e)
        }
    return

def connect_create_instance(ConnectInstanceAlias, IdentityManagementType, InboundCallsEnabled, OutboundCallsEnabled):
    connect = boto3.client("connect")
    try:
        id = getConnectInstanceIdFromInstanceAlias(ConnectInstanceAlias, connect)
        logger.info("Instance already created. Returning.")
        return { "Id": id }
    except:
        logger.info("Instance not created. Creating instance.")

    result = connect.create_instance(
        InstanceAlias=ConnectInstanceAlias,
        IdentityManagementType=IdentityManagementType,
        InboundCallsEnabled=InboundCallsEnabled,
        OutboundCallsEnabled=OutboundCallsEnabled
    )
    logger.info("result: %s" % json.dumps(result))
    return result

def kinesis_create_stream(StreamName, ShardCount):
    kinesis = boto3.client("kinesis")
    
    # check if stream already exists. If not then create the stream.
    try:
        kinesis_describe_stream(StreamName)
        return "Stream already created."
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            result = kinesis.create_stream(
                StreamName=StreamName,
                ShardCount=ShardCount
            )
            logger.info("result: %s" % json.dumps(result))
            return result
        else:
            raise e

def s3_create_bucket(Bucket):
    s3 = boto3.client("s3")
    aws_region = os.environ["AWS_REGION"]
    result = None
    if aws_region == "us-east-1":
        result = s3.create_bucket(
            Bucket=Bucket
        )
    else:
        result = s3.create_bucket(
            Bucket=Bucket,
            CreateBucketConfiguration={
                "LocationConstraint": aws_region
            }
        )
    logger.info("result: %s" % json.dumps(result))
    return result

def kinesis_describe_stream(StreamName):
    result = boto3.client("kinesis").describe_stream(StreamName=StreamName)
    formatted_result = format_datetime_values(result)
    logger.info("result: %s" % json.dumps(formatted_result))
    return formatted_result

def connect_associate_instance_storage_config(ConnectInstanceId, ResourceType, StorageType, BucketName="", BucketPrefix="", StreamArn="", s3KMSKeyARN=""):
    connect = boto3.client("connect")

    storage_config = { "StorageType": StorageType }
    if StorageType == "S3":
        storage_config["S3Config"] = {
            "BucketName": BucketName,
            "BucketPrefix": BucketPrefix
        }
        if s3KMSKeyARN:
            storage_config["S3Config"]["EncryptionConfig"] = {
                "EncryptionType": "KMS",
                "KeyId": s3KMSKeyARN
            }
    elif StorageType == "KINESIS_STREAM":
        storage_config["KinesisStreamConfig"] = {
            "StreamArn": StreamArn
        }

    result = None
    iter = 0
    errors = []
    while result is None:
        try:
            storage_config_list = connect.list_instance_storage_configs(InstanceId=ConnectInstanceId, ResourceType=ResourceType)["StorageConfigs"]
            if len(storage_config_list) > 0:
                # should only be one of each storage type
                association_id = storage_config_list[0]["AssociationId"]
                result = connect.update_instance_storage_config(
                    InstanceId=ConnectInstanceId,
                    AssociationId=association_id,
                    ResourceType=ResourceType,
                    StorageConfig=storage_config
                )
            else:
                result = connect.associate_instance_storage_config(
                    InstanceId=ConnectInstanceId,
                    ResourceType=ResourceType,
                    StorageConfig=storage_config
                )
        except botocore.exceptions.ClientError as e:
            iter += 1
            errors.append(str(e))
            if e.response['Error']['Code'] == 'ResourceNotFoundException' and iter < 5:
                sleep(2)
            else:
                raise Exception(str(errors))
    formatted_result = format_datetime_values(result)
    logger.info("result: %s" % json.dumps(formatted_result))
    return formatted_result

def connect_associate_approved_origin(ConnectInstanceAlias, Origin):
    connect = boto3.client("connect")
    instanceId = getConnectInstanceIdFromInstanceAlias(ConnectInstanceAlias, connect)
    result = connect.associate_approved_origin(InstanceId=instanceId, Origin=Origin)
    logger.info("result: %s" % json.dumps(result))
    return result

def retrieve_lambda_parameters(ConnectInstanceAlias):
    connect_client = boto3.client("connect")
    cloudformation_client = boto3.client("cloudformation")
    cloudformation_stack_id = os.environ["CLOUDFORMATION_STACK_ID"]

    connectInstanceId = getConnectInstanceIdFromInstanceAlias(ConnectInstanceAlias, connect_client)

    connectRecordingS3BucketName = connect_client.list_instance_storage_configs(
        InstanceId=connectInstanceId, ResourceType="CALL_RECORDINGS"
    )["StorageConfigs"][0]["S3Config"]["BucketName"]
    transcribeOutputS3BucketName = connect_client.list_instance_storage_configs(
        InstanceId=connectInstanceId, ResourceType="CHAT_TRANSCRIPTS"
    )["StorageConfigs"][0]["S3Config"]["BucketName"]
    connectReportingS3BucketName = connect_client.list_instance_storage_configs(
        InstanceId=connectInstanceId, ResourceType="SCHEDULED_REPORTS"
    )["StorageConfigs"][0]["S3Config"]["BucketName"]
    ctrKinesisConfig = connect_client.list_instance_storage_configs(
        InstanceId=connectInstanceId, ResourceType="CONTACT_TRACE_RECORDS"
    )

    ctrKinesisARN = ""
    if ctrKinesisConfig["StorageConfigs"]:
        ctrKinesisARN = ctrKinesisConfig["StorageConfigs"][0]["KinesisStreamConfig"]["StreamArn"]

    result = {
        "connectInstanceId": connectInstanceId,
        "connectRecordingS3BucketName": connectRecordingS3BucketName,
        "transcribeOutputS3BucketName": transcribeOutputS3BucketName,
        "connectReportingS3BucketName": connectReportingS3BucketName,
        "ctrKinesisARN": ctrKinesisARN,
        "cloudFormationStackId": cloudformation_stack_id
    }
    logger.info("result: %s" % json.dumps(result))
    return result

def setup_audio_recording(CloudfrontPublicKey):
    s3_client = boto3.client("s3")
    bucket_name = os.environ["RECORDING_BUCKET_NAME"]
    bucket_cors_rules = []
    try:
        bucket_cors_rules = s3_client.get_bucket_cors(Bucket=bucket_name)["CORSRules"]
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchCORSConfiguration':
            raise e
    
    sf_host = os.environ["SALESFORCE_HOST"]
    lightning_url = sf_host[:sf_host.index(".my.salesforce.com")] + ".lightning.force.com"
    visualforce_url = sf_host[:sf_host.index(".my.salesforce.com")]
    if os.environ["NAMESPACE"]:
        visualforce_url = visualforce_url + "--" + os.environ["NAMESPACE"] + ".visualforce.com"
    else:
        visualforce_url = visualforce_url + "--c.visualforce.com"

    for rule in bucket_cors_rules:
        if lightning_url in rule["AllowedOrigins"]:
            bucket_cors_rules.remove(rule)

    bucket_cors_rules.append({
        "AllowedHeaders": ["Access-Control-Allow-Origin"],
        "AllowedMethods": ["GET"],
        "AllowedOrigins": [lightning_url, visualforce_url]
    })
    s3_client.put_bucket_cors(
        Bucket=bucket_name,
        CORSConfiguration={
            "CORSRules": bucket_cors_rules
        }
    )

    cloudfront_client = boto3.client("cloudfront")
    create_public_key_response = cloudfront_client.create_public_key(
        PublicKeyConfig={
            'CallerReference': str(uuid.uuid4()),
            'Name': 'AmazonConnectSalesforceCTIAdapterContactLens',
            'EncodedKey': CloudfrontPublicKey
        }
    )
    create_key_group_response = cloudfront_client.create_key_group(
        KeyGroupConfig={
            'Name': 'AmazonConnectSalesforceCTIAdapterContactLens',
            'Items': [
                create_public_key_response["PublicKey"]["Id"]
            ]
        }
    )

    # edge lambdas must be created in us-east-1
    lambda_client = boto3.client("lambda", region_name='us-east-1')
    cloudformation_stack_name = os.environ["CLOUDFORMATION_STACK_NAME"]
    MAX_LAMBDA_NAME_LENGTH = 64
    function_name_end = '-sfSig4RequestToS3'
    function_name_start = cloudformation_stack_name[:MAX_LAMBDA_NAME_LENGTH - len(function_name_end)]
    function_name =  function_name_start + function_name_end
    create_function_response = lambda_client.create_function(
        FunctionName=function_name,
        Runtime='nodejs12.x',
        Role=os.environ["SIG4_LAMBDA_ROLE_ARN"],
        Handler='sfSig4RequestToS3.handler',
        Code={
            'ZipFile': open('./sfSig4RequestToS3.zip', 'rb').read()
        }
    )
    publish_version_response = lambda_client.publish_version(
        FunctionName=function_name
    )

    get_distribution_config_response = cloudfront_client.get_distribution_config(
        Id=os.environ["CLOUDFRONT_DISTRIBUTION_ID"]
    )

    distribution_config = get_distribution_config_response["DistributionConfig"]
    distribution_config["DefaultCacheBehavior"]["LambdaFunctionAssociations"] = {
        'Quantity': 1,
        'Items': [
            {
                'LambdaFunctionARN': publish_version_response["FunctionArn"],
                'EventType': 'origin-request',
                'IncludeBody': False
            },
        ]
    }
    distribution_config["DefaultCacheBehavior"]["TrustedSigners"] = {
        'Enabled': False,
        'Quantity': 0
    }
    distribution_config["DefaultCacheBehavior"]["TrustedKeyGroups"] = {
        'Enabled': True,
        'Quantity': 1,
        'Items': [ create_key_group_response["KeyGroup"]["Id"] ]
    }

    return format_datetime_values(cloudfront_client.update_distribution(
        DistributionConfig=distribution_config, 
        Id=os.environ["CLOUDFRONT_DISTRIBUTION_ID"], 
        IfMatch=get_distribution_config_response["ETag"]
    ))

def get_aws_region():
    return os.environ["AWS_REGION"]

def generate_audio_recording_url(params):
    lambda_client = boto3.client('lambda')
    resp = lambda_client.invoke(FunctionName=os.environ["GENERATE_AUDIO_RECORDING_LAMBDA"], InvocationType='RequestResponse', Payload=json.dumps(params))
    logger.info(resp)
    lambda_result = resp["Payload"].read().decode("utf-8")
    if resp["StatusCode"] < 200 or resp["StatusCode"] >= 300:
        raise Exception("ERROR: GENERATE_AUDIO_RECORDING_LAMBDA failed with " + lambda_result)
    return lambda_result

def getConnectInstanceIdFromInstanceAlias(ConnectInstanceAlias, connect_client):
    list_instances_result = connect_client.list_instances(MaxResults=20)
    instance_list = list_instances_result["InstanceSummaryList"]
    next_token = list_instances_result["NextToken"] if "NextToken" in list_instances_result else ''

    while len(instance_list):
        instance = instance_list.pop()
        if instance["InstanceAlias"] == ConnectInstanceAlias:
            return instance["Id"]
        if not len(instance_list) and next_token:
            list_instances_result = connect_client.list_instances(MaxResults=20, NextToken=next_token)
            instance_list = list_instances_result["InstanceSummaryList"]
            next_token = list_instances_result["NextToken"] if "NextToken" in list_instances_result else ''

    raise Exception("ERROR: Could not find Connect instance " + str(ConnectInstanceAlias))

def format_datetime_values(obj):
    for k, v in obj.items():
        if isinstance(v, datetime.datetime):
            obj[k] = str(v)
        elif isinstance(v, dict):
            obj[k] = format_datetime_values(v)
    return obj