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

import datetime
import boto3
import base64
import json, logging, os

from sf_util import get_arg
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from botocore.signers import CloudFrontSigner
import logging
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))

PRIVATE_KEY_HEADER = '-----BEGIN RSA PRIVATE KEY-----'
PRIVATE_KEY_FOOTER = '-----END RSA PRIVATE KEY-----\r\n'

def lambda_handler(event, context):
    if 'recordingPath' not in event or not event['recordingPath'] or event['recordingPath'] == 'null':
        logger.info("No recordingPath in event; returning.")
        return None
    # retrieve secrets
    logger.info("Retrieving cloudfront credentials")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    sf_credentials_secrets_manager_arn = get_arg(os.environ,
            'SF_CREDENTIALS_SECRETS_MANAGER_ARN')
    secrets = json.loads(client.get_secret_value(SecretId=sf_credentials_secrets_manager_arn)['SecretString'])
    private_key = secrets['CloudFrontPrivateKey']
    access_key_id = secrets['CloudFrontAccessKeyID']
    logger.info("Cloudfront credentials retrieved")

    # construct url to audio recording
    recordingPath = event['recordingPath'] # need to remove bucket name, connect dir from path
    if("/connect/" in recordingPath):
        recordingPath = "connect/" + recordingPath.split("/connect/", 1)[1]
    elif("/Analysis/" in recordingPath):
        recordingPath = "Analysis/" + recordingPath.split("/Analysis/", 1)[1]
    cloudfront_domain = get_arg(os.environ, 'CLOUDFRONT_DISTRIBUTION_DOMAIN_NAME')
    url = 'https://' + cloudfront_domain + '/' + recordingPath
    logger.info('Unsigned audio recording url: %s' % url)

    # sign url
    expire_date = datetime.datetime.utcnow() + datetime.timedelta(minutes=60)
    cloudfront_signer = CloudFrontSigner(access_key_id, rsa_signer(private_key))
    signed_url = cloudfront_signer.generate_presigned_url(
        url, date_less_than=expire_date)
    logger.info('Signed audio recording url: %s' % signed_url)
    return signed_url

def rsa_signer(key):
    def rsa_signer_with_key(message):
        private_key = serialization.load_pem_private_key(
            format_private_key(key),
            password=None,
            backend=default_backend()
        )
        return private_key.sign(message, padding.PKCS1v15(), hashes.SHA1())
    return rsa_signer_with_key

def format_private_key(private_key):
    header_len = len(PRIVATE_KEY_HEADER)
    footer_len = len(PRIVATE_KEY_FOOTER)
    rsa_key = PRIVATE_KEY_HEADER \
        + private_key[header_len:-(footer_len-2)].replace(' ', '\r\n') \
        + PRIVATE_KEY_FOOTER
    return rsa_key.encode('utf8')