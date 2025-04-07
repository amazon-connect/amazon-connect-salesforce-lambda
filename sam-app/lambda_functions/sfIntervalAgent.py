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

import json, csv, os, re
import boto3
import urllib.parse
from salesforce import Salesforce
from sf_util import get_arg, parse_date, split_bucket_key
from log_util import logger

s3 = boto3.client("s3")
pnamespace = os.environ['SF_ADAPTER_NAMESPACE']
if not pnamespace or pnamespace == '-':
  logger.info("SF_ADAPTER_NAMESPACE is empty")
  pnamespace = ''
else:
  pnamespace = pnamespace + "__"

def lambda_handler(event, context):

  logger.info("Logging Start sfIntervalAgent")
  logger.info("sfIntervalAgent event: %s" % json.dumps(event))

  event_record = event['Records'][0]
  bucket = event_record['s3']['bucket']['name']
  logger.info("bucket: %s" % bucket)
  key = urllib.parse.unquote(event_record['s3']['object']['key'])
  logger.info("key: %s" % key)
  data = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
  logger.info("sfIntervalAgent data: %s" % data)
  sf = Salesforce()


  for record in csv.DictReader(data.split("\n")):
    logger.info("sfIntervalAgent record: %s" % record)
    agent_record = prepare_agent_record(record, event_record['eventTime'])
    ac_record_id = "%s%s" % (agent_record[pnamespace + 'AC_Object_Name__c'], agent_record[pnamespace + 'StartInterval__c'])

    # Only add the new region field if the field is available on the Salesforce org
    if sf.isFieldInSObject(pnamespace + 'AC_AgentPerformance__c', pnamespace + 'Region__c'):
        session = boto3.session.Session()
        agent_record[pnamespace + 'Region__c'] = session.region_name
        ac_record_id = "%s%s" % (ac_record_id, session.region_name)

    sf.update_by_external(pnamespace + "AC_AgentPerformance__c", pnamespace + 'AC_Record_Id__c',ac_record_id, agent_record)

  logger.info("done")

def prepare_agent_record(record_raw, current_date):
  record = {label_parser(k):value_parser(v) for k, v in record_raw.items()}
  #record[pnamespace + 'Type__c'] = "Agent"
  record[pnamespace + 'Created_Date__c'] = current_date
  #record[pnamespace + 'AC_Record_Id__c'] = "%s%s" % (record[pnamespace + 'AC_Object_Name__c'], current_date)
  #record[pnamespace + 'AC_Record_Id__c'] = "%s%s" % (record[pnamespace + 'AC_Object_Name__c'], record[pnamespace + 'StartInterval__c'])
  return record

def label_parser(key):
  if key.lower() == 'average agent interaction and customer hold time':#To Long
    return pnamespace + 'Avg_agent_interaction_and_cust_hold_time__c'

  if key.lower() == "agent":
    return pnamespace + "AC_Object_Name__c"
  
  key = re.sub(r'[-\s]+', '_', key)

  return pnamespace + "%s__c" % key

def value_parser(value):
  return value.replace("%", "") if len(value) > 0 else None
