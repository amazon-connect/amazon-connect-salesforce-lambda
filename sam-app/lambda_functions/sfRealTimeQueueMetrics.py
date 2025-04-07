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

import json, csv, os
import boto3
import urllib.parse
from salesforce import Salesforce
from sf_util import get_arg, parse_date, split_bucket_key
from log_util import logger

objectnamespace = os.environ['SF_ADAPTER_NAMESPACE']

if not objectnamespace  or objectnamespace == '-':
    logger.info("SF_ADAPTER_NAMESPACE is empty")
    objectnamespace = ''
else:
    objectnamespace = objectnamespace + "__"
#get connect reference
connect=boto3.client('connect')

def lambda_handler(event, context):

    try:
        logger.info("Start")
        logger.info(f"boto3 version: {boto3.__version__}")

        instance_id = os.environ['AMAZON_CONNECT_INSTANCE_ID']
        queue_max_result = os.environ['AMAZON_CONNECT_QUEUE_MAX_RESULT']
        logger.info(f"instance id: {instance_id}")
        next_token = 'NoToken'

        while len(next_token)!=0:
            if next_token == 'NoToken':
                queues_data = connect.list_queues(InstanceId=instance_id,QueueTypes=['STANDARD'],MaxResults=int(queue_max_result))
            else:
                queues_data = connect.list_queues(InstanceId=instance_id,QueueTypes=['STANDARD'],MaxResults=int(queue_max_result), NextToken=next_token)

            logger.info(f"queues_data: {queues_data}")
            if 'QueueSummaryList' in queues_data.keys():
                queue_summary = queues_data['QueueSummaryList']
                logger.info(f"QueueSummaryList: {queue_summary}")
            else:
                queue_summary=[]
                logger.info("QueueSummaryList key is Not present")

            if 'NextToken' in queues_data.keys():
                next_token = queues_data['NextToken']
                logger.info(f"next_token: {next_token}")
            else:
                next_token=''
                logger.info("NextToken key is Not present")

            if len(queue_summary) !=0:
                logger.info(f"Items in queue List: {len(queue_summary)}")
                queue_ids =[]
                queue_id_name_dict ={}
                for dict_item in queue_summary:
                    queue_ids.append(dict_item['Id'])
                    queue_id_name_dict[dict_item['Id']] = dict_item['Name']
                logger.info(f"Queue_dict map: {queue_id_name_dict}")
                ac_queue_metrics(queue_id_name_dict,queue_ids,instance_id)

    except Exception as e:
        raise e


def ac_queue_metrics(queue_id_name_dict,queue_ids, instance_id):
    try:

        logger.info("Start ac_queue_metrics")
        logger.info(f"Queues : {queue_ids}")
        next_token = 'NoToken'

        queuemetics_max_result = os.environ['AMAZON_CONNECT_QUEUEMETRICS_MAX_RESULT']
        current_metrics = [
                    { 'Name': 'AGENTS_ONLINE', 'Unit': 'COUNT' },
                    { 'Name': 'AGENTS_AVAILABLE', 'Unit': 'COUNT' },
                    { 'Name': 'AGENTS_ON_CONTACT', 'Unit': 'COUNT' },
                    { 'Name': 'AGENTS_STAFFED', 'Unit': 'COUNT' },
                    { 'Name': 'AGENTS_AFTER_CONTACT_WORK', 'Unit': 'COUNT' },
                    { 'Name': 'AGENTS_NON_PRODUCTIVE', 'Unit': 'COUNT' },
                    { 'Name': 'AGENTS_ERROR', 'Unit': 'COUNT' },
                    { 'Name': 'CONTACTS_IN_QUEUE', 'Unit': 'COUNT' },
                    { 'Name': 'OLDEST_CONTACT_AGE', 'Unit': 'SECONDS' },
                    { 'Name': 'CONTACTS_SCHEDULED', 'Unit': 'COUNT' },
                ]

        while len(next_token)!=0:
            if next_token == 'NoToken':
                logger.info("Call QueueMetric : without no token")
                currentMetrics_data = connect.get_current_metric_data(InstanceId = instance_id,
                    Filters = {'Channels': ['VOICE', 'CHAT', 'TASK'], 'Queues': queue_ids},
                    Groupings = ['QUEUE'],
                    CurrentMetrics = current_metrics,
                    MaxResults = int(queuemetics_max_result)
                    )
            else:
                logger.info("Call QueueMetric : with token")
                currentMetrics_data = connect.get_current_metric_data(InstanceId = instance_id,
                    Filters = {'Channels': ['VOICE', 'CHAT', 'TASK'], 'Queues': queue_ids},
                    Groupings = ['QUEUE'],
                    CurrentMetrics = current_metrics,
                    MaxResults = int(queuemetics_max_result),
                    NextToken = next_token
                    )
            logger.info(f"currentMetrics_data: {currentMetrics_data}")

            if 'NextToken' in currentMetrics_data.keys():
                next_token = currentMetrics_data['NextToken']
                logger.info(f"NextToken: {next_token}")
            else:
                next_token=''
                logger.info("NextToke key is Not present")

            if 'MetricResults' in currentMetrics_data.keys():
                metricresults_data = currentMetrics_data['MetricResults']
                logger.info(f"metricresults_data: {metricresults_data}")
            else:
                metricresults_data=[]
                logger.info("MetricResults key is Not present")

            i =0


            sf = Salesforce()

            if len(metricresults_data) !=0:
                while i < len(metricresults_data):
                    queue_metics_data_dict ={}
                    data = metricresults_data[i]
                    logger.info('*********')
                    logger.info(f'Data : {i} ***  {data}')
                    if 'Dimensions' in data.keys():
                        dimensions_data = data['Dimensions']
                        if 'Queue' in dimensions_data.keys():
                            queue_data = dimensions_data['Queue']
                            queue_metics_data_dict['queue_id'] = queue_data['Id']
                            queue_metics_data_dict['queue_arn'] = queue_data['Arn']
                            logger.info(f"queue id : {queue_data['Id']}")
                            logger.info(f"queue ARN : {queue_data['Arn']}")
                            logger.info('*********')
                            logger.info('*********')

                    if 'Collections' in data.keys():
                        collections_data = data['Collections']
                        logger.info(f'collections_data : {i} ***  {collections_data}')
                        j = 0
                        while j < len(collections_data):
                            logger.info(f"J : {j}")
                            metrics_data = collections_data[j]
                            logger.info(metrics_data)

                            if 'Metric' in metrics_data.keys():
                                metric_data = metrics_data['Metric']
                                if metric_data['Name'] == 'AGENTS_ONLINE' and 'Value' in metrics_data.keys() :
                                    agent_online = int(metrics_data['Value'])
                                    queue_metics_data_dict['agent_online'] = agent_online
                                elif metric_data['Name'] == 'AGENTS_AVAILABLE' and 'Value' in metrics_data.keys() :
                                    agent_available = int(metrics_data['Value'])
                                    queue_metics_data_dict['agent_available'] = agent_available
                                elif metric_data['Name'] == 'AGENTS_ON_CONTACT' and 'Value' in metrics_data.keys() :
                                    agent_on_call = int(metrics_data['Value'])
                                    queue_metics_data_dict['agent_on_call'] = agent_on_call
                                elif metric_data['Name'] == 'AGENTS_STAFFED' and 'Value' in metrics_data.keys() :
                                    agent_staffed = int(metrics_data['Value'])
                                    queue_metics_data_dict['agent_staffed'] = agent_staffed
                                elif metric_data['Name'] == 'AGENTS_AFTER_CONTACT_WORK' and 'Value' in metrics_data.keys() :
                                    agent_awc = int(metrics_data['Value'])
                                    queue_metics_data_dict['agent_awc'] = agent_awc
                                elif metric_data['Name'] == 'AGENTS_NON_PRODUCTIVE' and 'Value' in metrics_data.keys() :
                                    agent_non_productive = int(metrics_data['Value'])
                                    queue_metics_data_dict['agent_non_productive'] = agent_non_productive
                                elif metric_data['Name'] == 'AGENTS_ERROR' and 'Value' in metrics_data.keys() :
                                    agent_error = int(metrics_data['Value'])
                                    queue_metics_data_dict['agent_error'] = agent_error
                                elif metric_data['Name'] == 'CONTACTS_IN_QUEUE' and 'Value' in metrics_data.keys() :
                                    contacts_in_queue = int(metrics_data['Value'])
                                    queue_metics_data_dict['contacts_in_queue'] = contacts_in_queue
                                elif metric_data['Name'] == 'OLDEST_CONTACT_AGE' and 'Value' in metrics_data.keys() :
                                    oldest_contact_age = int(metrics_data['Value'])
                                    queue_metics_data_dict['oldest_contact_age'] = oldest_contact_age
                                elif metric_data['Name'] == 'CONTACTS_SCHEDULED' and 'Value' in metrics_data.keys() :
                                    contacts_scheduled = int(metrics_data['Value'])
                                    queue_metics_data_dict['contacts_scheduled'] = contacts_scheduled
                            j = j + 1

                        sObjectData = prepare_record(queue_id_name_dict,queue_metics_data_dict)
                        sQueueId = queue_metics_data_dict['queue_id']
                        # If Region__c exists from Salesforce org, then multi-region is supported. Need to append the region to the Salesforce Queue Id
                        if sf.isFieldInSObject(objectnamespace + 'AC_QueueMetrics__c', objectnamespace + 'Region__c'):
                            logger.info("Multi-region enabled")
                            session = boto3.session.Session()
                            sObjectData[objectnamespace + 'Region__c'] = session.region_name
                            sQueueId = sQueueId + '-' + session.region_name

                        sf.update_by_external(objectnamespace + "AC_QueueMetrics__c", objectnamespace + 'Queue_Id__c', sQueueId, sObjectData)

                        i = i + 1

            logger.info("End ac_queue_metrics method")

    except Exception as e:
        raise e


def prepare_record(queue_id_name_dict,queue_metric_data):
    logger.info("prepare record method")
    logger.info(f"Queue Name: {queue_id_name_dict[queue_metric_data['queue_id']]}")
    logger.info(f"Print data : {queue_metric_data}")
    record = {}
    record['Name'] = queue_id_name_dict[queue_metric_data['queue_id']]
    if 'queue_id' in queue_metric_data.keys():

        if 'queue_arn' in queue_metric_data.keys():
            record[objectnamespace + 'Queue_ARN__c'] = queue_metric_data['queue_arn']
        else:
            record[objectnamespace + 'Queue_ARN__c'] = ''


        if 'agent_awc' in queue_metric_data.keys():
            record[objectnamespace + 'Agents_After_Contact_Work__c'] = queue_metric_data['agent_awc']
        else:
            record[objectnamespace + 'Agents_After_Contact_Work__c'] = 0

        if 'agent_available' in queue_metric_data.keys():
            record[objectnamespace + 'Agents_Available__c'] = queue_metric_data['agent_available']
        else:
            record[objectnamespace + 'Agents_Available__c'] = 0

        if 'agent_error' in queue_metric_data.keys():
            record[objectnamespace + 'Agents_Error__c'] = queue_metric_data['agent_error']
        else:
            record[objectnamespace + 'Agents_Error__c'] = 0

        if 'agent_non_productive' in queue_metric_data.keys():
            record[objectnamespace + 'Agents_Non_Productive__c'] = queue_metric_data['agent_non_productive']
        else:
            record[objectnamespace + 'Agents_Non_Productive__c'] = 0

        if 'agent_on_call' in queue_metric_data.keys():
            record[objectnamespace + 'Agents_On_Call__c'] = queue_metric_data['agent_on_call']
        else:
            record[objectnamespace + 'Agents_On_Call__c'] = 0

        if 'agent_online' in queue_metric_data.keys():
            record[objectnamespace + 'Agents_Online__c'] = queue_metric_data['agent_online']
        else:
            record[objectnamespace + 'Agents_Online__c'] = 0

        if 'agent_staffed' in queue_metric_data.keys():
            record[objectnamespace + 'Agents_Staffed__c'] = queue_metric_data['agent_staffed']
        else:
            record[objectnamespace + 'Agents_Staffed__c'] = 0

        if 'contacts_in_queue' in queue_metric_data.keys():
            record[objectnamespace + 'Contacts_In_Queue__c'] = queue_metric_data['contacts_in_queue']
        else:
            record[objectnamespace + 'Contacts_In_Queue__c'] = 0

        if 'contacts_scheduled' in queue_metric_data.keys():
            record[objectnamespace + 'Contacts_Scheduled__c'] = queue_metric_data['contacts_scheduled']
        else:
            record[objectnamespace + 'Contacts_Scheduled__c'] = 0

        if 'oldest_contact_age' in queue_metric_data.keys():
            record[objectnamespace + 'Oldest_Contact_Age__c'] = queue_metric_data['oldest_contact_age']
        else:
            record[objectnamespace + 'Oldest_Contact_Age__c'] = 0



    logger.info(f"record data : {record}")

    return record