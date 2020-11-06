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

import logging, os, json, phonenumbers
from salesforce import Salesforce
from datetime import datetime, timedelta
from sf_util import parse_date, text_replace_string
logger = logging.getLogger()
logger.setLevel(logging.getLevelName(os.environ["LOGGING_LEVEL"]))

def removekey(d, key):
    r = dict(d)
    del r[key]
    return r

def lambda_handler(event, context):
  logger.info("event: %s" % json.dumps(event))
  sf = Salesforce()
  sf.sign_in()

  sf_operation = str(event['Details']['Parameters']['sf_operation'])
  parameters = dict(event['Details']['Parameters'])
  del parameters['sf_operation']
  event['Details']['Parameters'] = parameters

  if(sf_operation == "lookup"):
    resp = lookup(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "create"):
    resp = create(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "update"):
    resp = update(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "phoneLookup"):
    resp = phoneLookup(sf, event['Details']['Parameters']['sf_phone'], event['Details']['Parameters']['sf_fields'])
  elif (sf_operation == "delete"):
    resp = delete(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "lookup_all"):
    resp = lookup_all(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "query"):
    resp = query(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "queryOne"):
    resp = queryOne(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "createChatterPost"):
    resp = createChatterPost(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "createChatterComment"):
    resp = createChatterComment(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "search"):
    resp = search(sf=sf, **event['Details']['Parameters'])
  elif (sf_operation == "searchOne"):
    resp = searchOne(sf=sf, **event['Details']['Parameters'])
  else:
    msg = "sf_operation unknown"
    logger.error(msg)
    raise Exception(msg)
  
  logger.info("result: %s" % resp)
  return resp

# ****WARNING**** -- this function will be deprecated in future versions of the integration; please use search/searchOne.
def lookup(sf, sf_object, sf_fields, **kwargs):
  where = " AND ".join([where_parser(*item) for item in kwargs.items()])
  query = "SELECT %s FROM %s WHERE %s" % (sf_fields, sf_object, where)
  records = sf.query(query=query)
  count = len(records)
  result = records[0] if count > 0 else {}
  result['sf_count'] = count
  return result
    
def where_parser(key, value):
  if key.lower() in ['mobilephone', 'homephone']:
    return "%s LIKE '%%%s%%%s%%%s%%'" % (key, value[-10:-7], value[-7:-4], value[-4:])
    
  if "%" in value:
    return "%s LIKE '%s'" % (key, value)

  return "%s='%s'" % (key, value)

def create(sf, sf_object, **kwargs):
  data = {k:parse_date(v) for k,v in kwargs.items()}
  return {'Id':sf.create(sobject=sf_object, data=data)}

def update(sf, sf_object, sf_id, **kwargs):
  data = {k:parse_date(v) for k,v in kwargs.items()}
  return {'Status':sf.update(sobject=sf_object, sobj_id=sf_id, data=data)}

def phoneLookup(sf, phone, sf_fields):
  phone_national = str(phonenumbers.parse(phone, None).national_number)

  data = {
    'q':phone_national,
    'sobjects':[{'name': 'Contact'}],
    'fields': sf_fields.split(", ") if isinstance(sf_fields, str) else sf_fields
  }
  records = sf.parameterizedSearch(data=data)

  count = len(records)
  
  if (count > 0):
    result = records[0]   
  else:
    result = {}

  result['sf_count'] = count
  return result

def delete(sf, sf_object, sf_id):
  return {'Response': sf.delete(sobject=sf_object, sobject_id=sf_id)}

# ****WARNING**** -- this function will be deprecated in future versions of the integration; please use search/searchOne.
def lookup_all(sf, sf_object, sf_fields, **kwargs):
  where = " AND ".join([where_parser(*item) for item in kwargs.items()])
  query_filter = (" WHERE" + where) if kwargs.__len__() > 0 else ''
  query = "SELECT %s FROM %s  %s" % (sf_fields, sf_object, query_filter)
  records = sf.query(query=query)
  return records

# ****WARNING**** -- this function will be deprecated in future versions of the integration; please use search/searchOne.
def query(sf, query, **kwargs):
  for key, value in kwargs.items():
    logger.info("Replacing [%s] with [%s] in [%s]" % (key, value, query))
    query = query.replace(key, value)

  records = sf.query(query=query)
  count = len(records)
  result = {}
  
  if count > 0:
    recordArray = []
    for record in records :
      recordArray.append(flatten_json(record))

    result['sf_records'] = recordArray
  else:
    result['sf_records'] = []

  result['sf_count'] = count
  return result

# ****WARNING**** -- this function will be deprecated in future versions of the integration; please use search/searchOne.
def queryOne(sf, query, **kwargs):
  for key, value in kwargs.items():
    logger.info("Replacing [%s] with [%s] in [%s]" % (key, value, query))
    query = query.replace(key, value)

  records = sf.query(query=query)
  count = len(records)
  result = flatten_json(records[0]) if count == 1 else {}
  result['sf_count'] = count
  return result

def createChatterPost(sf, sf_feedElementType, sf_subjectId, sf_messageType, sf_message, **kwargs):
  formatted_message = text_replace_string(sf_message, kwargs)
  logger.info('Formatted message: %s', formatted_message)

  data = {'sf_feedElementType': sf_feedElementType,
          'sf_subjectId': sf_subjectId,
          'sf_messageType': sf_messageType,
          'sf_message': formatted_message,
          'sf_mention': kwargs.get('sf_mention','')}
    
  return {'Id': sf.createChatterPost(data)}


def createChatterComment(sf, sf_feedElementId, sf_commentType, sf_commentMessage, **kwargs):
  formatted_message = text_replace_string(sf_commentMessage, kwargs)
  logger.info('Formatted message: %s', formatted_message)

  data = {'sf_feedElementId': sf_feedElementId,
          'sf_commentType': sf_commentType,
          'sf_commentMessage': formatted_message}

  return {'Id': sf.createChatterComment(sfeedElementId=sf_feedElementId, data=data)}

def search(sf, q, sf_fields, sf_object, where="", overallLimit=100, **kwargs):
  obj = [ { 'name': sf_object } ]
  if where:
    obj[0]['where'] = where
  
  data = {
    'q':q,
    'fields': sf_fields.split(', '),
    'sobjects': obj,
    'overallLimit': overallLimit
  }
  records = sf.parameterizedSearch(data=data)

  count = len(records)
  result = {}
  
  if count > 0:
    recordArray = []
    for record in records:
      recordArray.append(flatten_json(record))

    result['sf_records'] = recordArray
  else:
    result['sf_records'] = []

  result['sf_count'] = count
  return result

def searchOne(sf, q, sf_fields, sf_object, where="", **kwargs):
  obj = [ { 'name': sf_object } ]
  if where:
    obj[0]['where'] = where
  
  data = {
    'q':q,
    'fields': sf_fields.split(', '),
    'sobjects': obj
  }
  records = sf.parameterizedSearch(data=data)
  count = len(records)
  result = flatten_json(records[0]) if count == 1 else {}
  result['sf_count'] = count
  return result

def flatten_json(nested_json):
  out = {}
    
  def flatten(x, name=''):
    if type(x) is dict:
      for a in x:
        flatten(x[a], name + a + '.')
    elif type(x) is list:
      i = 0
      for a in x: 
        flatten(a, name)
        i += 1
    else:
      out[name[:-1]] = x

  flatten(nested_json)
  return out 