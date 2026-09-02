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

import json, os
import time
import requests
import boto3
import datetime
from botocore.exceptions import ClientError
from sf_util import get_arg
from log_util import logger, sanitize_log

# Salesforce errorCodes that mean "authenticated but not authorised". A 403 alone is
# not enough to infer a permissions fault: FUNCTIONALITY_NOT_ENABLED (feature not
# licensed) and REQUEST_LIMIT_EXCEEDED (API limit) also arrive as 403, and both were
# observed as 403 against a live sandbox.
ACCESS_DENIED_ERROR_CODES = frozenset((
  'INSUFFICIENT_ACCESS',
  'INSUFFICIENT_ACCESS_OR_READONLY',
  'INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY',
))

# Neither the status nor the errorCode is sufficient on its own. Measured against a live
# sandbox: removing AC_Administrator from the client-credentials Run As user and creating
# an AC_ContactTraceRecord__c returns
#   400 [{"errorCode": "FIELD_CUSTOM_VALIDATION_EXCEPTION",
#         "message": "Insufficient Privileges You do not have the level of access ..."}]
# The adapter's own triggers on that object intercept the access failure and re-raise it,
# so the real production denial carries neither a 403 nor an INSUFFICIENT_* code, and
# Salesforce's standard insufficient-privileges wording is the only reliable signal.
# A custom validation rule whose own message contains this wording would also match; that
# is an acceptable trade for not missing the case this guidance exists to explain.
ACCESS_DENIED_MESSAGE_FRAGMENTS = (
  'insufficient privileges',
  'level of access necessary',
)

def is_access_denied(error):
  """True when a data-API error means the identity lacks permission, not that the
  request was malformed. Keyed on errorCode and message because Salesforce reports the
  same condition several ways depending on whether a trigger intercepted it."""
  error = error or {}
  message = (error.get('message') or '').lower()
  return (error.get('errorCode') in ACCESS_DENIED_ERROR_CODES
          or any(fragment in message for fragment in ACCESS_DENIED_MESSAGE_FRAGMENTS))

class Salesforce:

  def __init__(self):
    session = boto3.session.Session()
    self.secrets = {}
    self.secrets_manager_client = session.client(
      service_name="secretsmanager"
    )
    self.cloudwatch_client = session.client(
      service_name="cloudwatch"
    )
    self.metric_namespace = os.environ.get("METRIC_NAMESPACE", "AmazonConnectSalesforceLambda")
    self.stack_name = os.environ.get("STACK_NAME", os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown"))
    self.sf_credentials_secrets_manager_arn = get_arg(os.environ, "SF_CREDENTIALS_SECRETS_MANAGER_ARN")

    self.__load_credentials()
    self.version=get_arg(os.environ, "SF_VERSION")
    self.host=get_arg(os.environ, "SF_HOST")

    self.login_host = self.host
    self.request = Request()
    self.auth_mode = os.environ.get("SF_AUTH_MODE", "password").lower()

    if self.auth_mode == "password":
      self.__init_password()
    elif self.auth_mode == "client_credentials":
      self.__init_client_credentials()
    else:
      raise Exception("Invalid SF_AUTH_MODE '%s'. Valid values are 'password' and 'client_credentials'." % sanitize_log(self.auth_mode))

    if get_arg(os.environ, "SF_PRODUCTION").lower() == "true":
      self.set_production()

  def __check_connected_app_credentials(self):
    # Both grants send the connected app's key/secret, so validate the attributes
    # that actually reach auth_data rather than the raw secret payload.
    if not self.consumer_key:
      raise Exception("'ConsumerKey' is missing from the Salesforce credentials secret")
    if not self.consumer_secret:
      raise Exception("'ConsumerSecret' is missing from the Salesforce credentials secret")

  def __init_password(self):
    # SF_USERNAME and the Password/AccessToken pair are only meaningful for the
    # password grant, so they are read here rather than for every auth mode.
    self.__check_connected_app_credentials()
    self.username = get_arg(os.environ, "SF_USERNAME")
    self.password = self.secrets["Password"] + self.secrets["AccessToken"]
    self.auth_data = {
      'grant_type': 'password',
      'client_id': self.consumer_key,
      'client_secret': self.consumer_secret,
      'username': self.username,
      'password': self.password
    }

  def __init_client_credentials(self):
    # The client credentials flow authenticates as the connected app's integration
    # ("Run As") user, so there is no username, password or security token here.
    self.__check_connected_app_credentials()
    self.auth_data = {
      'grant_type': 'client_credentials',
      'client_id': self.consumer_key,
      'client_secret': self.consumer_secret
    }

  def __load_credentials(self):
    logger.info("Loading credentials")
    self.secrets = json.loads(self.secrets_manager_client.get_secret_value(SecretId=self.sf_credentials_secrets_manager_arn)["SecretString"])

    self.consumer_key = self.secrets.get("ConsumerKey")
    self.consumer_secret = self.secrets.get("ConsumerSecret")
    self.auth_token = self.secrets["AuthToken"] if "AuthToken" in self.secrets else ''
    self.headers = { 
      'Authorization': 'Bearer %s' % self.auth_token,
      'Content-Type': 'application/json'
    }
    logger.info("Credentials Loaded")

  def __publish_metric(self, metric_name):
    # Best-effort publish of an operational metric to the customer's CloudWatch.
    # Failures here must never disrupt the caller's Salesforce request flow.
    try:
      self.cloudwatch_client.put_metric_data(
        Namespace=self.metric_namespace,
        MetricData=[{
          'MetricName': metric_name,
          'Dimensions': [{'Name': 'StackName', 'Value': self.stack_name}],
          'Value': 1,
          'Unit': 'Count'
        }]
      )
    except Exception as metric_err:
      logger.error("Failed to publish metric %s: %s" % (metric_name, str(metric_err)))

  def set_production(self):
    # Salesforce does not support login.salesforce.com/test.salesforce.com for the
    # client credentials flow; the token request must go to the org's My Domain host.
    if self.auth_mode == "client_credentials":
      logger.info("Client credentials flow: keeping SF_HOST as the token endpoint host")
      return
    self.login_host = 'https://login.salesforce.com'

  def __handle_auth_error(self, data):
    error = data.get('error', '')
    description = data.get('error_description', '')
    lowered = description.lower()

    # Order matters: Salesforce returns "no client credentials user enabled" for a
    # missing Run As user and "client credentials flow not enabled" for an unchecked
    # flow. Both contain "client credentials", so the narrower test must come first
    # or the two cases report each other's guidance.
    if error == 'invalid_grant' and 'no client credentials user enabled' in lowered:
      msg = "Salesforce rejected the client credentials request: the connected app has no integration ('Run As') user configured. Set one on the app's OAuth policies."
    elif error == 'invalid_grant' and 'client credentials' in lowered:
      msg = "Salesforce rejected the client credentials request: 'Enable Client Credentials Flow' is not checked on the connected app."
    elif error == 'invalid_client_id':
      msg = "Salesforce did not recognize the client id: 'ConsumerKey' in the credentials secret does not match a valid connected app."
    elif error == 'invalid_client':
      msg = "Salesforce rejected the client secret: 'ConsumerSecret' in the credentials secret is incorrect."
    else:
      msg = "%s: %s" % (error, description)

    logger.error(sanitize_log(msg))
    raise Exception(msg)

  def search(self, query):
    logger.info("Salesforce: Search")
    url = '%s/services/data/%s/search' % (self.host, self.version)
    resp = self.makeRequest(self.request.get, **{"url": url, "params":{'q':query}})
    return resp.json()['searchRecords']

  def describe_sObject(self, sobject):
    logger.info("Salesforce: DescribeSObject")
    url = '%s/services/data/%s/sobjects/%s/describe' % (self.host, self.version, sobject)
    resp = self.makeRequest(self.request.get, **{"url": url, "params": {}})
    return resp.json()

  def isFieldInSObject(self, sobject, field):
    logger.info("Salesforce: DescribeSObject field")
    url = '%s/services/data/%s/sobjects/%s/describe' % (self.host, self.version, sobject)
    resp = self.makeRequest(self.request.get, **{"url": url, "params": {}})
    fields = resp.json()['fields']
    logger.info("Describe SObject Fields: " + str(fields))

    #Check if field is on the list of fields for the specified sObject
    for sf_field in fields:
        if sf_field['name'] == field:
            return True
    return False

  def query(self, query):#TODO: create generator that takes care of subsequent request for more than 200 records
    logger.info("Salesforce: Query")
    url = '%s/services/data/%s/query' % (self.host, self.version)
    resp = self.makeRequest(self.request.get, **{"url": url, "params":{'q':query}})
    data = resp.json()
    for record in data['records']:
        del record['attributes']
    return data['records']

  def parameterizedSearch(self, data):#TODO: create generator that takes care of subsequent request for more than 200 records
    logger.info("Salesforce: Query")
    url = '%s/services/data/%s/parameterizedSearch' % (self.host, self.version)
    resp = self.makeRequest(self.request.post, **{"url": url, "data": data})
    data = resp.json()

    for record in data['searchRecords']:
        del record['attributes']
    return data['searchRecords']

  def update(self, sobject, sobj_id, data):
    logger.info("Salesforce: Update")
    url = '%s/services/data/%s/sobjects/%s/%s' % (self.host, self.version, sobject, sobj_id)
    resp = self.makeRequest(self.request.patch, **{"url": url, "data": data})
    return resp.status_code

  def update_by_external(self, sobject, field, sobj_id, data):
    logger.info("Salesforce: Update by external")
    url = '%s/services/data/%s/sobjects/%s/%s/%s' % (self.host, self.version, sobject, field, sobj_id)
    self.makeRequest(self.request.patch, **{"url": url, "data": data})

  def create(self, sobject, data):
    logger.info("Salesforce: Create")
    url = '%s/services/data/%s/sobjects/%s' % (self.host, self.version, sobject)
    resp = self.makeRequest(self.request.post, **{"url": url, "data": data})
    return resp.json()['id']

  def delete(self, sobject, sobject_id):
    logger.info("Salesforce: Delete")
    url = '%s/services/data/%s/sobjects/%s/%s' % (self.host, self.version, sobject, sobject_id)
    resp = self.makeRequest(self.request.delete, **{"url": url})

  def is_authenticated(self):
    return self.auth_token and self.host

  def createChatterPost(self, data):
    logger.info("Salesforce: CreatePost" )
    url = '%s/services/data/%s/chatter/feed-elements' % (self.host, self.version)

    if not data['sf_mention'] == "" and not data['sf_mention'] == None:

      data = {
          'body' : {
          'messageSegments' : [
          {
            'type' : data['sf_messageType'],
            'text' : data['sf_message']
          },
          {
            'type' : 'Mention',
            'id' : data['sf_mention']
          }]
        },
        'feedElementType' : data['sf_feedElementType'],
        'subjectId' : data['sf_subjectId']
      }
    else:
      data = {
          'body' : {
          'messageSegments' : [
          {
            'type' : data['sf_messageType'],
            'text' : data['sf_message']
          }]
        },
        'feedElementType' : data['sf_feedElementType'],
        'subjectId' : data['sf_subjectId']
      }
    resp = self.makeRequest(self.request.post, **{"url": url, "data": data})
    return resp.json()['id']

  def createChatterComment(self, sfeedElementId, data):
    logger.info("Salesforce: CreateComment" )
    url = '%s/services/data/%s/chatter/feed-elements/%s/capabilities/comments/items' % (self.host, self.version, sfeedElementId)
    data = {
        'body' : {
        'messageSegments' : [
        {
          'type' : data['sf_commentType'],
          'text' : data['sf_commentMessage']
        }]
      }
    }
    resp = self.makeRequest(self.request.post, **{"url": url, "data": data})
    return resp.json()['id']
  
  def __put_secret_value_with_retry(self, max_retries=3):
    """
    Persist the refreshed OAuth token to Secrets Manager with finite retries
    and exponential backoff on write conflicts from concurrent Lambda invocations.

    ResourceConflictException: concurrent invocations racing to write the same
    secret. Retried up to max_retries times with exponential backoff
    (0.3s, 0.6s, 1.2s — total max 2.1s) before re-raising.
    """
    delay = 0.3  # seconds; doubles each retry: 0.3 → 0.6 → 1.2
    for attempt in range(max_retries + 1):
      try:
        self.secrets_manager_client.put_secret_value(
          SecretId=self.sf_credentials_secrets_manager_arn,
          SecretString=json.dumps(self.secrets)
        )
        return
      except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'LimitExceededException':
          # Version limit exhausted — token is valid in-memory for this invocation
          # but cannot be cached for future invocations until Secrets Manager
          # auto-purges deprecated versions.
          logger.error("Secrets Manager version limit reached, cannot persist token: %s" % sanitize_log(str(e)))
          self.__publish_metric("SecretsManagerLimitExceeded")
          return
        elif error_code == 'ResourceConflictException' and attempt < max_retries:
          logger.warning(
            "Secrets Manager write conflict (attempt %d/%d), retrying in %.1fs: %s"
            % (attempt + 1, max_retries, delay, sanitize_log(str(e)))
          )
          time.sleep(delay)
          delay *= 2
        else:
          raise

  def makeRequest(self, requestMethod, **kwargs):
    try:
      return requestMethod(**kwargs, headers=self.headers)
    except SalesforceAccessDeniedException as denied_err:
      # Deliberately NOT routed into the token-refresh path below. A 403 means the
      # identity is authenticated but unauthorised, so refreshing would burn a
      # Secrets Manager version per call without ever succeeding — the same
      # exhaustion loop the circuit breaker further down exists to prevent.
      status = denied_err.status_code or 'unknown'
      if self.auth_mode == "client_credentials" and is_access_denied(denied_err.data):
        # CC issues the token for the connected app's "Run As" user and runs all DML
        # as that user, so object and field permissions must be granted to it. This
        # failure mode has no password-mode equivalent: reads can succeed on a bare
        # API_ONLY profile while every write fails — measured against a live sandbox.
        logger.error(
          "Salesforce denied the request in client_credentials mode (HTTP %s). Review "
          "Run As user permissions: the connected app's integration ('Run As') user is "
          "missing permissions this operation needs. Assign the AC_Administrator "
          "permission set to that user (amazonconnect__AC_Administrator for a "
          "managed-package install) under Setup > Users > Permission Set Assignments, "
          "and confirm the Run As user on the app's OAuth policies. Note that reads and "
          "deletes can still succeed while creates and updates fail, so partial "
          "function is not evidence the permissions are correct. Underlying error: %s"
          % (status, sanitize_log(str(denied_err)))
        )
      else:
        # Not every forbidden response is a permissions problem —
        # FUNCTIONALITY_NOT_ENABLED (feature not licensed) and REQUEST_LIMIT_EXCEEDED
        # (API limit) both arrive as 403. Emitting Run As guidance for those would send
        # an admin to change permissions for an unrelated fault, so surface Salesforce's
        # own wording instead.
        logger.error("Salesforce denied the request (HTTP %s): %s"
                     % (status, sanitize_log(str(denied_err))))
      self.__publish_metric("SalesforceAccessDenied")
      raise
    except InvalidAuthTokenException:
      # try re-fetching auth token. Naming the grant makes the two flows
      # distinguishable in CloudWatch — otherwise a customer who has switched
      # SalesforceAuthMode cannot tell from the logs which one actually ran.
      logger.info("Retrieving new Salesforce OAuth token (auth mode: %s)" % sanitize_log(self.auth_mode))
      headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
      try:
        if self.auth_mode == "client_credentials":
          # Salesforce forbids sensitive values in query strings, so the client
          # credentials are sent form-encoded in the request body.
          resp = self.request.post(url=self.login_host+"/services/oauth2/token", data=self.auth_data, headers=headers, hideData=True, formEncoded=True)
        else:
          resp = self.request.post(url=self.login_host+"/services/oauth2/token", params=self.auth_data, headers=headers, hideData=True)
      except requests.exceptions.RequestException as network_err:
        # Transport-level failure (timeout, DNS, connection refused). Not an auth
        # problem, so don't emit the auth-failure metric; just re-raise.
        raise network_err
      except SalesforceErrorResponseException as auth_err:
        # The token endpoint returned a structured OAuth error; translate it into
        # actionable guidance before re-raising.
        self.__publish_metric("SalesforceAuthFailure")
        self.__handle_auth_error(auth_err.data)
      except Exception as auth_err:
        # Salesforce returned a response that rejected the credentials (e.g. invalid
        # or expired API user, wrong password/token, IP restriction). Publish a metric
        # so customers can detect authentication failures, then re-raise.
        self.__publish_metric("SalesforceAuthFailure")
        raise auth_err
      data = resp.json()
      self.auth_token = self.secrets["AuthToken"] = data['access_token']
      self.headers = kwargs['headers'] = {
        'Authorization': 'Bearer %s' % self.auth_token,
        'Content-Type': 'application/json'
      }

      # Circuit breaker: verify the fresh token actually works before writing to
      # Secrets Manager. If Salesforce still returns 401 after a successful token
      # refresh, it is a persistent failure, not a transient
      # expiry. Raising here without writing prevents the Secrets Manager version
      # exhaustion loop.
      try:
        result = requestMethod(**kwargs)
      except InvalidAuthTokenException:
        logger.error(
          "Salesforce authentication still failing after token refresh — "
          "the APIUser account is frozen or the token is not being accepted "
          "by the API tier. Not persisting token to Secrets Manager to "
          "prevent secret manager version exhaustion (COE-402979)."
        )
        raise

      # The fresh token is valid — persist it for future invocations.
      # Non-fatal: a failure here (e.g. ResourceConflictException exhausted,
      # AccessDeniedException) must not discard a Salesforce mutation that already
      # committed in the circuit-breaker test call above. Failing to cache the
      # token only costs an extra OAuth refresh on the next invocation.
      try:
        self.__put_secret_value_with_retry()
      except ClientError as persist_err:
        logger.error("Failed to persist refreshed token to Secrets Manager (non-fatal): %s" % sanitize_log(str(persist_err)))
      return result

class Request:
  def post(self, url, headers, data=None, params=None, hideData=False, formEncoded=False):
    logger.info('POST Requests: url=%s' % sanitize_log(url))
    if not hideData:
      logger.info("data=%s params=%s" % (sanitize_log(str(data)), sanitize_log(str(params))))
    # formEncoded sends data as an application/x-www-form-urlencoded body instead of a
    # JSON string; opt-in only, so every existing caller keeps the JSON body it had.
    if formEncoded:
      r = requests.post(url=url, data=data, params=params, headers=headers)
    else:
      r = requests.post(url=url, data=json.dumps(data), params=params, headers=headers)
    if not hideData:
      logger.info("Response: %s" % sanitize_log(r.text))
    return __check_resp__(r)

  def delete(self, url, headers):
    logger.info("DELETE Requests: url=%s" % sanitize_log(url))
    r = requests.delete(url=url, headers=headers)
    logger.info("Response: %s" % sanitize_log(r.text))
    return __check_resp__(r)

  def patch(self, url, data, headers):
    logger.info("PATCH Requests: url=%s data=%s" % (sanitize_log(url), sanitize_log(str(data))))
    r = requests.patch(url=url, data=json.dumps(data), headers=headers)
    logger.info("Response: %s" % sanitize_log(r.text))
    return __check_resp__(r)

  def get(self, url, params, headers):
    logger.info("GET Requests: url=%s params=%s" % (sanitize_log(url), sanitize_log(str(params))))
    r = requests.get(url=url, params=params, headers=headers)
    logger.info("Response: %s" % sanitize_log(r.text))
    return __check_resp__(r)

def __check_resp__(resp):
  if resp.status_code // 100 == 2: 
    return resp
  
  if resp.status_code == 401:
    raise InvalidAuthTokenException("")
  
  data = resp.json()
  if 'error' in data:
    msg = "%s: %s" % (data['error'], data['error_description'])
    # Response-derived, so sanitize before logging (CWE-117): error_description is
    # attacker- or admin-influenced text and can carry newlines to forge log lines.
    logger.error(sanitize_log(msg))
    # Subclass of Exception so existing handlers behave identically, but it carries the
    # parsed body so the auth path can turn OAuth errors into actionable guidance.
    raise SalesforceErrorResponseException(msg, data)
  
  if isinstance(data, list):
    for error in data:
      if 'message' in error:
        msg = "%s: %s" % (error['errorCode'], error['message'])
        logger.error(sanitize_log(msg))
        # Order matters: this branch sits after the 'error' in data check above so
        # token-endpoint OAuth errors keep routing to SalesforceErrorResponseException
        # and __handle_auth_error. Only the list-shaped data-API bodies reach here.
        #
        # Not keyed on the status alone: the real insufficient-access response from this
        # adapter's objects is a 400, measured live (see ACCESS_DENIED_MESSAGE_FRAGMENTS).
        # 403 is kept so other forbidden responses still reach the caller as a
        # distinguishable type, even when they are not permissions faults.
        if resp.status_code == 403 or is_access_denied(error):
          raise SalesforceAccessDeniedException(msg, error, resp.status_code)
        raise Exception(msg)

  msg = "request returned status code: %d" % resp.status_code
  logger.error(msg)
  raise Exception(msg)

class InvalidAuthTokenException(Exception):
  pass

class SalesforceErrorResponseException(Exception):
  def __init__(self, message, data):
    super().__init__(message)
    self.data = data

class SalesforceAccessDeniedException(Exception):
  # An authorisation failure from the data API: the identity authenticated, but the
  # operation was refused. Deliberately not named "Forbidden" — the measured production
  # case is an HTTP 400, not a 403 (see ACCESS_DENIED_MESSAGE_FRAGMENTS). Distinct from
  # InvalidAuthTokenException (401) because it is not a transient expiry, so retrying or
  # refreshing the token cannot fix it. Carries the parsed body so makeRequest, which
  # knows auth_mode, can add mode-specific guidance, and the status so the log can report
  # what Salesforce actually returned rather than assuming one.
  def __init__(self, message, data, status_code=None):
    super().__init__(message)
    self.data = data
    self.status_code = status_code
