"""
Unit tests for the SF_AUTH_MODE auth-mode branching in salesforce.py.

Covers the OAuth 2.0 client credentials flow added alongside the existing
username-password (ROPC) grant, selected by the SalesforceAuthMode template
parameter via the SF_AUTH_MODE environment variable.

Tests cover:
1. auth_data construction per mode — CC must not carry username/password
2. A credentials-only secret (no Password/AccessToken) initialises in CC mode
3. Fail-fast on a missing ConsumerKey/ConsumerSecret, or an invalid mode
4. set_production() — CC keeps the My Domain host; password keeps legacy behaviour
5. Token-request transport — CC sends a form-encoded body, password keeps query params
6. __handle_auth_error classification, including the load-bearing branch order

Unlike test_salesforce_circuit_breaker.py, these tests exercise __init__ itself,
so boto3 clients and the environment are patched rather than hand-assembled.
"""

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add the lambda_functions directory to the path so we can import salesforce
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambda_functions'))

MY_DOMAIN = 'https://example--sandbox.sandbox.my.salesforce.com'

FULL_SECRET = {
    'Password': 'pass',
    'AccessToken': 'securitytoken',
    'ConsumerKey': 'consumer-key',
    'ConsumerSecret': 'consumer-secret',
    'AuthToken': 'cached-token',
}

# What a customer using client credentials actually stores: no password, no
# security token. The whole point of the flow.
CC_ONLY_SECRET = {
    'ConsumerKey': 'consumer-key',
    'ConsumerSecret': 'consumer-secret',
    'AuthToken': 'cached-token',
}

BASE_ENV = {
    'SF_CREDENTIALS_SECRETS_MANAGER_ARN': 'arn:aws:secretsmanager:us-east-1:123456789012:secret:test',
    'SF_VERSION': 'v67.0',
    'SF_HOST': MY_DOMAIN,
    'SF_PRODUCTION': 'false',
    'SF_USERNAME': 'apiuser@example.com',
}


def build_salesforce(secret=None, **env_overrides):
    """
    Construct a real Salesforce instance with __init__ running, backed by fake
    boto3 clients and a controlled environment.

    env_overrides values of None remove that variable entirely, which is how the
    "SF_USERNAME is absent" and "SF_AUTH_MODE is unset" cases are expressed.
    """
    from salesforce import Salesforce

    env = dict(BASE_ENV)
    env.update(env_overrides)
    env = {k: v for k, v in env.items() if v is not None}

    secrets_client = MagicMock()
    secrets_client.get_secret_value.return_value = {
        'SecretString': json.dumps(FULL_SECRET if secret is None else secret)
    }

    session = MagicMock()
    session.client.side_effect = lambda service_name: (
        secrets_client if service_name == 'secretsmanager' else MagicMock()
    )

    with patch.dict(os.environ, env, clear=True), \
            patch('boto3.session.Session', return_value=session):
        return Salesforce()


class TestAuthDataConstruction(unittest.TestCase):
    """auth_data must match the grant being requested — nothing more, nothing less."""

    def test_client_credentials_auth_data(self):
        """CC sends exactly grant_type/client_id/client_secret."""
        sf = build_salesforce(SF_AUTH_MODE='client_credentials')

        self.assertEqual(sf.auth_data, {
            'grant_type': 'client_credentials',
            'client_id': 'consumer-key',
            'client_secret': 'consumer-secret',
        })

    def test_client_credentials_omits_user_credentials(self):
        """
        CC must not leak username or password into the token request. Salesforce
        would ignore them, but sending them defeats the point of the flow.
        """
        sf = build_salesforce(SF_AUTH_MODE='client_credentials')

        self.assertNotIn('username', sf.auth_data)
        self.assertNotIn('password', sf.auth_data)

    def test_password_auth_data(self):
        """The password grant keeps all five fields, password being pass+token."""
        sf = build_salesforce(SF_AUTH_MODE='password')

        self.assertEqual(sf.auth_data, {
            'grant_type': 'password',
            'client_id': 'consumer-key',
            'client_secret': 'consumer-secret',
            'username': 'apiuser@example.com',
            'password': 'passsecuritytoken',
        })

    def test_mode_defaults_to_password(self):
        """
        With SF_AUTH_MODE absent, existing deployments must behave exactly as
        before — this is what makes the change non-breaking.
        """
        sf = build_salesforce(SF_AUTH_MODE=None)

        self.assertEqual(sf.auth_mode, 'password')
        self.assertEqual(sf.auth_data['grant_type'], 'password')

    def test_mode_is_case_insensitive(self):
        """A template value of Client_Credentials should not be a silent fallback."""
        sf = build_salesforce(SF_AUTH_MODE='Client_Credentials')

        self.assertEqual(sf.auth_mode, 'client_credentials')
        self.assertEqual(sf.auth_data['grant_type'], 'client_credentials')


class TestCredentialsOnlySecret(unittest.TestCase):
    """
    A CC customer has no password and no security token to store. Reading those
    fields unconditionally made init impossible for exactly the configuration the
    feature exists to support.
    """

    def test_cc_mode_with_no_password_fields(self):
        """Password/AccessToken absent entirely — CC init must still succeed."""
        sf = build_salesforce(secret=CC_ONLY_SECRET, SF_AUTH_MODE='client_credentials')

        self.assertEqual(sf.auth_data['grant_type'], 'client_credentials')

    def test_cc_mode_with_no_sf_username(self):
        """
        SF_USERNAME is fetched via get_arg, which raises when the variable is
        missing, so it must only be read in password mode. A CC deployment may
        legitimately leave SalesforceUsername blank.
        """
        sf = build_salesforce(
            secret=CC_ONLY_SECRET, SF_AUTH_MODE='client_credentials', SF_USERNAME=None)

        self.assertEqual(sf.auth_data['grant_type'], 'client_credentials')

    def test_password_mode_still_requires_password_fields(self):
        """
        The corollary: password mode genuinely needs them, so a credentials-only
        secret must fail rather than silently authenticate as nobody.
        """
        with self.assertRaises(KeyError):
            build_salesforce(secret=CC_ONLY_SECRET, SF_AUTH_MODE='password')

    def test_all_fields_secret_works_in_both_modes(self):
        """A secret holding everything is valid for either mode (mid-migration)."""
        for mode in ('password', 'client_credentials'):
            with self.subTest(mode=mode):
                sf = build_salesforce(SF_AUTH_MODE=mode)
                self.assertEqual(sf.auth_data['grant_type'], mode)


class TestFailFast(unittest.TestCase):
    """Misconfiguration should raise at init with the missing item named."""

    def test_missing_consumer_key(self):
        secret = dict(CC_ONLY_SECRET)
        del secret['ConsumerKey']

        with self.assertRaises(Exception) as ctx:
            build_salesforce(secret=secret, SF_AUTH_MODE='client_credentials')

        self.assertIn('ConsumerKey', str(ctx.exception))

    def test_missing_consumer_secret(self):
        secret = dict(CC_ONLY_SECRET)
        del secret['ConsumerSecret']

        with self.assertRaises(Exception) as ctx:
            build_salesforce(secret=secret, SF_AUTH_MODE='client_credentials')

        self.assertIn('ConsumerSecret', str(ctx.exception))

    def test_empty_consumer_key_is_rejected(self):
        """
        Present-but-empty must fail too: validating the raw secret payload while
        assigning the attribute would let auth_data receive an empty client_id.
        """
        secret = dict(CC_ONLY_SECRET)
        secret['ConsumerKey'] = ''

        with self.assertRaises(Exception) as ctx:
            build_salesforce(secret=secret, SF_AUTH_MODE='client_credentials')

        self.assertIn('ConsumerKey', str(ctx.exception))

    def test_invalid_auth_mode_lists_valid_values(self):
        """An operator who mistypes the parameter needs to be told the options."""
        with self.assertRaises(Exception) as ctx:
            build_salesforce(SF_AUTH_MODE='bogus')

        message = str(ctx.exception)
        self.assertIn('bogus', message)
        self.assertIn('password', message)
        self.assertIn('client_credentials', message)


class TestSetProduction(unittest.TestCase):
    """
    Salesforce does not support login.salesforce.com or test.salesforce.com for
    the client credentials flow — the token request must go to the org's My
    Domain host. set_production() rewrites login_host whenever
    SF_PRODUCTION=true, so without a mode guard CC breaks for every production
    customer. Sandboxes run SF_PRODUCTION=false, so this is not reachable by
    manual sandbox testing and these two tests are the only guard.
    """

    def test_cc_mode_keeps_my_domain_host_in_production(self):
        sf = build_salesforce(SF_AUTH_MODE='client_credentials', SF_PRODUCTION='true')

        self.assertEqual(sf.login_host, MY_DOMAIN)
        self.assertNotEqual(sf.login_host, 'https://login.salesforce.com')

    def test_password_mode_still_rewrites_host_in_production(self):
        """Legacy behaviour must be untouched for the password grant."""
        sf = build_salesforce(SF_AUTH_MODE='password', SF_PRODUCTION='true')

        self.assertEqual(sf.login_host, 'https://login.salesforce.com')

    def test_neither_mode_rewrites_host_when_not_production(self):
        for mode in ('password', 'client_credentials'):
            with self.subTest(mode=mode):
                sf = build_salesforce(SF_AUTH_MODE=mode, SF_PRODUCTION='false')
                self.assertEqual(sf.login_host, MY_DOMAIN)


class TestTokenRequestTransport(unittest.TestCase):
    """
    Salesforce's guidance forbids sensitive values in URL query strings, so the
    CC token request sends its credentials in a form-encoded body. The password
    grant deliberately keeps its existing query-param transport; changing it
    would risk regressing a working flow, so that is asserted too.
    """

    def _refresh_token(self, mode):
        from salesforce import InvalidAuthTokenException

        sf = build_salesforce(SF_AUTH_MODE=mode)
        sf.request = MagicMock()
        sf.request.post.return_value.json.return_value = {'access_token': 'new-token'}

        calls = {'n': 0}

        def api(**kwargs):
            calls['n'] += 1
            if calls['n'] == 1:
                raise InvalidAuthTokenException('expired')
            return 'ok'

        result = sf.makeRequest(api, url='%s/services/data/v67.0/query' % MY_DOMAIN)
        self.assertEqual(result, 'ok')
        return sf.request.post.call_args.kwargs

    def test_client_credentials_sends_body_not_query_string(self):
        kwargs = self._refresh_token('client_credentials')

        self.assertTrue(kwargs.get('formEncoded'))
        self.assertEqual(kwargs['data']['grant_type'], 'client_credentials')
        self.assertIsNone(kwargs.get('params'))

    def test_password_transport_is_unchanged(self):
        kwargs = self._refresh_token('password')

        self.assertFalse(kwargs.get('formEncoded', False))
        self.assertEqual(kwargs['params']['grant_type'], 'password')
        self.assertIsNone(kwargs.get('data'))

    def test_refresh_logs_the_auth_mode(self):
        """
        Both grants hit the same token endpoint, so without the mode in the log line a
        customer who has switched SalesforceAuthMode cannot tell from CloudWatch which
        flow actually ran.
        """
        from salesforce import InvalidAuthTokenException

        for mode in ('password', 'client_credentials'):
            with self.subTest(mode=mode):
                sf = build_salesforce(SF_AUTH_MODE=mode)
                sf.request = MagicMock()
                sf.request.post.return_value.json.return_value = {
                    'access_token': 'new-token'}

                calls = {'n': 0}

                def api(**kwargs):
                    calls['n'] += 1
                    if calls['n'] == 1:
                        raise InvalidAuthTokenException('expired')
                    return 'ok'

                with patch('salesforce.logger') as mock_logger:
                    sf.makeRequest(
                        api, url='%s/services/data/v67.0/query' % MY_DOMAIN)

                logged = ' '.join(str(c) for c in mock_logger.info.call_args_list)
                self.assertIn('auth mode: %s' % mode, logged)

    def test_credentials_are_never_logged(self):
        """
        hideData suppresses body and response logging on the token POST. Without
        it the client_secret and the returned access_token land in CloudWatch.
        """
        for mode in ('password', 'client_credentials'):
            with self.subTest(mode=mode):
                self.assertTrue(self._refresh_token(mode).get('hideData'))


class TestHandleAuthError(unittest.TestCase):
    """
    Each Salesforce token-endpoint error maps to guidance naming the thing the
    admin has to change. The raw errors are opaque: "invalid_grant" alone does
    not distinguish a missing Run As user from a disabled flow.
    """

    def setUp(self):
        self.sf = build_salesforce(SF_AUTH_MODE='client_credentials')
        # Name-mangled private method
        self.handle = self.sf._Salesforce__handle_auth_error

    def _message_for(self, error, description):
        with self.assertRaises(Exception) as ctx:
            self.handle({'error': error, 'error_description': description})
        return str(ctx.exception)

    def test_missing_run_as_user(self):
        message = self._message_for('invalid_grant', 'no client credentials user enabled')

        self.assertIn('Run As', message)

    def test_flow_not_enabled(self):
        message = self._message_for('invalid_grant', 'client credentials flow not enabled')

        self.assertIn('Enable Client Credentials Flow', message)

    def test_branch_order_distinguishes_the_two_invalid_grants(self):
        """
        Both descriptions contain "client credentials", so the narrower test has
        to be evaluated first. Reordering the branches makes each case report the
        other's guidance — a silent regression this test exists to catch.
        """
        no_user = self._message_for('invalid_grant', 'no client credentials user enabled')
        not_enabled = self._message_for('invalid_grant', 'client credentials flow not enabled')

        self.assertNotEqual(no_user, not_enabled)
        self.assertNotIn('Enable Client Credentials Flow', no_user)

    def test_invalid_client_id_points_at_consumer_key(self):
        message = self._message_for('invalid_client_id', 'client identifier invalid')

        self.assertIn('ConsumerKey', message)

    def test_invalid_client_points_at_consumer_secret(self):
        message = self._message_for('invalid_client', 'invalid client credentials')

        self.assertIn('ConsumerSecret', message)

    def test_unrecognised_error_is_passed_through(self):
        """An unmapped error must still surface Salesforce's own wording."""
        message = self._message_for('something_new', 'a description we do not map')

        self.assertIn('something_new', message)
        self.assertIn('a description we do not map', message)

    def test_error_response_reaches_handler_through_make_request(self):
        """
        End-to-end: __check_resp__ raises SalesforceErrorResponseException for an
        error-bodied response, and makeRequest routes it into the mapper. Covers
        the real production path — a plain Exception falls to the generic branch
        instead, which is not what Salesforce actually produces.
        """
        from salesforce import InvalidAuthTokenException, SalesforceErrorResponseException

        sf = build_salesforce(SF_AUTH_MODE='client_credentials')
        sf.request = MagicMock()
        sf.request.post.side_effect = SalesforceErrorResponseException(
            'invalid_grant: no client credentials user enabled',
            {'error': 'invalid_grant',
             'error_description': 'no client credentials user enabled'},
        )

        def api(**kwargs):
            raise InvalidAuthTokenException('expired')

        with self.assertRaises(Exception) as ctx:
            sf.makeRequest(api, url='%s/services/data/v67.0/query' % MY_DOMAIN)

        self.assertIn('Run As', str(ctx.exception))
        # A failed token exchange must not write to Secrets Manager.
        sf.secrets_manager_client.put_secret_value.assert_not_called()


class TestAccessDenied(unittest.TestCase):
    """
    Authenticated-but-unauthorised. In client credentials mode the token is issued for
    the connected app's Run As user and all DML runs as that user, so a missing
    permission set surfaces here. Measured against a live sandbox, reads *and deletes*
    still succeed while creates and updates fail — so partial function is not evidence
    the permissions are right. That failure mode has no password-mode equivalent, which
    is why the guidance is mode-specific.

    Note the status: the real denial is an HTTP 400, not a 403. See LIVE_DENIAL.
    """

    def _make_request_with_denial(self, mode, error_code='INSUFFICIENT_ACCESS',
                                  message='insufficient access rights on object id',
                                  status_code=403):
        from salesforce import SalesforceAccessDeniedException

        sf = build_salesforce(SF_AUTH_MODE=mode)
        sf.request = MagicMock()

        def api(**kwargs):
            raise SalesforceAccessDeniedException(
                '%s: %s' % (error_code, message),
                {'errorCode': error_code, 'message': message},
                status_code,
            )

        with patch('salesforce.logger') as mock_logger:
            with self.assertRaises(SalesforceAccessDeniedException):
                sf.makeRequest(api, url='%s/services/data/v67.0/query' % MY_DOMAIN)

        logged = ' '.join(str(c) for c in mock_logger.error.call_args_list)
        return sf, logged

    def test_denial_logs_run_as_guidance(self):
        """CC mode must name the Run As user and the permission set to assign."""
        _, logged = self._make_request_with_denial('client_credentials')

        self.assertIn('Run As', logged)
        self.assertIn('AC_Administrator', logged)

    def test_denial_cc_mode_says_review_run_as_user_permissions(self):
        """
        The exact phrasing the runbook and the Taskei acceptance criterion name, so
        an admin grepping CloudWatch for it finds the line.
        """
        _, logged = self._make_request_with_denial('client_credentials')

        self.assertIn('Review Run As user permissions', logged)

    def test_denial_names_namespaced_permission_set_for_managed_package(self):
        """
        A managed-package install exposes the permission set as
        amazonconnect__AC_Administrator, so the unqualified name alone would send an
        admin looking for something that does not exist in their org.
        """
        _, logged = self._make_request_with_denial('client_credentials')

        self.assertIn('amazonconnect__AC_Administrator', logged)

    def test_403_functionality_not_enabled_gets_no_run_as_guidance(self):
        """
        Observed against a live sandbox: GET /wave/datasets returns
            403 [{"message": "This feature is not currently enabled for this user
                  type or org: [Wave]", "errorCode": "FUNCTIONALITY_NOT_ENABLED"}]
        A feature-licensing fault, not a permissions one. Emitting Run As guidance here
        would send an admin to change permissions for an unrelated problem, so status
        403 alone must not select the guidance — the errorCode has to agree.
        """
        _, logged = self._make_request_with_denial(
            'client_credentials', error_code='FUNCTIONALITY_NOT_ENABLED',
            message='This feature is not currently enabled for this user type or org: '
                    '[Wave]',
            status_code=403)

        self.assertNotIn('Review Run As user permissions', logged)
        self.assertNotIn('AC_Administrator', logged)
        # Salesforce's own wording still has to reach the operator.
        self.assertIn('FUNCTIONALITY_NOT_ENABLED', logged)
        self.assertIn('HTTP 403', logged)

    def test_403_request_limit_exceeded_gets_no_run_as_guidance(self):
        """The other 403 that is not about permissions: an org API limit."""
        _, logged = self._make_request_with_denial(
            'client_credentials', error_code='REQUEST_LIMIT_EXCEEDED',
            message='TotalRequests Limit exceeded.', status_code=403)

        self.assertNotIn('Review Run As user permissions', logged)
        self.assertIn('REQUEST_LIMIT_EXCEEDED', logged)

    def test_denial_access_denied_codes_all_get_run_as_guidance(self):
        """Every code that does mean access denial must select the guidance."""
        for code in ('INSUFFICIENT_ACCESS',
                     'INSUFFICIENT_ACCESS_OR_READONLY',
                     'INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY'):
            with self.subTest(errorCode=code):
                _, logged = self._make_request_with_denial(
                    'client_credentials', error_code=code)

                self.assertIn('Review Run As user permissions', logged)

    def test_denial_password_mode_has_no_cc_guidance(self):
        """
        The regression counterpart: password mode authenticates as the API user
        directly, so Run As guidance would be actively misleading there.
        """
        _, logged = self._make_request_with_denial('password')

        self.assertNotIn('Run As', logged)
        self.assertNotIn('AC_Administrator', logged)

    def test_denial_does_not_trigger_token_refresh(self):
        """
        A 403 is not a transient expiry. Refreshing would burn a Secrets Manager
        version per call and never succeed — the exhaustion loop the circuit breaker
        exists to prevent.
        """
        for mode in ('password', 'client_credentials'):
            with self.subTest(mode=mode):
                sf, _ = self._make_request_with_denial(mode)

                sf.request.post.assert_not_called()
                sf.secrets_manager_client.put_secret_value.assert_not_called()

    def test_denial_preserves_salesforce_error_code(self):
        """Guidance is added alongside Salesforce's own wording, not instead of it."""
        for mode in ('password', 'client_credentials'):
            with self.subTest(mode=mode):
                _, logged = self._make_request_with_denial(mode)

                self.assertIn('INSUFFICIENT_ACCESS', logged)
                self.assertIn('insufficient access rights on object id', logged)

    def test_denial_publishes_metric(self):
        for mode in ('password', 'client_credentials'):
            with self.subTest(mode=mode):
                sf, _ = self._make_request_with_denial(mode)

                published = [
                    c.kwargs['MetricData'][0]['MetricName']
                    for c in sf.cloudwatch_client.put_metric_data.call_args_list
                ]
                self.assertIn('SalesforceAccessDenied', published)

    def test_denial_routed_from_check_resp(self):
        """
        End-to-end: a real 403 response body must produce SalesforceAccessDeniedException
        rather than the plain Exception the list branch raises for other statuses.
        Without this the handler above is unreachable in production.
        """
        from salesforce import SalesforceAccessDeniedException, __check_resp__

        resp = MagicMock()
        resp.status_code = 403
        resp.json.return_value = [
            {'errorCode': 'INSUFFICIENT_ACCESS', 'message': 'insufficient access'}]

        with self.assertRaises(SalesforceAccessDeniedException):
            __check_resp__(resp)

    # Verbatim from a live sandbox: AC_Administrator removed from the client-credentials
    # Run As user, then POST /sobjects/AC_ContactTraceRecord__c. This is the real
    # production denial — note it is a 400 and the errorCode is not an INSUFFICIENT_* one,
    # because the adapter's triggers on the object intercept and re-raise the failure.
    LIVE_DENIAL = {
        'message': 'Insufficient Privileges You do not have the level of access '
                   'necessary to perform the operation you requested. Please contact '
                   'your administrator if access is necessary.',
        'errorCode': 'FIELD_CUSTOM_VALIDATION_EXCEPTION',
        'fields': [],
    }

    def test_live_trigger_wrapped_denial_routes_to_forbidden(self):
        """
        The measured production case. Gating on status 403 or on an INSUFFICIENT_* code
        would both miss it, which is exactly what live testing caught.
        """
        from salesforce import SalesforceAccessDeniedException, __check_resp__

        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = [dict(self.LIVE_DENIAL)]

        with self.assertRaises(SalesforceAccessDeniedException):
            __check_resp__(resp)

    def _live_denial_log(self, mode, status_code=400):
        from salesforce import SalesforceAccessDeniedException

        sf = build_salesforce(SF_AUTH_MODE=mode)
        sf.request = MagicMock()

        def api(**kwargs):
            raise SalesforceAccessDeniedException(
                '%s: %s' % (self.LIVE_DENIAL['errorCode'], self.LIVE_DENIAL['message']),
                dict(self.LIVE_DENIAL), status_code)

        with patch('salesforce.logger') as mock_logger:
            with self.assertRaises(SalesforceAccessDeniedException):
                sf.makeRequest(api, url='%s/services/data/v67.0/query' % MY_DOMAIN)

        return ' '.join(str(c) for c in mock_logger.error.call_args_list)

    def test_live_trigger_wrapped_denial_gets_run_as_guidance(self):
        """And it must produce the Run As guidance, not just a distinct exception type."""
        logged = self._live_denial_log('client_credentials')

        self.assertIn('Review Run As user permissions', logged)
        self.assertIn('AC_Administrator', logged)

    def test_log_reports_the_real_status_not_an_assumed_403(self):
        """
        Caught in the sandbox: the guidance originally hardcoded "returned 403" while the
        measured response was a 400, so the log contradicted the wire. The status has to
        come from the response.
        """
        logged = self._live_denial_log('client_credentials', status_code=400)

        self.assertIn('HTTP 400', logged)
        self.assertNotIn('403', logged)

    def test_insufficient_access_at_400_still_routes_to_forbidden(self):
        """
        Salesforce's status for insufficient access is not consistent across endpoints,
        and could not be reproduced in the sandbox because an admin bypasses object
        permissions. Keying on the errorCode as well as the status means the guidance
        survives either way — gating on 403 alone would silently disable it if the real
        response turns out to be a 400.
        """
        from salesforce import SalesforceAccessDeniedException, __check_resp__

        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = [
            {'errorCode': 'INSUFFICIENT_ACCESS_OR_READONLY',
             'message': 'insufficient access rights on object id'}]

        with self.assertRaises(SalesforceAccessDeniedException):
            __check_resp__(resp)

    def test_restricted_picklist_400_stays_on_the_plain_path(self):
        """
        Observed live against the sandbox — the documented CTR data-loss case:
            POST AC_ContactTraceRecord__c {"Channel__c": "EMAIL"}
            -> 400 [{"errorCode": "INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST",
                     "message": "Channel: bad value for restricted picklist field: EMAIL",
                     "fields": ["Channel__c"]}]
        Same list shape as the 403s, so it must not be mistaken for a permissions fault.
        """
        from salesforce import SalesforceAccessDeniedException, __check_resp__

        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = [
            {'errorCode': 'INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST',
             'message': 'Channel: bad value for restricted picklist field: EMAIL',
             'fields': ['Channel__c']}]

        with self.assertRaises(Exception) as ctx:
            __check_resp__(resp)

        self.assertNotIsInstance(ctx.exception, SalesforceAccessDeniedException)

    def test_403_functionality_not_enabled_shape_from_live_org(self):
        """
        The exact 403 body observed from GET /wave/datasets. Confirms a real 403 uses
        the same list shape the handler depends on.
        """
        from salesforce import SalesforceAccessDeniedException, __check_resp__

        resp = MagicMock()
        resp.status_code = 403
        resp.json.return_value = [
            {'message': 'This feature is not currently enabled for this user type or '
                        'org: [Wave]',
             'errorCode': 'FUNCTIONALITY_NOT_ENABLED'}]

        with self.assertRaises(SalesforceAccessDeniedException):
            __check_resp__(resp)

    def test_non_403_list_error_still_raises_plain_exception(self):
        """Other statuses keep their existing behaviour — this is additive only."""
        from salesforce import SalesforceAccessDeniedException, __check_resp__

        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = [
            {'errorCode': 'MALFORMED_QUERY', 'message': 'unexpected token'}]

        with self.assertRaises(Exception) as ctx:
            __check_resp__(resp)

        self.assertNotIsInstance(ctx.exception, SalesforceAccessDeniedException)
        self.assertIn('MALFORMED_QUERY', str(ctx.exception))

    def test_403_oauth_error_body_still_routes_to_auth_handler(self):
        """
        Branch order guard. The 'error' in data check runs first, so a token-endpoint
        style body keeps going to SalesforceErrorResponseException and its actionable
        auth mapping even when the status happens to be 403.
        """
        from salesforce import SalesforceErrorResponseException, __check_resp__

        resp = MagicMock()
        resp.status_code = 403
        resp.json.return_value = {
            'error': 'invalid_grant',
            'error_description': 'no client credentials user enabled',
        }

        with self.assertRaises(SalesforceErrorResponseException):
            __check_resp__(resp)


class TestSecretsNeverLogged(unittest.TestCase):
    """
    hideData is the only thing keeping the client secret and the returned access token
    out of CloudWatch. Asserting that the flag is passed (see
    test_credentials_are_never_logged) does not prove Request.post honours it — with
    the two `if not hideData` guards deleted, that test still passes. These exercise
    Request.post itself.
    """

    SECRET_VALUES = ('consumer-secret', 'brand-new-access-token')

    def _post(self, hideData):
        from salesforce import Request

        response = MagicMock()
        response.status_code = 200
        response.text = json.dumps({'access_token': 'brand-new-access-token'})

        with patch('salesforce.requests.post', return_value=response), \
                patch('salesforce.logger') as mock_logger:
            Request().post(
                url='%s/services/oauth2/token' % MY_DOMAIN,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data={'grant_type': 'client_credentials',
                      'client_id': 'consumer-key',
                      'client_secret': 'consumer-secret'},
                hideData=hideData,
                formEncoded=True,
            )

        return ' '.join(str(c) for c in mock_logger.info.call_args_list)

    def test_hide_data_suppresses_body_and_response_logging(self):
        logged = self._post(hideData=True)

        for secret in self.SECRET_VALUES:
            self.assertNotIn(secret, logged)

    def test_without_hide_data_the_body_is_logged(self):
        """
        The counterpart that proves the assertion above is load-bearing rather than
        vacuously true — the values really would reach the logger unguarded.
        """
        logged = self._post(hideData=False)

        for secret in self.SECRET_VALUES:
            self.assertIn(secret, logged)

    def test_url_is_always_logged(self):
        """hideData suppresses the body, not the request line."""
        self.assertIn('services/oauth2/token', self._post(hideData=True))


class TestLogSanitization(unittest.TestCase):
    """
    sanitize_log replaces control characters to prevent log injection (CWE-117). It is
    not secret redaction — that is hideData's job, covered above. The auth path feeds
    it operator-supplied values, so a forged log line is reachable via a template
    parameter.
    """

    def test_invalid_auth_mode_is_sanitized(self):
        message = None
        try:
            build_salesforce(SF_AUTH_MODE='bogus\nINFO Auth succeeded')
        except Exception as err:
            message = str(err)

        self.assertIsNotNone(message)
        self.assertNotIn('\n', message)
        self.assertIn('[SANITIZED]', message)

    def test_sanitize_log_leaves_clean_values_alone(self):
        from log_util import sanitize_log

        self.assertEqual(sanitize_log('client_credentials'), 'client_credentials')


if __name__ == '__main__':
    unittest.main()
