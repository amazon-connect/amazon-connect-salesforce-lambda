"""
Unit tests for salesforce.py circuit breaker and exponential backoff changes.

COE-402979: AmazonConnectSalesforceLambda Secrets Manager Version Limit Exhaustion

Tests cover:
1. Normal happy path — token expiry, refresh succeeds, token persisted
2. Circuit breaker — frozen APIUser: refresh succeeds but API still 401 → raise without SM write
3. LimitExceededException — version limit hit, logged and skipped (non-fatal)
4. ResourceConflictException — concurrent write conflict, retried with backoff
5. ResourceConflictException exhausted — all retries fail, re-raise
6. Other ClientError — re-raise immediately
7. Backoff timing — worst-case 3 conflicts complete within Lambda timeout budget
"""

import json
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError

# Add the lambda_functions directory to the path so we can import salesforce
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambda_functions'))


def make_client_error(code):
    """Helper: create a ClientError with the given error code."""
    return ClientError(
        {'Error': {'Code': code, 'Message': 'test error'}},
        'PutSecretValue'
    )


def make_salesforce_instance():
    """
    Build a Salesforce instance without calling __init__ (which requires AWS credentials).
    Only the attributes needed for __put_secret_value_with_retry and makeRequest are set.
    """
    from salesforce import Salesforce, InvalidAuthTokenException

    sf = Salesforce.__new__(Salesforce)
    sf.secrets_manager_client = MagicMock()
    sf.sf_credentials_secrets_manager_arn = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:test'
    sf.secrets = {
        'AuthToken': 'old-token',
        'Password': 'pass',
        'AccessToken': 'token',
        'ConsumerKey': 'key',
        'ConsumerSecret': 'secret',
    }
    sf.auth_token = 'old-token'
    sf.headers = {'Authorization': 'Bearer old-token', 'Content-Type': 'application/json'}
    sf.login_host = 'https://test.salesforce.com'
    sf.auth_data = {'grant_type': 'password'}
    # makeRequest branches on auth_mode to pick the token-request transport; normally
    # set by __init__, so it must be declared here too. Kept consistent with the
    # password grant_type in auth_data above.
    sf.auth_mode = 'password'
    sf.host = 'https://test.salesforce.com'
    sf.version = 'v55.0'
    sf.request = MagicMock()
    return sf


class TestPutSecretValueWithRetry(unittest.TestCase):
    """Tests for Salesforce.__put_secret_value_with_retry"""

    def setUp(self):
        self.sf = make_salesforce_instance()

    def test_success_first_attempt(self):
        """Happy path: SM write succeeds on first attempt."""
        self.sf.secrets_manager_client.put_secret_value.return_value = {}

        self.sf._Salesforce__put_secret_value_with_retry()

        self.sf.secrets_manager_client.put_secret_value.assert_called_once_with(
            SecretId=self.sf.sf_credentials_secrets_manager_arn,
            SecretString=json.dumps(self.sf.secrets)
        )

    def test_limit_exceeded_is_non_fatal(self):
        """LimitExceededException: logs error and returns without raising."""
        self.sf.secrets_manager_client.put_secret_value.side_effect = make_client_error('LimitExceededException')

        # Should not raise
        self.sf._Salesforce__put_secret_value_with_retry()

        self.sf.secrets_manager_client.put_secret_value.assert_called_once()

    def test_resource_conflict_retried_up_to_max(self):
        """ResourceConflictException: retried 3 times, succeeds on 4th attempt."""
        call_count = {'n': 0}

        def conflict_then_succeed(**kwargs):
            call_count['n'] += 1
            if call_count['n'] <= 3:
                raise make_client_error('ResourceConflictException')

        self.sf.secrets_manager_client.put_secret_value.side_effect = conflict_then_succeed

        with patch('time.sleep') as mock_sleep:
            self.sf._Salesforce__put_secret_value_with_retry(max_retries=3)

        self.assertEqual(call_count['n'], 4)  # 1 initial + 3 retries
        self.assertEqual(mock_sleep.call_count, 3)
        # Verify exponential backoff: 0.3 → 0.6 → 1.2
        mock_sleep.assert_has_calls([call(0.3), call(0.6), call(1.2)])

    def test_resource_conflict_all_retries_exhausted_reraises(self):
        """ResourceConflictException: all retries exhausted → re-raises."""
        self.sf.secrets_manager_client.put_secret_value.side_effect = make_client_error('ResourceConflictException')

        with patch('time.sleep'):
            with self.assertRaises(ClientError) as ctx:
                self.sf._Salesforce__put_secret_value_with_retry(max_retries=3)

        self.assertEqual(ctx.exception.response['Error']['Code'], 'ResourceConflictException')
        self.assertEqual(self.sf.secrets_manager_client.put_secret_value.call_count, 4)

    def test_other_client_error_reraises_immediately(self):
        """Any other ClientError (e.g., AccessDeniedException) re-raises on first attempt."""
        self.sf.secrets_manager_client.put_secret_value.side_effect = make_client_error('AccessDeniedException')

        with patch('time.sleep') as mock_sleep:
            with self.assertRaises(ClientError) as ctx:
                self.sf._Salesforce__put_secret_value_with_retry(max_retries=3)

        self.assertEqual(ctx.exception.response['Error']['Code'], 'AccessDeniedException')
        self.sf.secrets_manager_client.put_secret_value.assert_called_once()
        mock_sleep.assert_not_called()

    def test_backoff_timing_within_lambda_budget(self):
        """
        Timing test (real sleep — not mocked): worst-case 3 ResourceConflictException
        retries complete within 4s, well under the 6s Lambda global timeout.
        Total sleep: 0.3 + 0.6 + 1.2 = 2.1s minimum.
        """
        call_count = {'n': 0}

        def conflict_then_succeed(**kwargs):
            call_count['n'] += 1
            if call_count['n'] <= 3:
                raise make_client_error('ResourceConflictException')

        self.sf.secrets_manager_client.put_secret_value.side_effect = conflict_then_succeed

        start = time.time()
        self.sf._Salesforce__put_secret_value_with_retry(max_retries=3)
        elapsed = time.time() - start

        self.assertGreaterEqual(elapsed, 2.0,
            "Expected at least 2.0s elapsed (0.3+0.6+1.2 sleep = 2.1s minimum)")
        self.assertLess(elapsed, 4.0,
            "Must complete well within 6s Lambda budget (got %.2fs)" % elapsed)
        self.assertEqual(call_count['n'], 4)

    def test_resource_conflict_succeeds_on_last_retry(self):
        """
        Boundary test: conflicts on all but the last retry.
        With max_retries=3, conflicts on attempts 0-2 (3 conflicts), then success
        on attempt 3 (the final retry). Verifies the boundary condition at exactly
        max_retries conflicts is still handled without re-raising.
        """
        call_count = {'n': 0}

        def conflict_until_last_retry(**kwargs):
            call_count['n'] += 1
            # Fail on attempts 1, 2, 3 (indices 0, 1, 2); succeed on attempt 4 (index 3)
            if call_count['n'] <= 3:
                raise make_client_error('ResourceConflictException')
            # 4th attempt (index 3 = max_retries) succeeds

        self.sf.secrets_manager_client.put_secret_value.side_effect = conflict_until_last_retry

        with patch('time.sleep') as mock_sleep:
            # Should succeed — exactly max_retries conflicts then success
            self.sf._Salesforce__put_secret_value_with_retry(max_retries=3)

        self.assertEqual(call_count['n'], 4)  # 3 conflicts + 1 success
        self.assertEqual(mock_sleep.call_count, 3)

    def test_put_secret_value_conflict_then_limit_exceeded_is_non_fatal(self):
        """
        Mixed error case: ResourceConflictException on attempt 1, then
        LimitExceededException on attempt 2. Should return without raising.
        NOTE: belongs here in TestPutSecretValueWithRetry since it only exercises
        __put_secret_value_with_retry directly, not makeRequest.
        """
        call_count = {'n': 0}

        def conflict_then_limit(**kwargs):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise make_client_error('ResourceConflictException')
            raise make_client_error('LimitExceededException')

        self.sf.secrets_manager_client.put_secret_value.side_effect = conflict_then_limit

        with patch('time.sleep'):
            # Should not raise — LimitExceededException is always non-fatal
            self.sf._Salesforce__put_secret_value_with_retry(max_retries=3)

        self.assertEqual(call_count['n'], 2)  # attempt 0 (conflict) + attempt 1 (limit)


class TestMakeRequestCircuitBreaker(unittest.TestCase):
    """Tests for the circuit breaker in Salesforce.makeRequest"""

    def setUp(self):
        from salesforce import InvalidAuthTokenException
        self.sf = make_salesforce_instance()
        self.InvalidAuthTokenException = InvalidAuthTokenException

    def test_normal_request_succeeds_without_token_refresh(self):
        """Happy path: first API call succeeds, no token refresh needed."""
        mock_response = MagicMock()
        mock_request_method = MagicMock(return_value=mock_response)

        result = self.sf.makeRequest(mock_request_method, url='https://test.com/api')

        self.assertEqual(result, mock_response)
        mock_request_method.assert_called_once()
        self.sf.secrets_manager_client.put_secret_value.assert_not_called()

    def test_expired_token_refresh_succeeds_token_persisted(self):
        """
        Token expires (401), refresh succeeds, circuit breaker test passes,
        new token is written to Secrets Manager.
        """
        from salesforce import InvalidAuthTokenException

        # OAuth token endpoint returns a new token
        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'new-token'}
        self.sf.request.post.return_value = mock_oauth_response

        # API call: first attempt → 401, second attempt (circuit breaker test) → success
        mock_response = MagicMock()
        call_count = {'n': 0}

        def api_side_effect(**kwargs):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise InvalidAuthTokenException("expired")
            return mock_response

        self.sf.secrets_manager_client.put_secret_value.return_value = {}

        result = self.sf.makeRequest(api_side_effect, url='https://test.com/api')

        self.assertEqual(result, mock_response)
        self.assertEqual(call_count['n'], 2)
        # Token was persisted after circuit breaker test passed
        self.sf.secrets_manager_client.put_secret_value.assert_called_once()
        # Verify the new token is in the persisted secret
        put_call_kwargs = self.sf.secrets_manager_client.put_secret_value.call_args.kwargs
        stored = json.loads(put_call_kwargs['SecretString'])
        self.assertEqual(stored['AuthToken'], 'new-token')

    def test_circuit_breaker_frozen_user_raises_without_sm_write(self):
        """
        Circuit breaker: OAuth refresh succeeds but API still returns 401 (frozen APIUser).
        InvalidAuthTokenException is raised and Secrets Manager is NOT written.
        """
        from salesforce import InvalidAuthTokenException

        # OAuth token endpoint returns a new token (succeeds even when user is frozen)
        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'new-token'}
        self.sf.request.post.return_value = mock_oauth_response

        # API always returns 401 — user is frozen
        def always_401(**kwargs):
            raise InvalidAuthTokenException("frozen")

        with self.assertRaises(InvalidAuthTokenException):
            self.sf.makeRequest(always_401, url='https://test.com/api')

        # CRITICAL: Secrets Manager must NOT be written when user is frozen
        self.sf.secrets_manager_client.put_secret_value.assert_not_called()

    def test_circuit_breaker_prevents_sm_exhaustion_under_sustained_freeze(self):
        """
        Simulate multiple invocations during a sustained APIUser freeze.
        Each invocation should raise without writing to Secrets Manager,
        preventing version exhaustion.
        """
        from salesforce import InvalidAuthTokenException

        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'new-token'}
        self.sf.request.post.return_value = mock_oauth_response

        def always_401(**kwargs):
            raise InvalidAuthTokenException("frozen")

        invocation_count = 10
        for _ in range(invocation_count):
            with self.assertRaises(InvalidAuthTokenException):
                self.sf.makeRequest(always_401, url='https://test.com/api')

        # No SM writes in any of the 10 invocations
        self.sf.secrets_manager_client.put_secret_value.assert_not_called()

    def test_make_request_conflict_exhausted_still_returns_result(self):
        """
        Critical: ResourceConflictException exhausted through makeRequest must
        return the API result — the Salesforce mutation already committed in the
        circuit-breaker test call, so raising here would cause the caller to
        retry and create a duplicate Salesforce record.
        """
        from salesforce import InvalidAuthTokenException

        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'new-token'}
        self.sf.request.post.return_value = mock_oauth_response

        mock_api_response = MagicMock()
        call_count = {'n': 0}

        def api_side_effect(**kwargs):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise InvalidAuthTokenException("expired")
            return mock_api_response

        # SM write always fails with ResourceConflictException (all retries exhausted)
        self.sf.secrets_manager_client.put_secret_value.side_effect = ClientError(
            {'Error': {'Code': 'ResourceConflictException', 'Message': 'conflict'}},
            'PutSecretValue'
        )

        # Must NOT raise — SM write failure is non-fatal, the API mutation is already done
        with patch('time.sleep'):
            result = self.sf.makeRequest(api_side_effect, url='https://test.com/api')

        self.assertEqual(result, mock_api_response)

    def test_make_request_oauth_refresh_failure_propagates(self):
        """
        If the OAuth token refresh itself fails (non-2xx from Salesforce OAuth endpoint),
        the exception should propagate and Secrets Manager must not be written.
        """
        from salesforce import InvalidAuthTokenException

        def api_always_401(**kwargs):
            raise InvalidAuthTokenException("expired")

        # The Request.post call raises Exception via __check_resp__ on non-2xx
        self.sf.request.post.side_effect = Exception("invalid_grant: authentication failure")

        with self.assertRaises(Exception) as ctx:
            self.sf.makeRequest(api_always_401, url='https://test.com/api')

        self.assertIn("invalid_grant", str(ctx.exception))
        # SM must not be written if OAuth refresh failed
        self.sf.secrets_manager_client.put_secret_value.assert_not_called()

    def test_circuit_breaker_test_non_401_error_propagates(self):
        """
        If the circuit-breaker test call raises a non-401 error (e.g., 500),
        the exception propagates. The fresh token is not persisted and no SM
        write happens — the API call failed, so there is no completed mutation
        to protect.
        """
        from salesforce import InvalidAuthTokenException

        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'new-token'}
        self.sf.request.post.return_value = mock_oauth_response

        call_count = {'n': 0}

        def api_500_on_retry(**kwargs):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise InvalidAuthTokenException("expired")
            raise Exception("request returned status code: 500")

        with self.assertRaises(Exception) as ctx:
            self.sf.makeRequest(api_500_on_retry, url='https://test.com/api')

        self.assertIn("500", str(ctx.exception))
        # SM must not be written — the API call failed, no mutation to protect
        self.sf.secrets_manager_client.put_secret_value.assert_not_called()

    def test_circuit_breaker_test_uses_new_authorization_header(self):
        """
        Verify the circuit-breaker test call (retry after token refresh) uses the
        NEW Authorization header, not the stale one. A regression that fails to
        update headers before the retry would loop forever on 401.
        """
        from salesforce import InvalidAuthTokenException

        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'fresh-token-abc'}
        self.sf.request.post.return_value = mock_oauth_response

        captured_headers = []
        mock_api_response = MagicMock()
        call_count = {'n': 0}

        def capture_headers(**kwargs):
            call_count['n'] += 1
            captured_headers.append(kwargs.get('headers', {}).copy())
            if call_count['n'] == 1:
                raise InvalidAuthTokenException("expired")
            return mock_api_response

        self.sf.secrets_manager_client.put_secret_value.return_value = {}

        self.sf.makeRequest(capture_headers, url='https://test.com/api')

        self.assertEqual(len(captured_headers), 2)
        # First call used stale token
        self.assertIn('old-token', captured_headers[0].get('Authorization', ''))
        # Circuit-breaker test call used the new token
        self.assertIn('fresh-token-abc', captured_headers[1].get('Authorization', ''))

    def test_make_request_limit_exceeded_still_returns_result(self):
        """
        If the SM write raises LimitExceededException, makeRequest should still
        return the API result — the write failure is non-fatal.
        """
        from salesforce import InvalidAuthTokenException

        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'new-token'}
        self.sf.request.post.return_value = mock_oauth_response

        mock_api_response = MagicMock()
        call_count = {'n': 0}

        def api_side_effect(**kwargs):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise InvalidAuthTokenException("expired")
            return mock_api_response

        # SM write fails with LimitExceededException
        self.sf.secrets_manager_client.put_secret_value.side_effect = ClientError(
            {'Error': {'Code': 'LimitExceededException', 'Message': 'too many versions'}},
            'PutSecretValue'
        )

        # Should NOT raise — LimitExceededException is non-fatal
        result = self.sf.makeRequest(api_side_effect, url='https://test.com/api')

        self.assertEqual(result, mock_api_response)
        # SM write was attempted once (and failed non-fatally)
        self.sf.secrets_manager_client.put_secret_value.assert_called_once()

    def test_expired_token_new_token_stored_in_secret(self):
        """
        Verify the persisted secret contains the new token, not the old one.
        Uses call_args.kwargs (modern API) instead of call_args[1].
        """
        from salesforce import InvalidAuthTokenException

        mock_oauth_response = MagicMock()
        mock_oauth_response.json.return_value = {'access_token': 'brand-new-token'}
        self.sf.request.post.return_value = mock_oauth_response

        mock_api_response = MagicMock()
        call_count = {'n': 0}

        def api_side_effect(**kwargs):
            call_count['n'] += 1
            if call_count['n'] == 1:
                raise InvalidAuthTokenException("expired")
            return mock_api_response

        self.sf.secrets_manager_client.put_secret_value.return_value = {}

        self.sf.makeRequest(api_side_effect, url='https://test.com/api')

        put_kwargs = self.sf.secrets_manager_client.put_secret_value.call_args.kwargs
        stored = json.loads(put_kwargs['SecretString'])
        self.assertEqual(stored['AuthToken'], 'brand-new-token')
        # Old fields must be preserved
        self.assertIn('Password', stored)
        self.assertIn('ConsumerKey', stored)


if __name__ == '__main__':
    unittest.main(verbosity=2)
