import unittest
import asyncio
from time import time
from sys import platform
from parameterized import parameterized
import d3a_api_client.websocket_device


class TestWebsocket(unittest.TestCase):

    def setUp(self):
        self.coro_backup = d3a_api_client.websocket_device.websocket_coroutine
        d3a_api_client.websocket_device.WEBSOCKET_WAIT_BEFORE_RETRY_SECONDS = 0
        d3a_api_client.websocket_device.WEBSOCKET_MAX_CONNECTION_RETRIES = 5
        d3a_api_client.websocket_device.WEBSOCKET_ERROR_THRESHOLD_SECONDS = 30

    def tearDown(self):
        d3a_api_client.websocket_device.websocket_coroutine = self.coro_backup

    @parameterized.expand(
        [(1, ),
         (3, ),
         (7, ),
         (13, )])
    def test_websocket_retries_the_connection_before_failing(self, num_of_retries):
        d3a_api_client.websocket_device.WEBSOCKET_MAX_CONNECTION_RETRIES = num_of_retries
        coro_execution_counter = 0

        async def exception_raising_coroutine(_1, _2, _3):
            nonlocal coro_execution_counter
            coro_execution_counter += 1
            raise Exception("exception!")

        d3a_api_client.websocket_device.websocket_coroutine = exception_raising_coroutine

        try:
            asyncio.get_event_loop().run_until_complete(
                d3a_api_client.websocket_device.retry_coroutine(None, None, None, 0)
            )
        except Exception:
            pass
        else:
            # Asserting here because the retry_coroutine should not succeed
            assert False

        assert coro_execution_counter == num_of_retries + 1

    def test_websocket_conforms_to_wait_before_retry_parameter(self):
        d3a_api_client.websocket_device.WEBSOCKET_WAIT_BEFORE_RETRY_SECONDS = 0.1

        async def exception_raising_coro(_1, _2, _3):
            raise Exception("exception!")

        d3a_api_client.websocket_device.websocket_coroutine = exception_raising_coro

        start_time = time()
        try:
            asyncio.get_event_loop().run_until_complete(
                d3a_api_client.websocket_device.retry_coroutine(None, None, None, 0)
            )
        except Exception:
            pass

        end_time = time()

        num_of_retries = d3a_api_client.websocket_device.WEBSOCKET_MAX_CONNECTION_RETRIES
        expected_duration = 0.1 * (num_of_retries + 1)

        # On MacOS, the precision of asyncio.sleep is far worse than in Linux, which has the result to
        # not sleep the exact time it is dictated, but a bit more. This test calls sleep multiple times
        # which has the effect that deviations from the sleep accumulate and can surpass the original
        # tolerance of 0.01. This is the reason for the explicit increased tolerance here.
        tolerance = 0.05 if platform == "darwin" else 0.01

        assert expected_duration <= end_time - start_time <= expected_duration + tolerance

    def test_websocket_restarts_the_retry_count_if_ws_coro_does_not_crash_for_some_time(self):
        d3a_api_client.websocket_device.WEBSOCKET_ERROR_THRESHOLD_SECONDS = 0.1
        coro_execution_counter = 0

        async def exception_after_time_coro(_1, _2, _3):
            nonlocal coro_execution_counter
            coro_execution_counter += 1
            if coro_execution_counter < 4:
                await asyncio.sleep(0.12)
            raise Exception("exception!")

        d3a_api_client.websocket_device.websocket_coroutine = exception_after_time_coro

        try:
            asyncio.get_event_loop().run_until_complete(
                d3a_api_client.websocket_device.retry_coroutine(None, None, None, 0)
            )
        except Exception:
            pass

        assert coro_execution_counter == 8
