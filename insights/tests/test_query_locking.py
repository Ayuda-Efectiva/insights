# Copyright (c) 2025, Frappe Technologies Pvt. Ltd. and Contributors
# See license.txt

import unittest
from unittest.mock import MagicMock, patch

import frappe
import pandas as pd

from insights.insights.doctype.insights_data_source_v3.ibis_utils import (
    LOCK_TIMEOUT,
    MAX_CONCURRENT_QUERIES,
    QUERY_LOCK_PREFIX,
    cache_results,
    execute_with_lock,
    get_cached_results,
    get_pending_query_result,
    has_cached_results,
    is_query_executing,
    release_lock,
    release_semaphore,
    try_acquire_lock,
    try_acquire_semaphore,
)


class TestQueryLocking(unittest.TestCase):

    def setUp(self):
        self.test_cache_key = f"test_lock_{frappe.generate_hash(length=8)}"
        # lock_key is what gets passed to try_acquire_lock (includes prefix)
        self.lock_key = f"{QUERY_LOCK_PREFIX}{self.test_cache_key}"
        # Clean up any existing test keys
        self.cleanup_test_keys()

    def tearDown(self):
        self._cleanup_test_keys()

    def cleanup_test_keys(self):
        try:
            cache = frappe.cache()
            # Clean up lock key
            full_lock_key = cache.make_key(self.lock_key)
            cache.delete(full_lock_key)
            # Clean up cache results
            results_key = "insights:query_results:" + self.test_cache_key
            cache.delete(cache.make_key(results_key))
        except Exception:
            pass


    def test_acquire_lock_success(self):
        acquired = try_acquire_lock(self.lock_key)
        self.assertTrue(acquired, "Should acquire lock when not held")
        # Clean up
        release_lock(self.lock_key)

    def test_acquire_lock_fails_when_held(self):
        # First acquisition should succeed
        first_acquired = try_acquire_lock(self.lock_key)
        self.assertTrue(first_acquired, "First acquisition should succeed")

        # Second acquisition should fail
        second_acquired = try_acquire_lock(self.lock_key)
        self.assertFalse(second_acquired, "Second acquisition should fail")

        # Clean up
        release_lock(self.lock_key)

    def test_release_lock(self):
        # Acquire lock
        try_acquire_lock(self.lock_key)

        # Release lock
        release_lock(self.lock_key)

        # Should be able to acquire again
        acquired = try_acquire_lock(self.lock_key)
        self.assertTrue(acquired, "Should acquire lock after release")

        # Clean up
        release_lock(self.lock_key)

    def test_is_query_executing(self):
        # Initially not executing
        self.assertFalse(
            is_query_executing(self.test_cache_key),
            "Should not be executing initially"
        )

        # Acquire lock (using the full lock_key with prefix)
        acquired = try_acquire_lock(self.lock_key)
        self.assertTrue(acquired, "Lock should be acquired")

        # Now should be executing (pass cache_key without prefix)
        executing = is_query_executing(self.test_cache_key)
        self.assertTrue(executing, "Should be executing after lock acquired")

        # Release lock
        release_lock(self.lock_key)

        # No longer executing
        self.assertFalse(
            is_query_executing(self.test_cache_key),
            "Should not be executing after lock released"
        )


    def test_semaphore_acquire_success(self):
        slot = try_acquire_semaphore()
        self.assertIsNotNone(slot, "Should acquire semaphore slot")
        release_semaphore()

    def test_semaphore_limit(self):
        acquired_slots = []

        # Acquire MAX_CONCURRENT_QUERIES slots
        for i in range(MAX_CONCURRENT_QUERIES):
            slot = try_acquire_semaphore()
            if slot is not None:
                acquired_slots.append(slot)

        # Next acquisition should fail
        extra_slot = try_acquire_semaphore()
        self.assertIsNone(
            extra_slot,
            f"Should not acquire more than {MAX_CONCURRENT_QUERIES} slots"
        )

        # Clean up release all slots
        for _ in acquired_slots:
            release_semaphore()

    def test_semaphore_release(self):
        # Fill up the semaphore
        for _ in range(MAX_CONCURRENT_QUERIES):
            try_acquire_semaphore()

        # Should be full
        self.assertIsNone(try_acquire_semaphore())

        # Release one
        release_semaphore()

        # Now should be able to acquire
        slot = try_acquire_semaphore()
        self.assertIsNotNone(slot, "Should acquire after release")

        # Clean up
        for _ in range(MAX_CONCURRENT_QUERIES):
            release_semaphore()


    def test_cache_results(self):
        test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        # Cache the results
        cache_results(self.test_cache_key, test_data, cache_expiry=60)

        # Verify cached
        self.assertTrue(
            has_cached_results(self.test_cache_key),
            "Should have cached results"
        )

        cached = get_cached_results(self.test_cache_key)
        self.assertIsNotNone(cached, "Should retrieve cached results")
        self.assertEqual(len(cached), 3, "Should have 3 rows")

    def test_cache_miss(self):
        non_existent_key = f"non_existent_{frappe.generate_hash(length=8)}"

        self.assertFalse(
            has_cached_results(non_existent_key),
            "Should not have cached results for non-existent key"
        )

        self.assertIsNone(
            get_cached_results(non_existent_key),
            "Should return None for cache miss"
        )


    def test_pending_query_result_executing(self):
        # Acquire lock to simulate executing query
        acquired = try_acquire_lock(self.lock_key)
        self.assertTrue(acquired, "Should acquire lock for test")

        # Check that is_query_executing works
        self.assertTrue(
            is_query_executing(self.test_cache_key),
            "is_query_executing should return True"
        )

        result = get_pending_query_result(self.test_cache_key)
        self.assertEqual(result["status"], "pending", "Should return pending status")

        # Clean up
        release_lock(self.lock_key)

    def test_pending_query_result_completed(self):
        # Cache some results
        test_data = pd.DataFrame({"value": [717]})
        cache_results(self.test_cache_key, test_data, cache_expiry=60)

        result = get_pending_query_result(self.test_cache_key)
        self.assertEqual(result["status"], "completed", "Should return completed status")
        self.assertIsNotNone(result["result"], "Should have result data")

    def test_pending_query_result_not_found(self):
        non_existent_key = f"non_existent_{frappe.generate_hash(length=8)}"

        result = get_pending_query_result(non_existent_key)
        self.assertEqual(result["status"], "not_found", "Should return not_found status")


    def testexecute_with_lock_returns_pending_when_locked(self):
        # Pre-acquire the lock
        try_acquire_lock(self.lock_key)

        # Create mock query
        mock_query = MagicMock()

        # Try to execute - should return pending
        result, time_taken = execute_with_lock(
            mock_query,
            "SELECT 1",
            self.test_cache_key,
            cache=True,
            cache_expiry=60,
            force=False,
            reference_name="test"
        )

        self.assertIsInstance(result, dict, "Should return dict")
        self.assertEqual(result["status"], "pending", "Should return pending status")
        self.assertEqual(result["cache_key"], self.test_cache_key)
        self.assertEqual(time_taken, -1)

        # Clean up
        release_lock(self.lock_key)

    def testexecute_with_lock_releases_lock_on_completion(self):
        # Create mock query that returns a DataFrame
        mock_query = MagicMock()
        mock_query.execute.return_value = pd.DataFrame({"result": [1]})

        # Execute
        with patch(
            "insights.insights.doctype.insights_data_source_v3.ibis_utils.create_execution_log"
        ):
            result, _ = execute_with_lock(
                mock_query,
                "SELECT 1",
                self.test_cache_key,
                cache=True,
                cache_expiry=60,
                force=False,
                reference_name="test"
            )

        # Lock should be released
        self.assertFalse(
            is_query_executing(self.test_cache_key),
            "Lock should be released after execution"
        )

    def testexecute_with_lock_releases_lock_on_error(self):
        # Create mock query that raises an error
        mock_query = MagicMock()
        mock_query.execute.side_effect = Exception("Test error")

        # Execute and expect error
        with self.assertRaises(Exception):
            with patch(
                "insights.insights.doctype.insights_data_source_v3.ibis_utils.create_execution_log"
            ):
                execute_with_lock(
                    mock_query,
                    "SELECT 1",
                    self.test_cache_key,
                    cache=True,
                    cache_expiry=60,
                    force=False,
                    reference_name="test"
                )

        # Lock should still be released
        self.assertFalse(
            is_query_executing(self.test_cache_key),
            "Lock should be released even after error"
        )


    def test_coalescing_flow(self):
        # Simulate first request acquiring lock and executing
        first_acquired = try_acquire_lock(self.lock_key)
        self.assertTrue(first_acquired, "First request should acquire lock")

        # Verify lock is held
        self.assertTrue(
            is_query_executing(self.test_cache_key),
            "Query should be marked as executing"
        )

        # Simulate second request trying to get results
        result = get_pending_query_result(self.test_cache_key)
        self.assertEqual(result["status"], "pending", "Second request should get pending")

        # First request finishes and caches results
        test_data = pd.DataFrame({"value": [717]})
        cache_results(self.test_cache_key, test_data, cache_expiry=60)
        release_lock(self.lock_key)

        # Second request polls and gets results
        result = get_pending_query_result(self.test_cache_key)
        self.assertEqual(result["status"], "completed", "Should get completed after cache")
        self.assertEqual(len(result["result"]), 1)
        self.assertEqual(result["result"]["value"].iloc[0], 717)

    def test_redis_failure_fails_open(self):
        # Test by directly calling with a mock cache that raises
        original_cache = frappe.cache

        def mock_cache():
            mock = MagicMock()
            mock.set.side_effect = Exception("Redis connection failed")
            return mock

        try:
            frappe.cache = mock_cache
            # Should return True (allow execution) on failure
            acquired = try_acquire_lock("test_lock_failure")
            self.assertTrue(acquired, "Should fail open and allow execution")
        finally:
            frappe.cache = original_cache


class TestQueryLockingConstants(unittest.TestCase):

    def test_lock_timeout_reasonable(self):
        self.assertGreaterEqual(LOCK_TIMEOUT, 60, "Lock timeout should be at least 60 seconds")
        self.assertLessEqual(LOCK_TIMEOUT, 600, "Lock timeout should be at most 10 minutes")

    def test_max_concurrent_queries_reasonable(self):
        self.assertGreaterEqual(MAX_CONCURRENT_QUERIES, 1, "Should allow at least 1 query")
        self.assertLessEqual(MAX_CONCURRENT_QUERIES, 100, "Should not allow too many concurrent queries")

    def test_lock_prefix_unique(self):
        self.assertTrue(
            QUERY_LOCK_PREFIX.startswith("insights:"),
            "Lock prefix should be namespaced to insights"
        )


if __name__ == "__main__":
    unittest.main()
