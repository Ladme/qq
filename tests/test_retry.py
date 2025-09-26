# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import MagicMock, patch

import pytest

from qq_lib.retry import QQRetryer


def test_retryer_success_first_try():
    mock_func = MagicMock(return_value=42)
    retryer = QQRetryer(mock_func, max_tries=5, wait_seconds=0.1)

    result = retryer.run()

    assert result == 42
    mock_func.assert_called_once()


def test_retryer_retries_until_success():
    mock_func = MagicMock(side_effect=[Exception("fail"), Exception("fail again"), 99])
    retryer = QQRetryer(mock_func, max_tries=5, wait_seconds=0.1)

    result = retryer.run()

    assert result == 99
    assert mock_func.call_count == 3


def test_retryer_raises_after_max_tries():
    mock_func = MagicMock(side_effect=Exception("persistent failure"))
    retryer = QQRetryer(mock_func, max_tries=3, wait_seconds=0.1)

    with pytest.raises(Exception, match="persistent failure"):
        retryer.run()

    assert mock_func.call_count == 3


def test_retryer_logs_warning_on_failure():
    mock_func = MagicMock(side_effect=[Exception("fail"), 123])
    retryer = QQRetryer(mock_func, max_tries=3, wait_seconds=0.1)

    with patch("qq_lib.retry.logger") as mock_logger:
        result = retryer.run()

    assert result == 123
    # logger.warning was called once
    assert mock_logger.warning.call_count == 1

    # log message includes the exception text and attempt info
    logged_message = mock_logger.warning.call_args[0][0]
    assert "fail" in logged_message
    assert "Attempting again in 0.1 seconds" in logged_message


def test_retryer_passes_args_and_kwargs():
    mock_func = MagicMock(return_value="ok")
    retryer = QQRetryer(mock_func, 1, 2, x=5, y=6, max_tries=2, wait_seconds=0.1)

    result = retryer.run()

    assert result == "ok"
    mock_func.assert_called_once_with(1, 2, x=5, y=6)
