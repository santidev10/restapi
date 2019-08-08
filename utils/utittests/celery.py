from contextlib import contextmanager
from unittest.mock import Mock
from unittest.mock import patch

from celery import Celery


@contextmanager
def mock_send_task():
    with patch.object(Celery, "send_task") as send_task_mock:
        feature_mock = Mock()
        feature_mock.ready.return_value = True
        send_task_mock.return_value = feature_mock
        yield send_task_mock
