import time
from unittest import mock

from utils.unittests.test_case import ExtendedAPITestCase
from brand_safety.auditors.utils import pickled_data


class PickedDataDecoratorTestCase(ExtendedAPITestCase):
    def test_expires_refreshes(self):
        """ Test that pickled data is not loaded if time has expired """
        expires = 10

        @pickled_data("test", expires)
        def test_func():
            pass

        expired_time = time.time() - expires
        with mock.patch("brand_safety.auditors.utils.time.time", return_value=expired_time),\
                mock.patch("brand_safety.auditors.utils.pickle.load") as mock_load:
            test_func()
            mock_load.assert_not_called()

    def test_refreshes(self):
        """ Test that pickled data is loaded if time has not expired """
        expires = 60

        @pickled_data("test", expires)
        def test_func():
            pass

        with mock.patch("brand_safety.auditors.utils.pickle.load") as mock_load:
            test_func()
            mock_load.assert_called_once()
