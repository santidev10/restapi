from django.core.urlresolvers import reverse

from aw_reporting.api.urls.names import Name
from utils.utils_tests import ExtendedAPITestCase


class GlobalTrendsDataTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.url = reverse(Name.GlobalTrends.DATA)
