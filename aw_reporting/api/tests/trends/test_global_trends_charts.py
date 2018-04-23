from django.core.urlresolvers import reverse

from aw_reporting.api.urls.names import Name
from utils.utils_tests import ExtendedAPITestCase


class GlobalTrendsChartsTestCase(ExtendedAPITestCase):
    def setUp(self):
        self.url = reverse(Name.GlobalTrends.FILTERS)
