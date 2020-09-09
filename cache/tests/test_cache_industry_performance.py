from django.test import TransactionTestCase

from cache.tasks import cache_industry_performance
from es_components.tests.utils import ESTestCase


class CacheIndustryPerformanceTestCase(TransactionTestCase, ESTestCase):
    def test_comparing_with_none(self):
        """
        Jira issue: https://channelfactory.atlassian.net/browse/VIQ2-501
        """
        cache_industry_performance()
