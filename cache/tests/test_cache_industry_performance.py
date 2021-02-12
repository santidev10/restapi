from django.test import TestCase

from cache.tasks import cache_industry_performance
from es_components.tests.utils import ESTestCase


class CacheIndustryPerformanceTestCase(TestCase, ESTestCase):
    def test_comparing_with_none(self):
        """
        Jira issue: https://channelfactory.atlassian.net/browse/VIQ2-501
        """
        cache_industry_performance()
