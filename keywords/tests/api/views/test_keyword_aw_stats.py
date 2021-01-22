from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_401_UNAUTHORIZED
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models import KeywordStatistic
from es_components.tests.utils import ESTestCase
from keywords.api.names import KeywordPathName
from saas.urls.namespaces import Namespace
from userprofile.constants import StaticPermissions
from utils.unittests.int_iterator import int_iterator
from utils.unittests.reverse import reverse
from utils.unittests.test_case import ExtendedAPITestCase


class KeywordBaseAWStatsTestCase(ExtendedAPITestCase, ESTestCase):
    def get_url(self, **kwargs):
        return reverse(KeywordPathName.KEYWORD_AW_STATS, [Namespace.KEYWORD], **kwargs)

    def create_aw_stats(self, keyword):
        campaign = Campaign.objects.create(id=next(int_iterator))
        ad_group = AdGroup.objects.create(id=next(int_iterator), campaign=campaign)
        KeywordStatistic.objects.create(date="2020-02-02", ad_group=ad_group, keyword=keyword)


class KeywordAWStatsPermissionsApiViewTestCase(KeywordBaseAWStatsTestCase):
    def test_unauthorized(self):
        response = self.client.get(self.get_url(args=("keyword",)))

        self.assertEqual(HTTP_401_UNAUTHORIZED, response.status_code)

    def test_no_permissions(self):
        self.create_test_user(perms={StaticPermissions.RESEARCH: False})

        response = self.client.get(self.get_url(args=("keyword",)))

        self.assertEqual(HTTP_403_FORBIDDEN, response.status_code)

    def test_success_for_admin(self):
        self.create_admin_user()
        keyword = "keyword"
        self.create_aw_stats(keyword)

        response = self.client.get(self.get_url(args=(keyword,)))

        self.assertEqual(HTTP_200_OK, response.status_code)

    def test_success_permissions(self):
        keyword = "keyword"
        self.create_aw_stats(keyword)
        required_permissions = ("keyword_details", "settings_my_yt_channels")
        for permission in required_permissions:
            with self.subTest(permission):
                user = self.create_test_user()
                user.add_custom_user_permission(permission)

                response = self.client.get(self.get_url(args=(keyword,)))

                self.assertEqual(HTTP_200_OK, response.status_code)


class KeywordAWStatsApiViewTestCase(KeywordBaseAWStatsTestCase):
    def setUp(self):
        super(KeywordAWStatsApiViewTestCase, self).setUp()
        self.create_admin_user()

    def test_no_stats(self):
        keyword = "keyword"

        url = self.get_url(args=(keyword,))
        response = self.client.get(url)

        self.assertEqual(HTTP_404_NOT_FOUND, response.status_code)

    def test_min_values(self):
        keyword = "keyword"
        self.create_aw_stats(keyword)

        url = self.get_url(args=(keyword,))
        response = self.client.get(url)

        self.assertEqual(
            {
                "average_cpm": None,
                "average_cpv": None,
                "campaigns_count": 1,
                "clicks": 0,
                "cost": 0.0,
                "ctr": None,
                "ctr_v": None,
                "impressions": 0,
                "video_clicks": None,
                "video_impressions": None,
                "video_view_rate": None,
                "video_views": 0,
            },
            response.data
        )
