from datetime import datetime, timedelta
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST,\
    HTTP_403_FORBIDDEN, HTTP_204_NO_CONTENT
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AdGroupAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_ad_group(self, owner, start=None, end=None):
        account_creation = AccountCreation.objects.create(
            name="Pep",
            owner=owner,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="",
            account_creation=account_creation,
            start=start,
            end=end,
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="",
            campaign_creation=campaign_creation,
        )
        return ad_group_creation

    def test_success_get(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ag = self.create_ad_group(**defaults)
        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ag.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def perform_format_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                'id', 'name', 'ad_creations', 'updated_at', 'max_rate',
                'targeting', 'parents', 'genders', 'age_ranges',
            }
        )
        for f in ('age_ranges', 'genders', 'parents'):
            if len(data[f]) > 0:
                self.assertEqual(
                    set(data[f][0].keys()),
                    {'id', 'name'}
                )
        self.assertEqual(
            set(data['targeting']),
            {'channel', 'video', 'topic', 'interest', 'keyword'}
        )

    def test_success_get_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))
        with patch("aw_reporting.demo.models.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def test_fail_update_demo(self):
        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))

        response = self.client.patch(
            url, json.dumps({}), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

    def test_success_update(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad_group = self.create_ad_group(**defaults)
        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))
        data = dict(
            name="Ad Group  1",
            genders=[AdGroupCreation.GENDER_FEMALE,
                     AdGroupCreation.GENDER_MALE],
            parents=[AdGroupCreation.PARENT_PARENT,
                     AdGroupCreation.PARENT_UNDETERMINED],
            age_ranges=[AdGroupCreation.AGE_RANGE_55_64,
                        AdGroupCreation.AGE_RANGE_65_UP],
            targeting={
                "keyword": {"positive": ["spam", "ham"], "negative": ["ai", "neural nets"]},
                "video": {"positive": ["iTKJ_itifQg"], "negative": ["1112yt"]},
            }
        )
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.patch(
                url, json.dumps(data), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
        ad_group.refresh_from_db()
        self.assertEqual(ad_group.name, data['name'])
        self.assertEqual(set(ad_group.genders), set(data['genders']))
        self.assertEqual(set(ad_group.parents), set(data['parents']))
        self.assertEqual(set(ad_group.age_ranges), set(data['age_ranges']))
        self.assertEqual(
            set(
                ad_group.targeting_items.filter(
                    type="keyword", is_negative=False
                ).values_list("criteria", flat=True)
            ),
            set(data['targeting']['keyword']['positive'])
        )
        self.assertEqual(
            set(
                ad_group.targeting_items.filter(
                    type="keyword", is_negative=True
                ).values_list("criteria", flat=True)
            ),
            set(data['targeting']['keyword']['negative'])
        )
        self.assertEqual(
            set(
                ad_group.targeting_items.filter(
                    type="video", is_negative=False
                ).values_list("criteria", flat=True)
            ),
            set(data['targeting']['video']['positive'])
        )
        self.assertEqual(
            set(
                ad_group.targeting_items.filter(
                    type="video", is_negative=True
                ).values_list("criteria", flat=True)
            ),
            set(data['targeting']['video']['negative'])
        )

    def test_fail_delete_the_only(self):
        ad_group = self.create_ad_group(owner=self.user)
        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))
        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_delete(self):
        ad_group = self.create_ad_group(owner=self.user)
        AdGroupCreation.objects.create(
            name="",
            campaign_creation=ad_group.campaign_creation,
        )
        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))

        response = self.client.delete(url)
        self.assertEqual(response.status_code, HTTP_204_NO_CONTENT)


