from datetime import datetime, timedelta
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST,\
    HTTP_403_FORBIDDEN
from aw_reporting.demo.models import DemoAccount
from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher
from unittest.mock import patch


class AdGroupAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_ad_group(self, owner, start, end):
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
        url = reverse("aw_creation_urls:optimization_ad_group",
                      args=(ag.id,))

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.perform_format_check(response.data)

    def perform_format_check(self, data):
        self.assertEqual(
            set(data.keys()),
            {
                'id', 'name',
                'max_rate',
                'is_approved',
                'thumbnail',
                'targeting',
                'ct_overlay_text',
                'parents',
                'final_url',
                'display_url',
                'genders',
                'video_url',
                'age_ranges',
            }
        )
        for f in ('age_ranges', 'genders', 'parents'):
            self.assertGreater(len(data[f]), 1)
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

        url = reverse("aw_creation_urls:optimization_ad_group",
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

        url = reverse("aw_creation_urls:optimization_ad_group",
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
        url = reverse("aw_creation_urls:optimization_ad_group",
                      args=(ad_group.id,))
        data = dict(
            name="Ad Group  1",
            max_rate="66.666",
            video_url="https://www.youtube.com/watch?v=zaa0r2WbmYo",
            genders=[AdGroupCreation.GENDER_FEMALE,
                     AdGroupCreation.GENDER_MALE],
            parents=[AdGroupCreation.PARENT_PARENT,
                     AdGroupCreation.PARENT_UNDETERMINED],
            age_ranges=[AdGroupCreation.AGE_RANGE_55_64,
                        AdGroupCreation.AGE_RANGE_65_UP],
            final_url="https://www.channelfactory.com/",
            display_url="www.channelfactory.com",
        )
        response = self.client.patch(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        ad_group.refresh_from_db()
        self.assertEqual(ad_group.name, data['name'])
        self.assertEqual(str(ad_group.max_rate), data['max_rate'])
        self.assertEqual(ad_group.video_url, data['video_url'])
        self.assertEqual(ad_group.final_url, data['final_url'])
        self.assertEqual(ad_group.display_url, data['display_url'])
        self.assertEqual(set(ad_group.genders), set(data['genders']))
        self.assertEqual(set(ad_group.parents), set(data['parents']))
        self.assertEqual(set(ad_group.age_ranges), set(data['age_ranges']))

    def test_fail_approve(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad_group = self.create_ad_group(**defaults)
        url = reverse("aw_creation_urls:optimization_ad_group",
                      args=(ad_group.id,))
        data = dict(
            is_approved=True,
        )
        response = self.client.patch(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data['non_field_errors'][0],
            "These fields are required for approving: "
            "max CPV, video URL, display URL, final URL"
        )

    def test_success_approve(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad_group = self.create_ad_group(**defaults)
        ad_group.display_url = "www.4e-tam.ua"
        ad_group.final_url = "https://www.4e-tam.ua"
        ad_group.video_url = "https://www.youtube.com/watch?v=a_6DEctiNSs"
        ad_group.max_rate = "20.0"
        ad_group.save()

        url = reverse("aw_creation_urls:optimization_ad_group",
                      args=(ad_group.id,))
        data = dict(
            is_approved=True,
        )
        response = self.client.patch(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_fail_set_max_rate(self):
        """
        SAAS-158: CPv that is entered on ad group level
        should be less than Max CPV at placement level
        :return:
        """
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            max_rate="0.075",  # max rate at campaign level
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        url = reverse("aw_creation_urls:optimization_ad_group",
                      args=(ad_group_creation.id,))
        data = dict(
            max_rate="0.076",   # max rate at ad group level
        )
        response = self.client.patch(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

    def test_success_set_max_rate(self):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            name="", account_creation=account_creation,
            max_rate="0.075",  # max rate at campaign level
        )
        ad_group_creation = AdGroupCreation.objects.create(
            name="", campaign_creation=campaign_creation,
        )
        url = reverse("aw_creation_urls:optimization_ad_group",
                      args=(ad_group_creation.id,))
        data = dict(
            max_rate="0.075",   # max rate at ad group level
        )
        response = self.client.patch(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)





