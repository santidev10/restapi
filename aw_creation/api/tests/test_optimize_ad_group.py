from datetime import datetime, timedelta

from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import *
from aw_reporting.models import *
from saas.utils_tests import ExtendedAPITestCase


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
        english, _ = Language.objects.get_or_create(id=1000,
                                                    name="English")
        campaign_creation.languages.add(english)

        # location rule
        geo_target = GeoTarget.objects.create(
            id=0, name="Hell", canonical_name="Hell", country_code="RU",
            target_type="place", status="hot",
        )
        LocationRule.objects.create(
            campaign_creation=campaign_creation,
            geo_target=geo_target,
        )
        FrequencyCap.objects.create(
            campaign_creation=campaign_creation,
            limit=10,
        )
        AdScheduleRule.objects.create(
            campaign_creation=campaign_creation,
            day=1,
            from_hour=6,
            to_hour=18,
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
        data = response.data
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
            is_approved=True,
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
        self.assertEqual(ad_group.is_approved, data['is_approved'])
        self.assertEqual(set(ad_group.genders), set(data['genders']))
        self.assertEqual(set(ad_group.parents), set(data['parents']))
        self.assertEqual(set(ad_group.age_ranges), set(data['age_ranges']))



