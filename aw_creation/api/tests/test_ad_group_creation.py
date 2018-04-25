from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth.models import Group
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_400_BAD_REQUEST, \
    HTTP_403_FORBIDDEN, HTTP_204_NO_CONTENT

from aw_creation.models import *
from aw_reporting.demo.models import DemoAccount
from utils.utils_tests import ExtendedAPITestCase, \
    SingleDatabaseApiConnectorPatcher


class AdGroupAPITestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()
        self.user.add_custom_user_permission("view_media_buying")

    def create_ad_group(self, owner, start=None, end=None, account=None):
        account_creation = AccountCreation.objects.create(
            name="Pep", owner=owner, account=account,
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

    def test_success_fail_has_no_permission(self):
        self.user.remove_custom_user_permission("view_media_buying")

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
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

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
            'video_ad_format',
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
        account_creation = ad_group.campaign_creation.account_creation
        account_creation.is_deleted = True
        account_creation.save()

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
                "keyword": {"positive": ["spam", "ham"],
                            "negative": ["ai", "neural nets"]},
                "video": {"positive": ["iTKJ_itifQg"], "negative": ["1112yt"]},
            }
        )
        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.patch(
                url, json.dumps(data), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)

        account_creation.refresh_from_db()
        self.assertIs(account_creation.is_deleted, False)

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

    def test_success_put(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad_group = self.create_ad_group(**defaults)
        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))
        data = {
            "name": "AdGroup 1", "max_rate": 0,
            "targeting": {"keyword": {"positive": [], "negative": []},
                          "topic": {"positive": [], "negative": []},
                          "interest": {"positive": [], "negative": []},
                          "channel": {"positive": [], "negative": []},
                          "video": {"positive": [], "negative": []}},
            "age_ranges": [AdGroupCreation.AGE_RANGE_18_24,
                           AdGroupCreation.AGE_RANGE_25_34],
            "parents": [AdGroupCreation.PARENT_NOT_A_PARENT],
            "genders": [AdGroupCreation.GENDER_FEMALE],
        }

        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(
                url, json.dumps(data), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)

    def test_fail_put_too_many_targeting_items(self):
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad_group = self.create_ad_group(**defaults)
        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))
        data = {
            "name": "AdGroup 1", "max_rate": 0,
            "targeting": {"keyword": {"positive": [], "negative": []},
                          "topic": {"positive": [], "negative": []},
                          "interest": {"positive": [], "negative": []},
                          "channel": {"positive": [], "negative": []},
                          "video": {"positive": [], "negative": []}},
            "age_ranges": [AdGroupCreation.AGE_RANGE_18_24,
                           AdGroupCreation.AGE_RANGE_25_34],
            "parents": [AdGroupCreation.PARENT_NOT_A_PARENT],
            "genders": [AdGroupCreation.GENDER_FEMALE],
        }
        # 20K is the limit
        channel = data["targeting"]["channel"]
        video = data["targeting"]["video"]
        for i in range(5000):
            channel["positive"].append("cp{}".format(i))
            channel["negative"].append("cn{}".format(i))
            video["positive"].append("vp{}".format(i))
            video["negative"].append("vn{}".format(i))

        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(
                url, json.dumps(data), content_type='application/json',
            )

        self.assertEqual(response.status_code, HTTP_200_OK)

        # add one extra targeting item
        data["targeting"]["keyword"]["positive"].append("Rick&Morty")

        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(
                url, json.dumps(data), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_400_BAD_REQUEST)

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

        ad_group.refresh_from_db()
        self.assertIs(ad_group.is_deleted, True)

    def test_enterprise_user_can_edit_ad_group(self):
        user = self.user
        all_perm_groups = Group.objects.values_list('name', flat=True)
        for perm_group in all_perm_groups:
            user.add_custom_user_group(perm_group)
        today = datetime.now().date()
        defaults = dict(
            owner=self.user,
            start=today,
            end=today + timedelta(days=10),
        )
        ad_group = self.create_ad_group(**defaults)
        url = reverse("aw_creation_urls:ad_group_creation_setup",
                      args=(ad_group.id,))
        data = {
            "name": "AdGroup 1", "max_rate": 0,
            "targeting": {"keyword": {"positive": [], "negative": []},
                          "topic": {"positive": [], "negative": []},
                          "interest": {"positive": [], "negative": []},
                          "channel": {"positive": [], "negative": []},
                          "video": {"positive": [], "negative": []}},
            "age_ranges": [AdGroupCreation.AGE_RANGE_18_24,
                           AdGroupCreation.AGE_RANGE_25_34],
            "parents": [AdGroupCreation.PARENT_NOT_A_PARENT],
            "genders": [AdGroupCreation.GENDER_FEMALE],
        }

        with patch("aw_creation.api.serializers.SingleDatabaseApiConnector",
                   new=SingleDatabaseApiConnectorPatcher):
            response = self.client.put(
                url, json.dumps(data), content_type='application/json',
            )
        self.assertEqual(response.status_code, HTTP_200_OK)
