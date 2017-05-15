from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, \
    HTTP_202_ACCEPTED
from urllib.parse import urlencode
from aw_campaign_creation.models import *
from utils.utils_tests import ExtendedAPITestCase as APITestCase
from aw_campaign.models import Topic


class TopicTargetingListTestCase(APITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    @staticmethod
    def create_ad_group():
        account = AccountManagement.objects.create(
            id="1", name="", brand="",
        )
        campaign_management = CampaignManagement.objects.create(
            account_management=account,
            name="", start="2017-01-01", end="2017-05-01",
            initial_goal_units=1000,
            goal_units=1000,
            goal_type="",
            budget=200,
            max_rate="0.08",
        )
        ad_group_management = AdGroupManagement.objects.create(
            id="1",
            campaign_management=campaign_management, name="",
            max_rate="10.1",
            video_url="https://www.youtube.com/video/woKSCH3e1Yg",
        )
        account.is_changed = False
        account.save()
        return ad_group_management

    def test_success_get(self):
        self.user.add_permission("campaign_creation")
        ad_group = self.create_ad_group()
        for i in range(10):
            Topic.objects.create(id=i, name="Topic#{}".format(i))
            TargetingItem.objects.create(
                criteria=i,
                ad_group_management=ad_group,
                type=TargetingItem.TOPIC_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:ad_group_targeting_list",
            args=(ad_group.id, TargetingItem.TOPIC_TYPE),
        )

        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(response.data[0].keys()),
            {
                'criteria',
                'is_negative',
                'name',
            }
        )
        self.assertIs(
            any(i['is_negative'] for i in response.data[:5]), False)
        self.assertIs(
            all(i['is_negative'] for i in response.data[5:]), True)

    def test_success_post(self):
        self.user.add_permission("campaign_creation")
        ad_group = self.create_ad_group()
        for i in range(10):
            Topic.objects.create(id=i, name="Topic#{}".format(i))

        url = reverse(
            "aw_creation_urls:ad_group_targeting_list",
            args=(ad_group.id, TargetingItem.TOPIC_TYPE),
        )

        data = [i for i in range(20)]
        response = self.client.post(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            set(str(i) for i in range(10)),
        )
        self.assertIs(
            any(i['is_negative'] for i in response.data),
            False,
        )
        ad_group.campaign_management.account_management.refresh_from_db()
        self.assertIs(
            ad_group.campaign_management.account_management.is_changed,
            True,
        )

    def test_success_post_negative(self):
        self.user.add_permission("campaign_creation")
        ad_group = self.create_ad_group()
        for i in range(10):
            Topic.objects.create(id=i, name="Topic#{}".format(i))

        url = reverse(
            "aw_creation_urls:ad_group_targeting_list",
            args=(ad_group.id, TargetingItem.TOPIC_TYPE),
        )

        data = [i for i in range(20)]
        response = self.client.post(
            "{}?is_negative=1".format(url),
            json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            set(str(i) for i in range(10)),
        )
        self.assertIs(
            all(i['is_negative'] for i in response.data),
            True,
        )
        ad_group.campaign_management.account_management.refresh_from_db()
        self.assertIs(
            ad_group.campaign_management.account_management.is_changed,
            True,
        )

    def test_success_delete(self):
        self.user.add_permission("campaign_creation")
        ad_group = self.create_ad_group()
        for i in range(10):
            Topic.objects.create(id=i, name="Topic#{}".format(i))
            TargetingItem.objects.create(
                criteria=i,
                ad_group_management=ad_group,
                type=TargetingItem.TOPIC_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:ad_group_targeting_list",
            args=(ad_group.id, TargetingItem.TOPIC_TYPE),
        )

        data = [i for i in range(5)]

        response = self.client.delete(
            url, json.dumps(data), content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 5)
        self.assertEqual(
            set(i['criteria'] for i in response.data),
            {str(i) for i in range(5, 10)}
        )
        ad_group.campaign_management.account_management.refresh_from_db()
        self.assertIs(
            ad_group.campaign_management.account_management.is_changed,
            True,
        )

    def test_export_list(self):
        self.user.add_permission("campaign_creation")
        ad_group = self.create_ad_group()
        for i in range(10):
            Topic.objects.create(id=i, name="Topic#{}".format(i))
            TargetingItem.objects.create(
                criteria=i,
                ad_group_management=ad_group,
                type=TargetingItem.TOPIC_TYPE,
                is_negative=i % 2,
            )

        url = reverse(
            "aw_creation_urls:ad_group_targeting_list_export",
            args=(ad_group.id, TargetingItem.TOPIC_TYPE),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)

        url = "{}?{}".format(
            str(url),
            urlencode({'auth_token': self.user.auth_token.key}),
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, HTTP_200_OK)
        lines = list(response)
        self.assertEqual(len(lines), 11)

    def test_import_list(self):
        self.user.add_permission("campaign_creation")
        ad_group = self.create_ad_group()
        for i in range(3):
            Topic.objects.create(id=i, name="Topic#{}".format(i))

        url = reverse(
            "aw_creation_urls:ad_group_targeting_list_import",
            args=(ad_group.id, TargetingItem.TOPIC_TYPE),
        )
        with open('aw_campaign_creation/fixtures/'
                  'import_topics_list.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        with open('aw_campaign_creation/fixtures/'
                  'import_topics_list.csv', 'rb') as fp:
            response = self.client.post(url, {'file': fp},
                                        format='multipart')
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        ad_group.campaign_management.account_management.refresh_from_db()
        self.assertIs(
            ad_group.campaign_management.account_management.is_changed,
            True,
        )



