from django.core.urlresolvers import reverse
from django.utils import timezone
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
from aw_creation.models import *
from aw_reporting.demo.models import DemoAccount
from saas.utils_tests import ExtendedAPITestCase


class TargetingListImportFromUserListTestCase(ExtendedAPITestCase):

    def setUp(self):
        self.user = self.create_test_user()

    def create_ad_group(self):
        account = AccountCreation.objects.create(
            id="1", name="", owner=self.user,
        )
        campaign_creation = CampaignCreation.objects.create(
            account_creation=account, name="", 
        )
        ad_group_creation = AdGroupCreation.objects.create(
            id="1", name="", max_rate=0.01,
            campaign_creation=campaign_creation,
        )
        AccountCreation.objects.filter(pk=account.id).update(sync_at=timezone.now())
        account.refresh_from_db()
        self.assertEqual(account.is_changed, False)
        return ad_group_creation

    def test_success_import_keyword_list(self):
        from keyword_tool.models import KeywordsList, KeyWord

        ad_group = self.create_ad_group()
        kw_list = KeywordsList.objects.create(name="", user_email="1@2.3")
        for i in range(10):
            kw = KeyWord.objects.create(text="kw #{}".format(i))
            kw_list.keywords.add(kw)

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import_lists",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        response = self.client.post(
            url, json.dumps([kw_list.id]),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)

    def test_success_import_empty_list(self):
        from keyword_tool.models import KeywordsList

        ad_group = self.create_ad_group()
        kw_list = KeywordsList.objects.create(name="", user_email="1@2.3")

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import_lists",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        response = self.client.post(
            url, json.dumps([kw_list.id]),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_success_with_duplicates(self):
        from keyword_tool.models import KeywordsList, KeyWord

        ad_group = self.create_ad_group()
        kw_list = KeywordsList.objects.create(name="", user_email="1@2.3")
        for i in range(10):
            kw = KeyWord.objects.create(text="kw #{}".format(i))
            kw_list.keywords.add(kw)
            if i % 2:
                TargetingItem.objects.create(
                    ad_group_creation=ad_group,
                    criteria=kw.text,
                    type=TargetingItem.KEYWORD_TYPE,
                )

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import_lists",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        response = self.client.post(
            url, json.dumps([kw_list.id]),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_200_OK)
        self.assertEqual(len(response.data), 10)

    def test_fail_demo(self):
        from keyword_tool.models import KeywordsList, KeyWord

        ac = DemoAccount()
        campaign = ac.children[0]
        ad_group = campaign.children[0]

        kw_list = KeywordsList.objects.create(name="", user_email="1@2.3")
        for i in range(10):
            kw = KeyWord.objects.create(text="kw #{}".format(i))
            kw_list.keywords.add(kw)

        url = reverse(
            "aw_creation_urls:optimization_ad_group_targeting_import_lists",
            args=(ad_group.id, TargetingItem.KEYWORD_TYPE),
        )
        response = self.client.post(
            url, json.dumps([kw_list.id]),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, HTTP_403_FORBIDDEN)



