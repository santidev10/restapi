import json
import re

from django.core.files.uploadedfile import SimpleUploadedFile
from django.urls import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models import default_languages
from aw_reporting.api.tests.base import Account
from aw_reporting.api.tests.base import AwReportingAPITestCase
from aw_reporting.models import BudgetType
from es_components.tests.utils import ESTestCase
from utils.unittests.int_iterator import int_iterator


class CreationCodeAPITestCase(AwReportingAPITestCase, ESTestCase):

    def test_success_get(self):
        self.create_test_user(auth=False)
        account = Account.objects.create(id="123", name="What")
        account_creation = account.account_creation
        account_creation.is_managed = True
        account_creation.save()
        campaign_creation = CampaignCreation.objects.create(
            id=1,
            name="Sm name", account_creation=account_creation,
            budget="333.333",
            is_deleted=True,
            content_exclusions_raw=json.dumps(
                [i[0] for i in CampaignCreation.CONTENT_LABELS]
            ),
        )
        for language in default_languages():
            campaign_creation.languages.add(language)
        CampaignCreation.objects.create(
            id=2, name="", account_creation=account_creation
        )
        AdGroupCreation.objects.create(id=1, name="",
                                       campaign_creation=campaign_creation)
        ad_group_creation = AdGroupCreation.objects.create(
            id=2, name="Not empty", campaign_creation=campaign_creation,
            max_rate="0.07")

        AdCreation.objects.create(
            id=1,
            name="Spoke", ad_group_creation=ad_group_creation,
            video_url="https://www.youtube.com/watch?v=5HJyfoSZ2kw",
            display_url="www.nasdaq.com",
            final_url="https://www.nasdaq.com",
            companion_banner=SimpleUploadedFile(
                name="video_thumbnail.png",
                content=open("aw_creation/fixtures/tests/video_thumbnail.png",
                             "rb").read(),
                content_type="image/png",
            ),
            tracking_template="https://iq.channelfactory.com",
            custom_params_raw="""[{"name": "name", "value": "value1"}]""",
        )
        AdCreation.objects.create(id=2, name="",
                                  ad_group_creation=ad_group_creation)
        # --
        url = reverse("aw_creation_urls:aw_creation_code", args=(account.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data

        self.assertIsInstance(data, dict)
        self.assertIn("code", data)
        code = data["code"]

        self.assertIsNotNone(
            re.search(r"createOrUpdateCampaign\(.*?\"id\": 1", code,
                      re.MULTILINE)
        )
        self.assertIsNone(
            re.search(r"createOrUpdateCampaign\(.*?\"id\": 2", code,
                      re.MULTILINE)
        )

        self.assertIsNotNone(
            re.search(r"createOrUpdateAdGroup\(.*?\"id\": 2", code,
                      re.MULTILINE)
        )
        self.assertIsNone(
            re.search(r"createOrUpdateAdGroup\(.*?\"id\": 1", code,
                      re.MULTILINE)
        )

        self.assertIsNotNone(
            re.search(r"createOrUpdateVideoAd\(.*?\"id\": 1", code,
                      re.MULTILINE)
        )
        self.assertIsNone(
            re.search(r"createOrUpdateVideoAd\(.*?\"id\": 2", code,
                      re.MULTILINE)
        )

    def test_campaign_budget_type(self):
        self.create_test_user(auth=False)
        account = Account.objects.create(id="123", name="What")
        account_creation = account.account_creation
        account_creation.is_managed = True
        account_creation.save()
        campaign_daily = CampaignCreation.objects.create(
            id=next(int_iterator),
            name="",
            account_creation=account_creation,
            budget=1,
            budget_type=BudgetType.DAILY.value
        )
        campaign_total = CampaignCreation.objects.create(
            id=next(int_iterator),
            name="",
            account_creation=account_creation,
            budget=1,
            budget_type=BudgetType.TOTAL.value
        )
        url = reverse("aw_creation_urls:aw_creation_code", args=(account.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data

        self.assertIsInstance(data, dict)
        self.assertIn("code", data)
        code = data["code"]

        def get_campaign_creation_invocation(campaign_id):
            return re.search(
                r"createOrUpdateCampaign\(.*?\"id\": {}.*$".format(campaign_id),
                code,
                re.MULTILINE
            ).group(0)

        daily_campaign_creation = get_campaign_creation_invocation(campaign_daily.id)
        total_campaign_creation = get_campaign_creation_invocation(campaign_total.id)
        self.assertIn("\"budget_type\": \"{}\"".format(BudgetType.DAILY.value), daily_campaign_creation)
        self.assertIn("\"budget_type\": \"{}\"".format(BudgetType.TOTAL.value), total_campaign_creation)

    def test_ad_creation_parameters(self):
        self.create_test_user(auth=False)
        account = Account.objects.create(id="123", name="What")
        account_creation = account.account_creation
        account_creation.is_managed = True
        account_creation.save()
        campaign = CampaignCreation.objects.create(
            id=next(int_iterator),
            name="",
            account_creation=account_creation,
            budget=1,
            budget_type=BudgetType.DAILY.value
        )
        ad_group = AdGroupCreation.objects.create(
            id=next(int_iterator),
            campaign_creation=campaign,
            max_rate=1,
        )
        ad = AdCreation.objects.create(
            id=next(int_iterator),
            ad_group_creation=ad_group,
            headline="Headline",
            description_1="Description 1",
            description_2="Description 2",
            video_url="http://youtube.com",
            display_url="http://youtube.com",
            final_url="http://youtube.com",
        )
        url = reverse("aw_creation_urls:aw_creation_code", args=(account.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data

        self.assertIsInstance(data, dict)
        self.assertIn("code", data)
        code = data["code"]
        ad_creation = re.search(
            r"createOrUpdateVideoAd\(.*?\"id\": {}.*$".format(ad.id),
            code,
            re.MULTILINE
        ).group(0)

        fields = ("headline", "description_1", "description_2")
        for field in fields:
            self.assertIn("\"{}\": \"{}\"".format(field, getattr(ad, field)), ad_creation)
