import json
import re

from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK

from aw_creation.models import AccountCreation, CampaignCreation, \
    default_languages, AdGroupCreation, AdCreation
from aw_reporting.api.tests.base import AwReportingAPITestCase, Account


class CreationCodeAPITestCase(AwReportingAPITestCase):

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
                name='video_thumbnail.png',
                content=open("aw_creation/fixtures/tests/video_thumbnail.png",
                             'rb').read(),
                content_type='image/png',
            ),
            tracking_template="https://iq.channelfactory.com",
            custom_params_raw='[{"name": "name", "value": "value1"}]',
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
        code = data['code']

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
