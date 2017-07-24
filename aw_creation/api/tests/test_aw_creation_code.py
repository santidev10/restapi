from django.core.urlresolvers import reverse
from rest_framework.status import HTTP_200_OK
from aw_creation.models import *
from aw_reporting.models import *
from aw_reporting.api.tests.base import AwReportingAPITestCase
from django.core.files.uploadedfile import SimpleUploadedFile


class CreationCodeAPITestCase(AwReportingAPITestCase):

    def test_success_get(self):
        user = self.create_test_user(auth=False)
        account = Account.objects.create(id="123", name="What")
        account_creation = AccountCreation.objects.create(
            name="", owner=user, account=account, is_approved=True
        )
        campaign_creation = CampaignCreation.objects.create(
            name="Sm name", account_creation=account_creation,
            budget="333.333",
            start="2017-05-10", end="2018-03-04",
            content_exclusions_raw=json.dumps(
                [i[0] for i in CampaignCreation.CONTENT_LABELS]
            ),
        )
        for language in default_languages():
            campaign_creation.languages.add(language)
        CampaignCreation.objects.create(
            name="", account_creation=account_creation
        )
        AdGroupCreation.objects.create(name="", campaign_creation=campaign_creation)
        ad_group_creation = AdGroupCreation.objects.create(
            name="Not empty", campaign_creation=campaign_creation, max_rate="0.07")

        AdCreation.objects.create(
            name="Spoke", ad_group_creation=ad_group_creation,
            video_url="https://www.youtube.com/watch?v=5HJyfoSZ2kw",
            display_url="www.nasdaq.com",
            final_url="https://www.nasdaq.com",
            video_thumbnail=SimpleUploadedFile(
                name='video_thumbnail.png',
                content=open("aw_creation/fixtures/video_thumbnail.png", 'rb').read(),
                content_type='image/png',
            ),
            tracking_template="https://iq.channelfactory.com",
            custom_params_raw='[{"name": "name", "value": "value1"}]',
        )
        AdCreation.objects.create(name="", ad_group_creation=ad_group_creation)
        # --
        url = reverse("aw_creation_urls:aw_creation_code", args=(account.id,))
        response = self.client.get(url)

        self.assertEqual(response.status_code, HTTP_200_OK)
        data = response.data

        self.assertIsInstance(data, dict)
        self.assertIn("code", data)
        # with open("code.js", "w") as f:
        #     f.write(data['code'])


