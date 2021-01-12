from unittest import mock

import boto3
from django.conf import settings
from moto import mock_s3

from es_components.models import Channel
from utils.unittests.test_case import ExtendedAPITestCase
from performiq.tasks.generate_exports import generate_exports
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from utils.unittests.int_iterator import int_iterator


class GenerateExportsTestCase(ExtendedAPITestCase):
    @mock_s3
    def test_no_duplicates(self):
        """ Test that recommended export does not contain duplicates """
        user = self.create_test_user()
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)
        iq_campaign = IQCampaign.objects.create(user=user, params={})
        clean = [
            IQCampaignChannel.objects.create(
                iq_campaign=iq_campaign, clean=True, channel_id=f"channel_id_{next(int_iterator)}".zfill(24)
            ) for _ in range(5)
        ]
        with mock.patch("performiq.tasks.generate_exports.bulk_search", return_value=[[Channel(c.channel_id)
                                                                                       for c in clean]]):
            results = generate_exports(iq_campaign)
        lines = conn.Object(settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME,
                            results["recommended_export_filename"]).get()["Body"].read()
        # Skip header
        lines = lines.decode('utf-8').split()[1:]
        self.assertEqual(len(lines), len(clean))
