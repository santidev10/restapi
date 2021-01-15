from unittest import mock

import boto3
from django.conf import settings
from elasticsearch_dsl import Q
from moto import mock_s3

from .utils import get_params
from es_components.constants import LAST_VETTED_AT_MIN_DATE
from es_components.constants import Sections
from es_components.managers import ChannelManager
from es_components.tests.utils import ESTestCase
from es_components.models import Channel
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from performiq.tasks.generate_exports import generate_exports
from utils.unittests.int_iterator import int_iterator
from utils.unittests.test_case import ExtendedAPITestCase


class GenerateExportsTestCase(ExtendedAPITestCase, ESTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.channel_manager = ChannelManager(
            [Sections.STATS, Sections.GENERAL_DATA, Sections.ADS_STATS, Sections.TASK_US_DATA, Sections.BRAND_SAFETY])

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def _get_base_doc(self):
        """ Get document with field values required for export"""
        doc = Channel(f"channel_id_{next(int_iterator)}")
        doc.populate_task_us_data(last_vetted_at=LAST_VETTED_AT_MIN_DATE)
        doc.populate_brand_safety(overall_score=100)
        return doc

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
        with mock.patch("performiq.tasks.generate_exports.bulk_search", return_value=[[Channel(c.channel_id)                                                                 for c in clean]]):
            results = generate_exports(iq_campaign)
        lines = conn.Object(settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME,
                            results["recommended_export_filename"]).get()["Body"].read()
        # Skip header
        lines = lines.decode('utf-8').split()[1:]
        self.assertEqual(len(lines), len(clean))

    @mock_s3
    def test_optimized_ads_stats_export(self):
        """ Test optimized export returns correct matches """
        user = self.create_test_user()
        params = get_params(dict(
            average_cpm=0.5,
            average_cpv=0.5,
        ))
        iq_campaign = IQCampaign.objects.create(params=params, user=user)
        # Optimization for average_cpm / average_cpv should include documents
        # with values < param threshold
        doc1 = self._get_base_doc()
        doc1.populate_ads_stats(average_cpm=0.1, average_cpv=0.1)

        doc2 = self._get_base_doc()
        doc2.populate_ads_stats(average_cpm=1.2, average_cpv=0.4)

        doc3 = self._get_base_doc()
        doc3.populate_ads_stats(average_cpm=0.3, average_cpv=4.2)

        docs = [doc1, doc2, doc3]
        self.channel_manager.upsert(docs)
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)

        with mock.patch.object(ChannelManager, "forced_filters", return_value=Q()):
            results = generate_exports(iq_campaign)
        lines = conn.Object(settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME,
                            results["recommended_export_filename"]).get()["Body"].read()
        # Skip header
        lines = lines.decode('utf-8').split()[1:]
        # doc1 is the only doc with average_cpm and average_cpv less than param threshold
        self.assertEqual(lines[0].split("/channel/")[-1], doc1.main.id)

    @mock_s3
    def test_default_vetted_safe(self):
        """ Test that exports only include vetted safe items """
        user = self.create_test_user()
        params = get_params(dict(score_threshold=1))
        iq_campaign = IQCampaign.objects.create(params=params, user=user)

        doc1 = self._get_base_doc()
        doc1.populate_task_us_data(brand_safety=None, last_vetted_at=LAST_VETTED_AT_MIN_DATE)

        doc2 = self._get_base_doc()
        doc2.populate_task_us_data(brand_safety=["Profanity"], last_vetted_at=LAST_VETTED_AT_MIN_DATE)

        self.channel_manager.upsert([doc1, doc2])
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)

        with mock.patch.object(ChannelManager, "forced_filters", return_value=Q()):
            results = generate_exports(iq_campaign)
        lines = conn.Object(settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME,
                            results["recommended_export_filename"]).get()["Body"].read()
        # Skip header
        lines = lines.decode('utf-8').split()[1:]
        # doc1 vetted safe
        self.assertEqual(lines[0].split("/channel/")[-1], doc1.main.id)
