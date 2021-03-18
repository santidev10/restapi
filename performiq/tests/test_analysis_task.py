import io
from unittest import mock

import boto3
from moto import mock_s3
from django.conf import settings
from django.utils import timezone

from .utils import get_params
from es_components.tests.utils import ESTestCase
from oauth.models import Campaign
from oauth.models import OAuthAccount
from performiq.analyzers.base_analyzer import PerformIQDataFetchError
from performiq.analyzers.executor_analyzer import ExecutorAnalyzer
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers.constants import AnalysisResultSection
from performiq.analyzers import ChannelAnalysis
from performiq.models import IQCampaign
from performiq.models.constants import OAuthType
import performiq.tasks.start_analysis as start_analysis
from utils.unittests.test_case import ExtendedAPITestCase
from utils.unittests.int_iterator import int_iterator
from utils.unittests.patch_bulk_create import patch_bulk_create


class PerformIQAnalysisTestCase(ExtendedAPITestCase, ESTestCase):
    @mock_s3
    def test_results_keys(self):
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)

        params = get_params({})
        iq_campaign = IQCampaign.objects.create(params=params)
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data={})
        ]
        with mock.patch.object(ExecutorAnalyzer, "_prepare_data", return_value=analyses),\
             mock.patch("performiq.analyzers.executor_analyzer.ExecutorAnalyzer._merge_es_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create):
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        results = iq_campaign.results
        expected_export_result_keys = {"wastage_spend", "recommended_count", "wastage_export_filename",
                                       "wastage_channels_percent", "recommended_export_filename",
                                       "wastage_percent", "wastage_count"}
        self.assertEqual(set(results["exports"].keys()), expected_export_result_keys)
        self.assertTrue(AnalysisResultSection.PERFORMANCE_RESULT_KEY in results)
        self.assertTrue(AnalysisResultSection.CONTEXTUAL_RESULT_KEY in results)
        self.assertTrue(AnalysisResultSection.SUITABILITY_RESULT_KEY in results)

    def test_iqcampaign_status(self):
        before = timezone.now()
        params = get_params({})
        iq_campaign = IQCampaign.objects.create(params=params)
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data={})
        ]
        with mock.patch.object(ExecutorAnalyzer, "_prepare_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.ExecutorAnalyzer._merge_es_data", return_value=[]), \
             mock.patch("performiq.tasks.start_analysis.generate_exports", return_value=dict()),\
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create),\
             mock.patch("performiq.tasks.start_analysis._send_completion_email") as mock_email:
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        self.assertTrue(iq_campaign.started > before)
        self.assertTrue(iq_campaign.completed > iq_campaign.started)
        mock_email.assert_called_once()

    def test_iqcampaign_empty_results(self):
        """ Test that resuls no_placement_analyzed is True if no placements to analyze """
        params = get_params({})
        iq_campaign = IQCampaign.objects.create(params=params)
        with mock.patch.object(ExecutorAnalyzer, "_prepare_data", return_value=[]), \
             mock.patch("performiq.analyzers.executor_analyzer.ExecutorAnalyzer._merge_es_data", return_value=[]), \
             mock.patch("performiq.tasks.start_analysis.generate_exports", return_value=dict()),\
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create),\
             mock.patch("performiq.tasks.start_analysis._send_completion_email") as mock_email:
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        self.assertTrue(iq_campaign.results["no_placement_analyzed"], True)
        mock_email.assert_called_once()

    @mock_s3
    def test_no_filters_null_results(self):
        """ Test that setting no filters for analysis sets None result """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)

        performance_null_params = {
            key: None for key in PerformanceAnalyzer.ANALYSIS_FIELDS
        }
        params = get_params(performance_null_params)
        iq_campaign = IQCampaign.objects.create(params=params)
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data={})
        ]
        with mock.patch.object(ExecutorAnalyzer, "_prepare_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.ExecutorAnalyzer._merge_es_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create):
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        results = iq_campaign.results
        # contextual params of empty lists means no params were set for analysis
        self.assertIsNone(results[AnalysisResultSection.CONTEXTUAL_RESULT_KEY]["overall_score"])
        self.assertIsNone(results[AnalysisResultSection.PERFORMANCE_RESULT_KEY]["overall_score"])

    @mock_s3
    def test_null_results_excluded_total_score(self):
        """ Test that analyzers with None overall scores do not contribute to total score.
         An analyzer may have a None overall score if no params were set"""
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)

        performance_null_params = {
            key: None for key in PerformanceAnalyzer.ANALYSIS_FIELDS
        }
        _params = dict(
            # params set for contextual and suitability, but not performance
            content_type=[0],
            score_threshold=1,
            **performance_null_params
        )
        params = get_params(_params)
        iq_campaign = IQCampaign.objects.create(params=params)
        analysis_data = dict(
            # Pass both contextual and suitability analysis
            content_type=0,
            overall_score=100,
        )
        analyses = [
            ChannelAnalysis(f"channel_id_{next(int_iterator)}", data=analysis_data)
        ]
        with mock.patch.object(ExecutorAnalyzer, "_prepare_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.ExecutorAnalyzer._merge_es_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create):
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        results = iq_campaign.results
        # No params set for performance, so overall score should be None and not factored into average total score
        # Params set for contextual and suitability and with analysis passed for both sections. Total score
        # should be calculated as 100 (contextual) + 100 (suitability) / 2
        self.assertEqual(results["total_score"], 100)

    @mock_s3
    def test_csv_encoding(self):
        """ Test different csv encodings are supported """
        conn = boto3.resource("s3", region_name="us-east-1")
        conn.create_bucket(Bucket=settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME)
        # Create channel id of length 24 starting with UC
        channel_id = "UC" + "0" * 22
        csv_filename = "test_csv_encoding.csv"
        csv_file = io.BytesIO(f"http://youtube.com/channel/{channel_id},0.05".encode("utf-16"))
        csv_file.seek(0)
        conn.Object(settings.AMAZON_S3_PERFORMIQ_CUSTOM_CAMPAIGN_UPLOADS_BUCKET_NAME, csv_filename).put(Body=csv_file)
        params = get_params(dict(
            cpm=0.1,
            csv_s3_key=csv_filename,
            csv_column_mapping={
                "URL": "A",
                "Avg CPM": "B"
            }
        ))
        iq_campaign = IQCampaign.objects.create(params=params)
        analyses = [
            ChannelAnalysis(channel_id, data=dict(cpm=1))
        ]
        with mock.patch("performiq.analyzers.executor_analyzer.ExecutorAnalyzer._merge_es_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create):
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        self.assertTrue(iq_campaign.results and iq_campaign.completed is not None)

    def test_performiq_data_error(self):
        """ Test that error is saved if exception is thrown while fetching oauth data
            and that the oauth account oauth status is revoked """
        user = self.create_test_user()
        with self.subTest("Catch DV360 oauth error"),\
                mock.patch("performiq.analyzers.executor_analyzer.get_dv360_data", side_effect=PerformIQDataFetchError),\
                mock.patch("performiq.tasks.start_analysis._send_completion_email") as mock_email:
            oauth = OAuthAccount.objects.create(oauth_type=OAuthType.DV360.value, user=user)

            campaign = Campaign.objects.create(oauth_type=OAuthType.DV360.value)
            iq_campaign = IQCampaign.objects.create(user=user, campaign=campaign)
            with mock.patch.object(ExecutorAnalyzer, "_get_oauth_account", return_value=oauth):
                start_analysis.start_analysis_task(iq_campaign.id, "", "")
            iq_campaign.refresh_from_db()
            oauth.refresh_from_db()
            self.assertTrue(iq_campaign.results["error"])
            self.assertEqual(oauth.is_enabled, False)
            mock_email.assert_not_called()

        with self.subTest("Catch Adwords oauth error"),\
                mock.patch("performiq.analyzers.executor_analyzer.get_google_ads_data", side_effect=PerformIQDataFetchError),\
                mock.patch("performiq.tasks.start_analysis._send_completion_email") as mock_email:
            oauth = OAuthAccount.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value, user=user)

            campaign = Campaign.objects.create(oauth_type=OAuthType.GOOGLE_ADS.value)
            iq_campaign = IQCampaign.objects.create(user=user, campaign=campaign)
            with mock.patch.object(ExecutorAnalyzer, "_get_oauth_account", return_value=oauth):
                start_analysis.start_analysis_task(iq_campaign.id, "", "")
            iq_campaign.refresh_from_db()
            oauth.refresh_from_db()
            self.assertTrue(iq_campaign.results["error"])
            self.assertEqual(oauth.is_enabled, False)
            mock_email.assert_not_called()
