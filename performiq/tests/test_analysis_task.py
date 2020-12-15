from unittest import mock

import boto3
from moto import mock_s3
from django.conf import settings
from django.utils import timezone

from .utils import get_params
from performiq.analyzers.executor_analyzer import ExecutorAnalyzer
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers.constants import AnalysisResultSection
from performiq.analyzers import ChannelAnalysis
from performiq.models import IQCampaign
from utils.unittests.test_case import ExtendedAPITestCase
from es_components.tests.utils import ESTestCase
import performiq.tasks.start_analysis as start_analysis 
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
             mock.patch.object(ExecutorAnalyzer, "_merge_es_data", return_value=analyses), \
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
        with mock.patch.object(ExecutorAnalyzer, "_prepare_data", return_value=[]), \
             mock.patch.object(ExecutorAnalyzer, "_merge_es_data", return_value=[]), \
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
             mock.patch.object(ExecutorAnalyzer, "_merge_es_data", return_value=[]), \
             mock.patch("performiq.tasks.start_analysis.generate_exports", return_value=dict()),\
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create):
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        self.assertTrue(iq_campaign.results["no_placement_analyzed"], True)

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
             mock.patch.object(ExecutorAnalyzer, "_merge_es_data", return_value=analyses), \
             mock.patch("performiq.analyzers.executor_analyzer.safe_bulk_create", new=patch_bulk_create):
            start_analysis.start_analysis_task(iq_campaign.id, "", "")
        iq_campaign.refresh_from_db()
        results = iq_campaign.results
        # contextual params of empty lists means no params were set for analysis
        self.assertIsNone(results[AnalysisResultSection.CONTEXTUAL_RESULT_KEY]["overall_score"])
        self.assertIsNone(results[AnalysisResultSection.PERFORMANCE_RESULT_KEY]["overall_score"])
