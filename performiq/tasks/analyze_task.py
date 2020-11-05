from collections import namedtuple
from operator import attrgetter
from collections import defaultdict
from typing import (
    Dict, Text, Union, Tuple, Any, Optional, Mapping, Iterable, Callable, List, Type
)
from django.core.management import BaseCommand

from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS, COMPLETED_FIELDS
from aw_reporting.adwords_reports import placement_performance_report
from es_components.models import Channel
from es_components.managers import ChannelManager
from es_components.constants import SortDirections
from es_components.constants import SUBSCRIBERS_FIELD
from performiq.tasks import google_ads_update_scheduler
from performiq.analyzers.base_analyzer import GoogleAdsAnalyzer
from performiq.tasks.google_ads_scheduler import google_ads_update_scheduler
from performiq.tasks import update_campaigns_task
from performiq.utils.adwords_report import get_campaign_report
from performiq.models import OAuthAccount
from performiq.models import Campaign
from performiq.models import IQCampaign
from performiq.tasks.utils.get_google_ads_data import get_google_ads_data
from performiq.models import IQCampaignChannel
# from performiq.models.constants import CampaignData
from performiq.models.constants import CampaignDataFields
from performiq.oauth_utils import get_client
from segment.api.serializers import CTLParamsSerializer
from segment.utils.query_builder import SegmentQueryBuilder
from segment.utils.bulk_search import bulk_search
from utils.utils import chunks_generator
from performiq.models import Account
from performiq.models import Campaign
from performiq.models import IQCampaignChannel
from utils.db.functions import safe_bulk_create


class Coercers:
    @staticmethod
    def percentage(val):
        return float(val.strip("%"))

    @staticmethod
    def float(val):
        return float(val)

    @staticmethod
    def cost(val):
        return val / 10**6

    @staticmethod
    def integer(val):
        return int(val)

    @staticmethod
    def raw(val):
        return val

data_mapping = {
    "Impressions": CampaignDataFields.IMPRESSIONS,
    "VideoViews": CampaignDataFields.VIDEO_VIEWS,
    "Criteria": CampaignDataFields.CHANNEL_ID,
    "Ctr": CampaignDataFields.CTR,
    "AverageCpm": CampaignDataFields.CPM,
    "AverageCpv": CampaignDataFields.CPV,
    "ActiveViewViewability": CampaignDataFields.ACTIVE_VIEW_VIEWABILITY,

}
coerce_fields = {
    CampaignDataFields.IMPRESSIONS: Coercers.integer,
    CampaignDataFields.VIDEO_VIEWS: Coercers.integer,
    CampaignDataFields.CTR: Coercers.percentage,
    CampaignDataFields.CPM: Coercers.float,
    CampaignDataFields.CPV: Coercers.float,
    CampaignDataFields.ACTIVE_VIEW_VIEWABILITY: Coercers.percentage,
}


def analyze(iq_campaign_id=None, campaign_id=None):
    iq_campaign_id = 1
    iq_campaign = IQCampaign.objects.get(id=iq_campaign_id)
    mapped_api_data = get_google_ads_data(iq_campaign)

    performance_analyzer = PerformanceAnalyzer(iq_campaign)
    contextual_analyzer = ContextualAnalyzer(iq_campaign)
    suitability_analyzer = SuitabilityAnalyzer(iq_campaign)

    performance_results = performance_analyzer(mapped_api_data)
    iq_channels = performance_analyzer.iq_channels

    analyzers = [contextual_analyzer, suitability_analyzer]
    placement_results = process_channels(iq_channels, analyzers)
    all_results = {
        "performance_results": performance_results,
        "contextual_results": contextual_analyzer.results,
        "suitability_results": suitability_analyzer.results
    }
    pass


class PerformanceAnalyzer:
    def __init__(self, iq_campaign):
        self.iq_campaign = iq_campaign
        self.analyze_params = self.iq_campaign.params
        self.iq_campaign = iq_campaign
        self.iq_channels = None
        self._results = None

    def _create_data(self, mapped_api_data):
        to_create = [
            IQCampaignChannel(iq_campaign_id=self.iq_campaign.id,
                              channel_id=data[CampaignDataFields.CHANNEL_ID], meta_data=data)
            for data in mapped_api_data
        ]
        # safe_bulk_create(IQCampaignChannel, to_create)
        return to_create

    def __call__(self, data):
        created = self._create_data(data)
        self.iq_channels = created
        self._results = self._analyze(created)
        return self._results

    def _analyze(self, all_data: List[IQCampaignChannel]):
        results = defaultdict(lambda: dict(failed=0, passed=0))
        for item in all_data:
            # Get the Coercer method to map raw values from the api for comparisons
            data = {
                # Default to using the raw value if method not defined for key
                key: coerce_fields.get(key, Coercers.raw)(val) for key, val in item.meta_data.copy().items()
            }
            for metric_name, threshold in self.analyze_params.items():
                metric_value = data.get(metric_name, None)
                try:
                    if self._is_pass(metric_value, threshold):
                        results[metric_name]["passed"] += 1
                    else:
                        results[metric_name]["failed"] += 1
                    results[metric_name]["threshold"] = threshold
                except TypeError:
                    continue
        return results

    def _is_pass(self, value, threshold, direction="+"):
        if direction == "+":
            passed = value > threshold
        else:
            passed = value < threshold
        return passed


def process_channels(iq_channels, analyzers):
    placement_results = {}
    placement_ids_generator = (item.channel_id for item in iq_channels)
    channel_manager = ChannelManager(["general_data", "task_us_data", "brand_safety"])
    for batch in chunks_generator(placement_ids_generator, size=5000):
        channels = channel_manager.get(batch, skip_none=True)
        for channel in channels:
            # Each analyzer call returns True if the channel fails, else if the channel passes False
            analyzer_results = [analyzer(channel) for analyzer in analyzers]
            passed = all(fail is False for fail in analyzer_results)
            placement_results[channel.main.id] = passed
    return placement_results


class ContextualAnalyzer:
    ES_FIELD_RESULTS_MAPPING = {
        "general_data.primary_category": "content_categories",
        "general_data.top_lang_code": "languages",
        "task_us_data.content_quality": "content_quality",
        "task_us_data.content_type": "content_type",
    }

    def __init__(self, iq_campaign):
        self.iq_campaign = iq_campaign
        self.analyze_params = iq_campaign.params
        self._failed_count = 0
        self._result_counts = dict(
            languages_counts=defaultdict(int),
            content_categories_counts=defaultdict(int),
            content_quality_counts=defaultdict(int),
            content_type_counts=defaultdict(int),
        )

    @property
    def results(self):
        result = {
            "content_categories": self.analyze_params.get("content_categories", []),
            "languages": self.analyze_params.get("languages", []),
            "content_quality": self.analyze_params.get("content_quality", []),
            "content_type": self.analyze_params.get("content_type", []),
            **self._result_counts
        }
        return result

    def __call__(self, channel):
        contextual_failed = False
        for es_field, params_field in self.ES_FIELD_RESULTS_MAPPING.items():
            count_field = params_field + "_counts"
            # Get value of current field e.g. channel.general_data.primary_category
            attr_value = attrgetter(es_field)(channel)
            self._result_counts[count_field][attr_value] += 1
            # Check if current value is in campaign params e.g. If channel language is in params["languages"]
            if attr_value not in self.analyze_params[params_field]:
                contextual_failed = True
        if contextual_failed is True:
            self._failed_count += 1
        return contextual_failed


class SuitabilityAnalyzer:
    def __init__(self, iq_campaign):
        self.iq_campaign = iq_campaign
        self.analyze_params = iq_campaign.params
        self._result_counts = dict(
            passed=0,
            failed=0
        )

    @property
    def results(self):
        return self._result_counts

    def __call__(self, channel):
        suitability_failed = False
        try:
            if channel.brand_safety.overall_score > self.analyze_params["suitability"]:
                self._result_counts["passed"] += 1
            else:
                self._result_counts["failed"] += 1
                suitability_failed = True
        except TypeError:
            return
        return suitability_failed
