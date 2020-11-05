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

PERFORMANCE_RESULT_KEY = "performance"
CONTEXTUAL_RESULT_KEY = "contextual"
SUITABILITY_RESULT_KEY = "suitability"


class IQChannelResult:
    def __init__(self, iq_channel: IQCampaignChannel):
        self.iq_channel = iq_channel
        self.results = {}

    def add_result(self, results_key, data):
        self.results[results_key] = data

    def fail(self):
        self.iq_channel.clean = False


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


def analyze(iq_campaign_id):
    iq_campaign = IQCampaign.objects.get(id=iq_campaign_id)
    api_data = get_google_ads_data(iq_campaign)
    iq_channels = create_data(iq_campaign, api_data)[:30]
    iq_results = {
        item.channel_id: IQChannelResult(item) for item in iq_channels
    }
    performance_analyzer = PerformanceAnalyzer(iq_campaign, iq_results)
    performance_results = performance_analyzer()

    contextual_analyzer = ContextualAnalyzer(iq_campaign, iq_results)
    suitability_analyzer = SuitabilityAnalyzer(iq_campaign, iq_results)
    analyzers = [contextual_analyzer, suitability_analyzer]
    placement_results = process_channels(iq_channels, analyzers)

    all_results = {
        "performance_results": performance_results,
        "contextual_results": contextual_analyzer.results,
        "suitability_results": suitability_analyzer.results,
        "params": iq_campaign.params
    }
    pass


class PerformanceAnalyzer:
    def __init__(self, iq_campaign, iq_results: Dict[str, IQChannelResult]):
        self.iq_campaign = iq_campaign
        self.analyze_params = self.iq_campaign.params
        self.iq_campaign = iq_campaign
        self.iq_results = iq_results
        self._results = None
        # If channel fails in any metric, it fails entirely
        # This will be set by _init_channel_results in the call method as we first need to retrieve API
        # data to set channel ids
        self._failed_channels = set()
        # Keep track of actual channels analyzed as a channel may not have sufficient data to analyze
        self._analyzed_channels_count = 0

    def __call__(self):
        self._results = self._analyze()
        try:
            overall_score = round((self._analyzed_channels_count - len(self._failed_channels))
                                  / self._analyzed_channels_count) * 100
        except ZeroDivisionError:
            overall_score = 0
        self._results["overall_score"] = overall_score
        return self._results

    def _analyze(self):
        total_results = defaultdict(lambda: dict(failed=0, passed=0))
        for iq_result in self.iq_results.values():
            iq_channel = iq_result.iq_channel
            curr_result = {}
            # Get the Coercer method to map raw values from the api for comparisons
            data = {
                # Default to using the raw value if method not defined for key
                key: coerce_fields.get(key, Coercers.raw)(val) for key, val in iq_channel.meta_data.copy().items()
            }
            analyzed = False
            for metric_name, threshold in self.analyze_params.items():
                metric_value = data.get(metric_name, None)
                try:
                    if self.passes(metric_value, threshold):
                        total_results[metric_name]["passed"] += 1
                    else:
                        total_results[metric_name]["failed"] += 1
                        self._failed_channels.add(iq_channel.channel_id)
                        self.iq_results[iq_channel.channel_id].fail()
                    curr_result[metric_name] = metric_value
                    total_results[metric_name]["threshold"] = threshold
                    analyzed = True
                except TypeError:
                    continue
            iq_result.add_result(PERFORMANCE_RESULT_KEY, curr_result)
            if analyzed is True:
                self._analyzed_channels_count += 1
        return dict(total_results)

    def passes(self, value, threshold, direction="+"):
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

    def __init__(self, iq_campaign, iq_results: Dict[str, IQChannelResult]):
        self.iq_campaign = iq_campaign
        self.iq_results = iq_results
        self.analyze_params = iq_campaign.params
        self._failed_channels = set()
        self._result_counts = dict(
            languages_counts=defaultdict(int),
            content_categories_counts=defaultdict(int),
            content_quality_counts=defaultdict(int),
            content_type_counts=defaultdict(int),
        )
        self._analyzed_channels_count = 0

    @property
    def results(self):
        passed_count = self._analyzed_channels_count - len(self._failed_channels)
        result = {
            "content_categories": self.analyze_params.get("content_categories", []),
            "languages": self.analyze_params.get("languages", []),
            "content_quality": self.analyze_params.get("content_quality", []),
            "content_type": self.analyze_params.get("content_type", []),
            "overall_score": get_overall_score(passed_count, self._analyzed_channels_count),
            **self._result_counts
        }
        return result

    def __call__(self, channel: Channel):
        contextual_failed = False
        analyzed = False
        result = {}
        for es_field, params_field in self.ES_FIELD_RESULTS_MAPPING.items():
            analyzed = True
            count_field = params_field + "_counts"
            # Get value of current field e.g. channel.general_data.primary_category
            attr_value = attrgetter(es_field)(channel)
            self._result_counts[count_field][attr_value] += 1
            # Check if current value is in campaign params e.g. If channel language is in params["languages"]
            if attr_value not in self.analyze_params[params_field]:
                contextual_failed = True
            result[params_field] = attr_value
        if contextual_failed is True:
            self.iq_results[channel.main.id].fail()
            self._failed_channels.add(channel.main.id)
        if analyzed is True:
            self._analyzed_channels_count += 1
        self.iq_results[channel.main.id].add_result(CONTEXTUAL_RESULT_KEY, result)
        return contextual_failed


class SuitabilityAnalyzer:
    def __init__(self, iq_campaign: IQCampaign, iq_results: Dict[str, IQChannelResult]):
        self.iq_campaign = iq_campaign
        self.iq_results = iq_results
        self.analyze_params = iq_campaign.params
        self._failed_channels = set()
        self._result_counts = dict(
            passed=0,
            failed=0
        )

    @property
    def results(self):
        total_count = self._result_counts["passed"] + self._result_counts["failed"]
        self._result_counts["overall_score"] = get_overall_score(self._result_counts["passed"], total_count)
        return self._result_counts

    def __call__(self, channel: Channel):
        suitability_failed = False
        result = {"overall_score": None}
        try:
            if channel.brand_safety.overall_score > self.analyze_params["suitability"]:
                self._result_counts["passed"] += 1
            else:
                self._result_counts["failed"] += 1
                self._failed_channels.add(channel.main.id)
                self.iq_results[channel.main.id].fail()
                suitability_failed = True
            result["overall_score"] = channel.brand_safety.overall_score
        except TypeError:
            return
        self.iq_results[channel.main.id].add_result(SUITABILITY_RESULT_KEY, result)
        return suitability_failed


def get_overall_score(passed, total):
    try:
        overall_score = passed // total * 100
    except ZeroDivisionError:
        overall_score = 0
    return overall_score


def create_data(iq_campaign, api_data):
    to_create = [
        IQCampaignChannel(iq_campaign_id=iq_campaign.id,
                          channel_id=data[CampaignDataFields.CHANNEL_ID], meta_data=data)
        for data in api_data
    ]
    # safe_bulk_create(IQCampaignChannel, to_create)
    return to_create
