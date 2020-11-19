from operator import attrgetter
from typing import List, Dict

from .base_analyzer import BaseAnalyzer
from .constants import AnalyzeSection
from .base_analyzer import ChannelAnalysis
from .constants import DataSourceType
from es_components.managers import ChannelManager
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers import SuitabilityAnalyzer
from performiq.analyzers import ContextualAnalyzer
from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from performiq.models import OAuthAccount
from performiq.models.constants import AnalysisFields
from performiq.models.constants import OAuthType
from performiq.tasks.utils.get_csv_data import get_csv_data
from performiq.tasks.utils.get_google_ads_data import get_google_ads_data
from performiq.tasks.utils.get_dv360_data import get_dv360_data
from utils.db.functions import safe_bulk_create
from utils.utils import chunks_generator


ES_FIELD_RESULTS_MAPPING = {
    "general_data.primary_category": AnalysisFields.CONTENT_CATEGORIES,
    "general_data.top_lang_code": AnalysisFields.LANGUAGES,
    "task_us_data.content_quality": AnalysisFields.CONTENT_QUALITY,
    "task_us_data.content_type": AnalysisFields.CONTENT_TYPE,
    "brand_safety.overall_score": AnalysisFields.OVERALL_SCORE,
}


class ExecutorAnalyzer(BaseAnalyzer):
    """
    Manages PerformIQ analysis flow by providing data to all analyzers and gathering results
    """
    def __init__(self, iq_campaign: IQCampaign):
        self.iq_campaign = iq_campaign
        # Prepare results for each analyzer to add results to
        self.channel_analyses = self._prepare_data()
        self._analyzers = [
            PerformanceAnalyzer(self.iq_campaign.params),
            ContextualAnalyzer(self.iq_campaign.params),
            SuitabilityAnalyzer(self.iq_campaign.params),
        ]
        self.channel_manager = ChannelManager(["general_data", "task_us_data", "brand_safety"])

    def analyze(self, *args, **kwargs):
        """
        Main method to call analyze method for each analyzer used in PerformIQ analysis
        Saves results of IQCampaignChannels with _save_results method
        """
        for batch in chunks_generator(self.channel_analyses, size=10):
            channel_data = self._merge_es_data(batch)
            for channel in channel_data:
                for analyzer in self._analyzers:
                    result = analyzer.analyze(channel)
                    channel.add_result(analyzer.RESULT_KEY, result)
            break

    def _merge_es_data(self, channel_data):
        by_id = {
            c.channel_id: c for c in channel_data
        }
        es_data = self.channel_manager.get(by_id.keys(), skip_none=True)
        for channel in es_data:
            mapped = {}
            for es_field, mapped_key in ES_FIELD_RESULTS_MAPPING.items():
                attr_value = attrgetter(es_field)(channel)
                mapped[mapped_key] = attr_value
            by_id[channel.main.id].add_dict_data(mapped)
        return list(by_id.values())

    def get_results(self):
        """
        Format results from each analyzer
        :return:
        """
        all_results = {
            analyzer.RESULT_KEY: analyzer.get_results()
            for analyzer in self._analyzers
        }
        return all_results

    def calculate_wastage_statistics(self):
        """
        Calculates statistics for channels that do not pass analysis. This should be called only after the self.analyze
            method has been called
        """
        wastage_channels = [analysis for analysis in self.channel_analyses if analysis.clean is False]
        statistics = {
            "wastage_channels_percent": self.get_score(len(wastage_channels), len(self.channel_analyses)),
            "wastage_spend": sum(iq_channel.meta_data[AnalysisFields.COST] for iq_channel in wastage_channels),
        }
        return statistics

    def _prepare_data(self) -> iter:
        """
        Retrieve data to create IQCampaignChannels for analysis
        After db creation, a dict of channel_id: IQChannelResult key, values are created for analyzers
        :return: dict
        """
        raw_data = self._get_data()
        channel_data = (ChannelAnalysis(data[AnalysisFields.CHANNEL_ID], dict_data=data) for data in raw_data)
        return channel_data

    def _get_data(self):
        """
        Retrieve data from either Google Ads (Adwords API) / DV360 API's or CSV file
        Each function in GET_DATA_FUNCS should map their raw values using AnalysisFields to ensure that all keys
            are predictable
        :return: list
        """
        GET_DATA_FUNCS = {
            DataSourceType.GOOGLE_ADS.value: get_google_ads_data,
            DataSourceType.DV360.value: get_dv360_data,
            DataSourceType.CSV: get_csv_data,
        }
        if self.iq_campaign.params.get("csv_s3_key"):
            data_source = DataSourceType.CSV.value
            kwargs = dict()
        else:
            # Get the appropriate oauth account depending on the oauth type of IQCampaign
            oauth_account = self._get_oauth_account()
            data_source = DataSourceType(oauth_account.oauth_type).value
            kwargs = dict(oauth_account_id=oauth_account.id)
        get_data_func = GET_DATA_FUNCS[data_source]
        raw_data = get_data_func(self.iq_campaign, **kwargs)
        return raw_data

    def _get_oauth_account(self) -> OAuthAccount:
        """
        Get related OAuthAccount to current IQCampaign being processed. This method should only be used if
        the data source is either Google Ads (Adwords API) or DV360 API
        :return:
        """
        if self.iq_campaign.campaign.oauth_type == OAuthType.GOOGLE_ADS.value:
            oauth_account = self.iq_campaign.campaign.account.oauth_account
        else:
            oauth_account = self.iq_campaign.campaign.advertiser.oauth_accounts\
                .filter(oauth_type=OAuthType.DV360.value).first()
        return oauth_account

    def _save_results(self, channel_analyses: List[ChannelAnalysis]):
        to_create = (
            IQCampaignChannel(
                iq_campaign=self.iq_campaign, clean=analysis.clean, meta_data=analysis.meta_data,
                channel_id=analysis.channel_id, results=analysis.results) for analysis in channel_analyses
        )
        IQCampaignChannel.objects.bulk_create(to_create)
        return to_create

    def _get_es_data(self, channel_ids):
        channels = self.channel_manager.get(channel_ids, skip_none=True)
        mapped_data = []
        for channel in channels:
            mapped = {}
            for es_field, mapped_key in ES_FIELD_RESULTS_MAPPING:
                attr_value = str(attrgetter(es_field)(channel))
                mapped[mapped_key] = attr_value
            mapped_data.append(mapped)
        return mapped_data
