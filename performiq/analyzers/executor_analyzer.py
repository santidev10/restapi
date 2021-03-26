from operator import attrgetter
from typing import List

from elasticsearch_dsl import AttrList

from .base_analyzer import BaseAnalyzer
from .constants import COERCE_FIELD_FUNCS
from .base_analyzer import ChannelAnalysis
from .constants import DataSourceType
from .constants import ESFieldMapping
from es_components.managers import ChannelManager
from es_components.constants import Sections
from oauth.models import OAuthAccount
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers import SuitabilityAnalyzer
from performiq.analyzers import ContextualAnalyzer
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from performiq.models.constants import AnalysisFields
from performiq.tasks.utils.get_csv_data import get_csv_data
from performiq.tasks.utils.get_google_ads_data import get_google_ads_data
from performiq.tasks.utils.get_dv360_data import get_dv360_data
from utils.db.functions import safe_bulk_create
from utils.utils import chunks_generator


class ExecutorAnalyzer(BaseAnalyzer):
    """
    Manages PerformIQ analysis flow by providing data to all analyzers defined in self._analyzers
        and gathering / saving results

    Usage:
        1. Instantiate ExecutorAnalyzer which will prepare data
        2. Call analyze method
        3. Call get_results method to get formatted results from all analyzers
    """
    def __init__(self, iq_campaign: IQCampaign):
        self.iq_campaign = iq_campaign
        # Prepare results for each analyzer to add results to
        self.channel_analyses = self._prepare_data()
        self._analyzers = [
            PerformanceAnalyzer(iq_campaign.params),
            ContextualAnalyzer(iq_campaign.params),
            SuitabilityAnalyzer(iq_campaign.params),
        ]
        self.channel_manager = ChannelManager(
            sections=(Sections.GENERAL_DATA, Sections.TASK_US_DATA, Sections.BRAND_SAFETY),
            upsert_sections=None
        )

    def analyze(self, *args, **kwargs):
        """
        Main method to call analyze method for each analyzer used in PerformIQ analysis
        Saves results of IQCampaignChannels with _save_results method
        """
        for batch in chunks_generator(self.channel_analyses, size=5000):
            channel_data = self._merge_es_data(list(batch))
            for channel in channel_data:
                for analyzer in self._analyzers:
                    result = analyzer.analyze(channel)
                    channel.add_result(analyzer.RESULT_KEY, result)

    def _merge_es_data(self, channel_data: List[ChannelAnalysis]) -> List[ChannelAnalysis]:
        """
        Merges Elasticsearch data by adding to each ChannelAnalysis object using ESFieldMapping
        First attempt to extract a value using a ESFieldMapping.PRIMARY field. If the document also as a SECONDARY
            field, it is implied the final value should be a list with the combined values of the
            PRIMARY and SECONDARY fields
        :param channel_data: list -> ChannelAnalysis instantiations
        :return: list
        """
        by_id = {
            c.channel_id: c for c in channel_data
            if len(str(c.channel_id)) == 24
        }
        for batch in chunks_generator(by_id.keys(), size=2000):
            es_data = self.channel_manager.get(batch, skip_none=True)
            for channel in es_data:
                if not channel.main.id:
                    continue
                mapped = {}
                for es_field, mapped_key in ESFieldMapping.PRIMARY.items():
                    # Map multi dot attribute fields to single keys
                    attr_value = attrgetter(es_field)(channel)
                    coercer = COERCE_FIELD_FUNCS.get(mapped_key)
                    try:
                        # ESFieldMapping.PRIMARY value may be either a single or list value
                        combined = []
                        if isinstance(attr_value, (list, AttrList)):
                            combined.extend(attr_value)
                        elif attr_value is not None:
                            combined.append(attr_value)
                        # If has secondary field, it is implied that the final attr_value should be a list
                        secondary_field = ESFieldMapping.SECONDARY[es_field]
                        second_attr_value = attrgetter(secondary_field)(channel) or []
                        combined.extend(second_attr_value)
                        attr_value = combined
                    except (KeyError, AttributeError):
                        pass
                    # Not all fields will need to be coerced
                    mapped[mapped_key] = coercer(attr_value) if coercer and attr_value is not None else attr_value
                by_id[channel.main.id].add_data(mapped)
        return list(by_id.values())

    def get_results(self):
        """
        Gather and format results from each analyzer
        :return:
        """
        self._save_results()
        all_results = {
            analyzer.RESULT_KEY: analyzer.get_results()
            for analyzer in self._analyzers
        }
        # Calculate total score average for all analysis sections. Only factor in section overall score if analysis
        # was done (i.e. params were set for analysis)
        sum_score = 0
        sections_analyzed = 0
        for result in all_results.values():
            if result.get("overall_score") is not None:
                sum_score += result["overall_score"]
                sections_analyzed += 1
        try:
            total_score = round(sum_score / sections_analyzed, 4)
        except ZeroDivisionError:
            total_score = None
        all_results["total_score"] = total_score
        return all_results

    def calculate_wastage_statistics(self):
        """
        Calculates statistics for channels that do not pass analysis. This should be called only after the self.analyze
            method has been called
        """
        wastage = [analysis for analysis in self.channel_analyses if analysis.clean is False]
        total_spend = sum(analysis.get(AnalysisFields.COST, 0) or 0 for analysis in self.channel_analyses)
        wastage_spend = sum(analysis.get(AnalysisFields.COST, 0) or 0 for analysis in wastage)
        statistics = {
            "wastage_channels_percent": self.get_score(len(wastage), len(self.channel_analyses)),
            "wastage_spend": wastage_spend,
            "wastage_percent": wastage_spend / (total_spend or 1) * 100,
        }
        return statistics

    def _prepare_data(self) -> iter:
        """
        Retrieve data to create ChannelAnalysis objects to track results throughout analysis processes
        :return: dict
        """
        raw_data = self._get_data()
        channel_data = [ChannelAnalysis(data[AnalysisFields.CHANNEL_ID], data=data) for data in raw_data
                        if data.get(AnalysisFields.CHANNEL_ID)]
        return channel_data

    def _get_data(self):
        """
        Retrieve data from either APIs or CSV file
        Each function in GET_DATA_FUNCS should map their raw values using AnalysisFields to ensure that all keys
            are predictable throughout analysis process
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

        oauth_account = OAuthAccount.objects.get(oauth_type=self.iq_campaign.campaign.oauth_type,
                                                 user=self.iq_campaign.user)
        return oauth_account

    def _save_results(self) -> None:
        """
        Save final results stored in ChannelAnalysis objects
        :return: list
        """
        to_create = (
            IQCampaignChannel(
                iq_campaign=self.iq_campaign, clean=analysis.clean, meta_data=analysis.meta_data,
                channel_id=analysis.channel_id, results=analysis.results) for analysis in self.channel_analyses
        )
        safe_bulk_create(IQCampaignChannel, to_create)
