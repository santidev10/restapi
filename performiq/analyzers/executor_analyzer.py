from typing import List, Dict

from .base_analyzer import BaseAnalyzer
from .constants import DataSourceType
from es_components.managers import ChannelManager
from performiq.analyzers import IQChannelResult
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers import SuitabilityAnalyzer
from performiq.analyzers import ContextualAnalyzer
from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from performiq.models import OAuthAccount
from performiq.models.constants import CampaignDataFields
from performiq.models.constants import OAuthType
from performiq.tasks.utils.get_csv_data import get_csv_data
from performiq.tasks.utils.get_google_ads_data import get_google_ads_data
from performiq.tasks.utils.get_dv360_data import get_dv360_data
from utils.db.functions import safe_bulk_create
from utils.utils import chunks_generator


class ExecutorAnalyzer(BaseAnalyzer):
    """
    Manages PerformIQ analysis flow by providing data to all analyzers and gathering results
    """
    def __init__(self, iq_campaign: IQCampaign):
        self.iq_campaign = iq_campaign
        # Prepare dict results for each analyzer to add results to
        self._iq_channel_results = self._prepare_data()
        # Prepare analyzers with results
        self._performance_analyzer = PerformanceAnalyzer(iq_campaign, self._iq_channel_results)
        self._contextual_analyzer = ContextualAnalyzer(iq_campaign, self._iq_channel_results)
        self._suitability_analyzer = SuitabilityAnalyzer(iq_campaign, self._iq_channel_results)

    def analyze(self, *args, **kwargs):
        """
        Main method to call analyze method for each analyzer used in PerformIQ analysis
        Saves results of IQCampaignChannels with _save_results method
        """
        # PerformanceAnalyzer is analyzed separately since we have all available data (i.e. Either api data or from csv)
        # ContextualAnalyzer and SuitabilityAnalyzer rely on retrieving documents from Elasticsearch which
        # is done in batches
        self._performance_analyzer.analyze()
        channel_metadata_analyzers = [self._contextual_analyzer, self._suitability_analyzer]
        self._analyze_channels(channel_metadata_analyzers)
        self._save_results()

    def get_results(self):
        """
        Format results from each analyzer
        :return:
        """
        all_results = {
            "performance_results": self._performance_analyzer.get_results(),
            "contextual_results": self._contextual_analyzer.get_results(),
            "suitability_results": self._suitability_analyzer.get_results(),
        }
        return all_results

    def calculate_wastage_statistics(self):
        """
        Calculates statistics for channels that do not pass analysis. This should be called only after the self.analyze
            method has been called
        """
        wastage_channels = [r.iq_channel for r in self._iq_channel_results.values() if r.iq_channel.clean is False]
        statistics = {
            "wastage_channels_percent": self.get_score(len(wastage_channels), len(self._iq_channel_results)),
            "wastage_spend": sum(iq_channel.meta_data[CampaignDataFields.COST] for iq_channel in wastage_channels),
        }
        return statistics

    def _analyze_channels(self, analyzers: list):
        """
        Method that applies analyzers to each channel retrieved using iq_results
        Each analyzer will mutate the result of each channel being analyzed, which is stored in self._iq_channel_results
            which was passed during each analyzer instantiation in __init__ method
        Each analyzer in analyzer must implement an analyze method that accepts a es_components.models.Channel object
        :param analyzers: list -> List of analyzers to apply to each channel
        :return: dict
        """
        channel_manager = ChannelManager(["general_data", "task_us_data", "brand_safety"])
        for batch in chunks_generator(self._iq_channel_results.keys(), size=5000):
            channels = channel_manager.get(batch, skip_none=True)
            for channel in channels:
                # Each analyzer will mutate channel results in self._iq_channel_results, as each analyzer should
                # have been instantiated with the same iq_channel_results dict in the __init__ method
                [analyzer.analyze(channel) for analyzer in analyzers]

    def _prepare_data(self) -> Dict[str, IQChannelResult]:
        """
        Retrieve data to create IQCampaignChannels for analysis
        After db creation, a dict of channel_id: IQChannelResult key, values are created for analyzers
        :return: dict
        """
        data = self._get_data()
        created_iq_channels = self._create_data(data)
        # This dict will be passed into each analyzer to be mutated with results
        iq_results = {
            item.channel_id: IQChannelResult(item) for item in created_iq_channels
        }
        return iq_results

    def _get_data(self):
        """
        Retrieve data from either Google Ads (Adwords API) / DV360 API's or CSV file
        Each function in GET_DATA_FUNCS should map their raw values using CampaignDataFields to ensure that all keys
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

    def _create_data(self, raw_data: List[dict]) -> List[IQCampaignChannel]:
        """
        Uses raw data retrieved from _get_data method to create IQCampaignChannels for current self.iq_campaign
        """
        init_results = {
            key: {} for key in ANALYZE_SECTIONS
        }
        to_create = [
            IQCampaignChannel(iq_campaign_id=self.iq_campaign.id, results=init_results,
                              channel_id=data[CampaignDataFields.CHANNEL_ID], meta_data=data)
            for data in raw_data
        ]
        safe_bulk_create(IQCampaignChannel, to_create)
        return to_create

    def _save_results(self):
        IQCampaignChannel.objects.bulk_update((r.iq_channel for r in self._iq_channel_results.values()),
                                              fields=["clean", "results"])