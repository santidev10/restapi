from typing import Dict

from es_components.managers import ChannelManager
from performiq.analyzers import IQChannelResult
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers import SuitabilityAnalyzer
from performiq.analyzers import ContextualAnalyzer
from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from performiq.models.constants import CampaignDataFields
from performiq.models.constants import OAuthType
from performiq.tasks.utils.get_google_ads_data import get_google_ads_data
from performiq.tasks.utils.get_dv360_data import get_dv360_data
from performiq.tasks.generate_exports import generate_exports
from saas import celery_app
from utils.db.functions import safe_bulk_create
from utils.utils import chunks_generator


GET_DATA_FUNCS = {
    OAuthType.GOOGLE_ADS.value: get_google_ads_data,
    OAuthType.DV360.value: get_dv360_data,
}


@celery_app.task
def analyze(oauth_account_id, iq_campaign_id):
    iq_campaign = IQCampaign.objects.get(id=iq_campaign_id)
    get_data_func = GET_DATA_FUNCS[iq_campaign.campaign.oauth_type]
    raw_data = get_data_func(iq_campaign, oauth_account_id=oauth_account_id)
    iq_channels = create_data(iq_campaign, raw_data)

    # Prepare dict results for each analyzer to add results to
    iq_results = {
        item.channel_id: IQChannelResult(item) for item in iq_channels
    }
    performance_analyzer = PerformanceAnalyzer(iq_campaign, iq_results)
    contextual_analyzer = ContextualAnalyzer(iq_campaign, iq_results)
    suitability_analyzer = SuitabilityAnalyzer(iq_campaign, iq_results)

    # PerformanceAnalyzer is analyzed separately since we have all available data (i.e. Either api data or from csv)
    # ContextualAnalyzer and SuitabilityAnalyzer rely on retrieving documents from Elasticsearch
    # which is done in batches
    performance_analyzer.analyze()
    analyzers = [contextual_analyzer, suitability_analyzer]
    process_channels(iq_results, analyzers)
    IQCampaignChannel.objects.bulk_update((r.iq_channel for r in iq_results.values()), fields=["clean", "results"])
    statistics = get_statistics(iq_results.values())
    all_results = {
        "params": iq_campaign.params,
        "performance_results": performance_analyzer.get_results(),
        "contextual_results": contextual_analyzer.get_results(),
        "suitability_results": suitability_analyzer.get_results(),
        "statistics": statistics,
    }
    iq_campaign.results.update(all_results)
    iq_campaign.save(update_fields=["results"])
    generate_exports(iq_campaign)


def process_channels(iq_results: Dict[str, IQChannelResult], analyzers: list):
    """
    Handle applying analyzers to each channel retrieved using iq_results
    Each analyzer will mutate result of channel being analyzed stored in iq_results
    :param iq_results: dict
    :param analyzers: list -> List of analzyers to apply to each channel
    :return: dict
    """
    channel_manager = ChannelManager(["general_data", "task_us_data", "brand_safety"])
    for batch in chunks_generator(iq_results.keys(), size=5000):
        channels = channel_manager.get(batch, skip_none=True)
        for channel in channels:
            # Each analyzer will mutate channel results in iq_results
            [analyzer(channel).analyze() for analyzer in analyzers]
    return iq_results


def create_data(iq_campaign: IQCampaign, raw_data: list):
    init_results = {
        key: {} for key in ANALYZE_SECTIONS
    }
    to_create = [
        IQCampaignChannel(iq_campaign_id=iq_campaign.id, results=init_results,
                          channel_id=data[CampaignDataFields.CHANNEL_ID], meta_data=data)
        for data in raw_data
    ]
    safe_bulk_create(IQCampaignChannel, to_create)
    return to_create


def get_statistics(iq_results):
    wastage_channels = [r.iq_channel for r in iq_results]
    statistics = {
        "wastage_channels_percent": len(wastage_channels) / len(iq_results) or 1,
        "wastage_spend": sum(iq_channel.meta_data[CampaignDataFields.COST] for iq_channel in wastage_channels),
    }
    return statistics

