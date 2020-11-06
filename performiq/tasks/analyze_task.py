from es_components.managers import ChannelManager
from performiq.analyzers import IQChannelResult
from performiq.analyzers import PerformanceAnalyzer
from performiq.analyzers import SuitabilityAnalyzer
from performiq.analyzers import ContextualAnalyzer
from performiq.analyzers.constants import ANALYZE_SECTIONS
from performiq.models import IQCampaign
from performiq.models import IQCampaignChannel
from performiq.models.constants import CampaignDataFields
from performiq.tasks.utils.get_google_ads_data import get_google_ads_data
from performiq.tasks.generate_exports import generate_exports
from utils.utils import chunks_generator


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
    generate_exports(iq_campaign)


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



def create_data(iq_campaign, api_data):
    init_results = {
        key: {} for key in ANALYZE_SECTIONS
    }
    to_create = [
        IQCampaignChannel(iq_campaign_id=iq_campaign.id, results=init_results,
                          channel_id=data[CampaignDataFields.CHANNEL_ID], meta_data=data)
        for data in api_data
    ]
    # safe_bulk_create(IQCampaignChannel, to_create)
    return to_create
