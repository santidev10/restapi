from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS, COMPLETED_FIELDS
from aw_reporting.adwords_reports import placement_performance_report
from performiq.analyzers.constants import COERCE_FIELD_FUNCS
from performiq.models import IQCampaign
from performiq.models.constants import AnalysisFields
from performiq.oauth_utils import get_client
from performiq.models import Campaign


ADWORDS_API_FIELD_MAPPING = {
    "VideoViewRate": AnalysisFields.VIDEO_VIEW_RATE,
    "VideoQuartile100Rate": AnalysisFields.VIDEO_QUARTILE_100_RATE,
    "Impressions": AnalysisFields.IMPRESSIONS,
    "VideoViews": AnalysisFields.VIDEO_VIEWS,
    "Criteria": AnalysisFields.CHANNEL_ID,
    "Ctr": AnalysisFields.CTR,
    "Cost": AnalysisFields.COST,
    "AverageCpm": AnalysisFields.CPM,
    "AverageCpv": AnalysisFields.CPV,
    "ActiveViewViewability": AnalysisFields.ACTIVE_VIEW_VIEWABILITY,
}


def get_google_ads_data(iq_campaign: IQCampaign, **_):
    """
    Retrieve Placement Report for Campaign from Adwords API
    Returns dictionary of mapped keys to raw API data values. This is done so that data from multiple
        sources e.g. Google Ads, DV360 can be accessed with unified keys as API field names may differ.
        Preserve raw API data as it may be needed in it"s raw form during processing
    """
    campaign_id = iq_campaign.campaign_id
    campaign = Campaign.objects.get(id=campaign_id)
    account = campaign.account
    client = get_client(
        client_customer_id=account.id,
        refresh_token=account.oauth_account.refresh_token,
    )
    predicates = [
        {"field": "AdNetworkType1", "operator": "EQUALS", "values": ["YOUTUBE_WATCH"]},
        {"field": "CampaignId", "operator": "EQUALS", "values": [campaign.id]},
        {"field": "Impressions", "operator": "GREATER_THAN", "values": 100},
    ]
    fields = ("AdGroupId", "Criteria", "DisplayName", "Ctr", "AverageCpm", "AverageCpv", "VideoViewRate",
              "ActiveViewViewability") + MAIN_STATISTICS_FILEDS + COMPLETED_FIELDS
    report = placement_performance_report(client, predicates=predicates, fields=fields)
    all_rows = []
    for row in report:
        if "channel" not in row.DisplayName:
            continue
        formatted = {}
        # Create new dictionary of mapped keys to mapped API data values
        for report_field, mapped_key in ADWORDS_API_FIELD_MAPPING.items():
            coercer = COERCE_FIELD_FUNCS.get(mapped_key)
            api_value = getattr(row, report_field, None)
            formatted[mapped_key] = coercer(api_value) if coercer is not None else api_value
        all_rows.append(formatted)
    return all_rows
