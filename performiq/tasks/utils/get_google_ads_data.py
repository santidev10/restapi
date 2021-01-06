from aw_reporting.adwords_reports import MAIN_STATISTICS_FILEDS, COMPLETED_FIELDS
from aw_reporting.adwords_reports import placement_performance_report
from performiq.analyzers.constants import ADWORDS_COERCE_FIELD_FUNCS
from performiq.models import IQCampaign
from performiq.models import OAuthAccount
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
    "Clicks": AnalysisFields.CLICKS,
    "ActiveViewMeasurableImpressions": AnalysisFields.ACTIVE_VIEW_MEASURABLE_IMPRESSIONS,
    "ActiveViewImpressions": AnalysisFields.ACTIVE_VIEW_IMPRESSIONS,
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
    oauth_account = OAuthAccount.objects.get(user=iq_campaign.user, oauth_type=campaign.oauth_type)
    account = campaign.account
    client = get_client(
        client_customer_id=account.id,
        refresh_token=oauth_account.refresh_token,
    )
    predicates = [
        {"field": "AdNetworkType1", "operator": "EQUALS", "values": ["YOUTUBE_WATCH", "YOUTUBE_SEARCH"]},
        {"field": "CampaignId", "operator": "EQUALS", "values": [campaign.id]},
        {"field": "Impressions", "operator": "GREATER_THAN", "values": 100},
    ]
    fields = ("AdGroupId", "Criteria", "DisplayName", "Ctr", "AverageCpm", "AverageCpv", "VideoViewRate",
              "ActiveViewMeasurableImpressions", "ActiveViewImpressions",
              "ActiveViewViewability") + MAIN_STATISTICS_FILEDS + COMPLETED_FIELDS
    report = placement_performance_report(client, predicates=predicates, fields=fields)
    all_rows = []
    for row in report:
        if "channel" not in row.DisplayName:
            continue
        formatted = {}
        # Create new dictionary of mapped keys to mapped API data values
        for report_field, mapped_key in ADWORDS_API_FIELD_MAPPING.items():
            coercer = ADWORDS_COERCE_FIELD_FUNCS.get(mapped_key)
            api_value = getattr(row, report_field, None)
            formatted[mapped_key] = coercer(api_value) if coercer is not None else api_value
        all_rows.append(formatted)
    aggregated = _aggregate_rows(all_rows)
    return aggregated


def _aggregate_rows(rows: list) -> list:
    """
    Add additional data to rows, combine rows with same placement ids, and calculate percentages
    Adwords Placement Performance Report returns multiple line items since placement performance is segmented by
    adgroup. To correctly calculate percentages, we must combine all statistics that are used to calculate percentages
    across all line items that have same placement ids
    :param rows: list
    :return: list-> Mapped data rows with AnalysisFields keys
    """
    by_channel_id = {}
    for row in rows:
        channel_id = row[AnalysisFields.CHANNEL_ID]
        row = _add_data(row)
        try:
            by_channel_id[channel_id] = _aggregate_row(by_channel_id[channel_id], row)
        except KeyError:
            by_channel_id[channel_id] = row
    aggregated = list(by_channel_id.values())
    # Calculate overall percentages with summed values
    for row in aggregated:
        row[AnalysisFields.CTR] = _safe_percent(row[AnalysisFields.CLICKS], row[AnalysisFields.IMPRESSIONS])
        row[AnalysisFields.VIDEO_VIEW_RATE] = _safe_percent(row[AnalysisFields.VIDEO_VIEWS], row[AnalysisFields.IMPRESSIONS])
        row[AnalysisFields.VIDEO_QUARTILE_100_RATE] = _safe_percent(row[AnalysisFields.VIDEO_VIEWS_100_PERCENT], row[AnalysisFields.IMPRESSIONS])
        row[AnalysisFields.ACTIVE_VIEW_VIEWABILITY] = _safe_percent(row[AnalysisFields.ACTIVE_VIEW_IMPRESSIONS], row[AnalysisFields.ACTIVE_VIEW_MEASURABLE_IMPRESSIONS])
    return aggregated


def _aggregate_row(row1, row2):
    """
    Combine line items for same placement ids
    :param row1: dict -> Mapped data rows with AnalysisFields keys
    :param row2: dict -> Mapped data rows with AnalysisFields keys
    :return:
    """
    sum_fields = (AnalysisFields.COST, AnalysisFields.CLICKS, AnalysisFields.IMPRESSIONS, AnalysisFields.VIDEO_VIEWS,
                  AnalysisFields.VIDEO_VIEWS_100_PERCENT, AnalysisFields.ACTIVE_VIEW_IMPRESSIONS,
                  AnalysisFields.ACTIVE_VIEW_MEASURABLE_IMPRESSIONS
                  )

    def _sum(val1, val2):
        try:
            val = val1 + val2
        except (TypeError, ValueError):
            val = val1 if val1 is not None else val2
        return val

    for field in sum_fields:
        row1[field] = _sum(row1[field], row2[field])
    return row1


def _add_data(row: dict):
    """
    Adds additional calculated data to row to be able to calculate percentages across multiple line items
    :param row: dict -> Mapped data rows with AnalysisFields keys
    :return: dict
    """
    # number of views that reached 100% of video
    row[AnalysisFields.VIDEO_VIEWS_100_PERCENT] = row[AnalysisFields.VIDEO_QUARTILE_100_RATE] * row[AnalysisFields.IMPRESSIONS] / 100
    return row


def _safe_percent(val1, val2):
    try:
        val = val1 / val2 * 100
    except (ZeroDivisionError, TypeError, ValueError):
        val = 0
    return val

