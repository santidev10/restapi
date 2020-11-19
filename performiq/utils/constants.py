import enum


CAMPAIGN_REPORT_FIELDS = (
    "CampaignId", "ServingStatus"
)

CAMPAIGN_FIELDS_MAPPING = dict(
    id="CampaignId",
    impressions="Impressions",
    video_views="VideoViews",
    cost="Cost",
    name="CampaignName",
    active_view_viewability="ActiveViewViewability",
    video_quartile_100_rate="VideoQuartile100Rate",
)


class CSVFieldTypeEnum(enum.Enum):
    URL = "URL"
    IMPRESSIONS = "Impressions"
    VIEWS = "Views"
    COST = "Cost"
    AVERAGE_CPV = "Avg CPV"
    AVERAGE_CPM = "Avg CPM"
    VIEW_RATE = "View Rate"
    VIDEO_PLAYED_TO_100_RATE = "Video Played to 100%"
    CTR = "CTR"
