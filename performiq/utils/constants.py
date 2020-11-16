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
    IMPRESSIONS = "impressions"
    VIEWS = "views"
    COST = "cost"
    AVERAGE_CPV = "average CPV"
    AVERAGE_CPM = "average CPM"
    VIDEO_PLAYED_VIEW_RATE = "video played view rate"
