BASE_SERIALIZER_FIELDS = (
    "name",
    "type",
    "campaign_name",
    "ad_group_name",
    "placement_name",
    "max_bid",
    "rate_type",
    "contracted_rate",
    "impressions",
    "video_views",
    "clicks",
    "cost",
    "ctr",
    "view_rate",
    "avg_rate",
    "revenue",
    "profit",
    "margin",
)

KPI_FILTERS = (
    "average_cpv",
    "average_cpm",
    "margin",
    "cost",
    "video_views_share", # Percentage of video_views of total
    "impressions_share", # Percentage of impressions of total
    "view_rate",
)

CAMPAIGN_ID = "campaign_id"
CAMPAIGN_NAME = "campaign_name"
AD_GROUP_NAME = "ad_group_name"
TARGET = "target"
AVG_CPV = "margin"
AVG_CPM = "avg_cpm"
MARGIN = "margin"
COST = "cost"
VIEWS_DELIVERY = "views_delivery"
IMPRESSIONS_DELIVERY = "impressions_delivery"
VIEW_RATE = "view_rate"

STATS_REPORT = "stats"
KPI_FILTERS_REPORT = "kpi_filters"
SUMMARY_REPORT = "summary"


class ReportType:
    ALL = {STATS_REPORT, KPI_FILTERS_REPORT, SUMMARY_REPORT}
    STATS = STATS_REPORT
    KPI_FILTERS = KPI_FILTERS_REPORT
    SUMMARY = SUMMARY_REPORT
