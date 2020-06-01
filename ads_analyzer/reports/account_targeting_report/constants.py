CAMPAIGN_ID = "campaign_id"
CAMPAIGN_NAME = "campaign_name"
AD_GROUP_NAME = "ad_group_name"
TARGET = "target"
AVERAGE_CPV = "average_cpv"
AVERAGE_CPM = "average_cpm"
AVERAGE_COST = "average_cost"
MARGIN = "margin"
COST = "cost"
COST_SHARE = "cost_share"
VIDEO_VIEWS_SHARE = "video_views_share"
IMPRESSIONS_SHARE = "impressions_share"
VIDEO_VIEW_RATE = "video_view_rate"
IMPRESSIONS = "impressions"
VIDEO_VIEWS = "video_views"
CONTRACTED_RATE = "contracted_rate"
REVENUE = "revenue"
PROFIT = "profit"
CLICKS = "clicks"
CTR_I = "ctr_i"
CTR_V = "ctr_v"
MAX_CPV = "max_cpv"
MAX_CPM = "max_cpm"
SUM_IMPRESSIONS = "sum_impressions"
SUM_VIDEO_VIEWS = "sum_video_views"
SUM_CLICKS = "sum_clicks"
SUM_COST = "sum_cost"
SUM_VIDEO_VIEWS_100_QUARTILE = "sum_video_views_100_quartile"
SUM_VIDEO_IMPRESSIONS = "sum_video_impressions"
VIDEO_IMPRESSIONS = "video_impressions"
AVERAGE_REVENUE = "average_revenue"
AVERAGE_VIEW_RATE = "average_view_rate"
SUM_REVENUE = "sum_revenue"
SUM_PROFIT = "sum_profit"
AVERAGE_CONTRACTED_RATE = "average_contracted_rate"
SUM_MARGIN = "sum_margin"
AVERAGE_CTR_I = "average_ctr_i"
AVERAGE_CTR_V = "average_ctr_v"


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

AGGREGATION_COLUMN_NAME_MAP = {
    SUM_CLICKS: CLICKS,
    SUM_IMPRESSIONS: IMPRESSIONS,
    SUM_VIDEO_VIEWS: VIDEO_VIEWS
}
KPI_FILTER_NAME_MAP = {
    AVERAGE_CPV: "Avg. CPV ($)",
    AVERAGE_CPM: "Avg. CPM ($)",
    MARGIN: "Margin (%)",
    COST_SHARE: "Cost Share (%)",
    COST: "Cost ($)",
    VIDEO_VIEWS_SHARE: "Views SOV (%)", # Percentage of video_views of total
    IMPRESSIONS_SHARE: "Impressions SOV (%)", # Percentage of impressions of total
    VIDEO_VIEW_RATE: "View Rate (%)",
    IMPRESSIONS: "Impressions",
    VIDEO_VIEWS: "Video Views",
    CONTRACTED_RATE: "Contracted Rate ($)",
    REVENUE: "Revenue ($)",
    PROFIT: "Profit ($)",
    CLICKS: "Clicks",
    CTR_I: "CTR(i) (%)",
    CTR_V: "CTR(v) (%)",
    SUM_IMPRESSIONS: "Impressions",
    SUM_VIDEO_VIEWS: "Video Views",
    SUM_COST: "Cost ($)",
    SUM_CLICKS: "Clicks"
}

STATISTICS_ANNOTATIONS = (CONTRACTED_RATE, SUM_IMPRESSIONS, SUM_VIDEO_VIEWS, SUM_CLICKS, SUM_COST,
                          SUM_VIDEO_VIEWS_100_QUARTILE, SUM_VIDEO_IMPRESSIONS, REVENUE)


TOTAL_SUMMARY_COLUMN_AGG_MAPPING = {
    "impressions__sum": SUM_IMPRESSIONS,
    "video_views__sum": SUM_VIDEO_VIEWS,
    "clicks__sum": SUM_CLICKS,

    "sum_impressions__sum": SUM_IMPRESSIONS,
    "sum_video_views__sum": SUM_VIDEO_VIEWS,
    "sum_clicks__sum": SUM_CLICKS,
    "sum_cost__sum": SUM_COST,

    "average_cpm__avg": AVERAGE_CPM,
    "average_cpv__avg": AVERAGE_CPV,
    "revenue__sum": REVENUE,
    "profit__sum": PROFIT,
    "margin__sum": MARGIN,
    "ctr_i__avg": CTR_I,
    "ctr_v__avg": CTR_V,

    "impressions_share__avg": IMPRESSIONS_SHARE,
    "video_views_share__avg": VIDEO_VIEWS_SHARE,
    "video_view_rate__avg": VIDEO_VIEW_RATE,
    "contracted_rate__avg": CONTRACTED_RATE,
}

EXPORT_FIELDS = (
    "campaign_name",
    "campaign_id",
    "ad_group_name",
    "ad_group_id",
    "target_name",
    "type_name",
    "rate_type",
    "contracted_rate",
    "sum_impressions",
    "sum_video_views",
    "sum_clicks",
    "sum_cost",
    "revenue",
    "average_cpm",
    "average_cpv",
    "cost_share",
    "ctr_i",
    "ctr_v",
    "impressions_share",
    "margin",
    "profit",
    "video_views_share",
    "video_view_rate",
)