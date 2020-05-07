from aw_reporting.models import CriterionType
import ads_analyzer.reports.account_targeting_report.constants as names


SHARED_AGGREGATIONS = (names.AVERAGE_CPV, names.AVERAGE_CPM, names.CONTRACTED_RATE, names.COST_SHARE, names.CTR_I,
                       names.CTR_V, names.IMPRESSIONS_SHARE, names.MARGIN, names.PROFIT, names.REVENUE,
                       names.VIDEO_VIEWS_SHARE, names.VIDEO_VIEW_RATE)

AGGREGATION_CONFIG = {
    "all": SHARED_AGGREGATIONS + (names.SUM_IMPRESSIONS, names.SUM_VIDEO_VIEWS, names.SUM_CLICKS, names.SUM_COST),
}

REPORT_CONFIG = {
    "all": {
        "criteria": [CriterionType.AGE, CriterionType.GENDER, CriterionType.USER_INTEREST_LIST, CriterionType.KEYWORD,
                     CriterionType.PLACEMENT, CriterionType.VERTICAL],
        "aggregations": AGGREGATION_CONFIG["all"],
        "sorts": ("campaign_name", "ad_group_name", "target_name") + AGGREGATION_CONFIG["all"],
    },
}
