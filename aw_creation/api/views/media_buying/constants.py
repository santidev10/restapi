from aw_reporting.models import CriterionType
import ads_analyzer.reports.account_targeting_report.constants as names


REPORT_CONFIG = {
    "all": {
        "criteria": [CriterionType.AGE, CriterionType.GENDER, CriterionType.USER_INTEREST_LIST, CriterionType.KEYWORD,
                     CriterionType.PLACEMENT, CriterionType.VERTICAL],
        "aggregations": (names.COST_SHARE, names.IMPRESSIONS_SHARE, names.VIDEO_VIEWS_SHARE, names.VIDEO_VIEW_RATE,
                         names.CONTRACTED_RATE, names. AVERAGE_CPV, names.AVERAGE_CPM, names.PROFIT, names.MARGIN,
                         names.CTR_I, names.CTR_V),
        "summary": (names.SUM_IMPRESSIONS, names.SUM_VIDEO_VIEWS, names.AVERAGE_VIEW_RATE, names.AVERAGE_CONTRACTED_RATE,
                    names.AVERAGE_CPM, names.AVERAGE_CPV, names.SUM_COST, names.SUM_REVENUE, names.SUM_PROFIT,
                    names.SUM_MARGIN, names.SUM_CLICKS, names.AVERAGE_CTR_I, names.AVERAGE_CTR_V)
    },
}
