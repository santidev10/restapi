from collections import namedtuple
from enum import Enum

import ads_analyzer.reports.account_targeting_report.constants as names
from aw_creation.models import CampaignCreation
from aw_reporting.models import CriteriaTypeEnum

ScalarFilter = namedtuple("ScalarFilter", "name operator type")

SHARED_AGGREGATIONS = (names.AVERAGE_CPV, names.AVERAGE_CPM, names.CONTRACTED_RATE, names.COST_SHARE, names.CTR_I,
                       names.CTR_V, names.IMPRESSIONS_SHARE, names.MARGIN, names.PROFIT, names.REVENUE,
                       names.VIDEO_VIEWS_SHARE, names.VIDEO_VIEW_RATE)

AGGREGATION_CONFIG = {
    "all": SHARED_AGGREGATIONS + (names.SUM_IMPRESSIONS, names.SUM_VIDEO_VIEWS, names.SUM_CLICKS, names.SUM_COST),
}

REPORT_CONFIG = {
    "all": {
        "type": "all",
        "aggregations": AGGREGATION_CONFIG["all"],
        "sorts": ("campaign_name", "ad_group_name", "target_name") + AGGREGATION_CONFIG["all"],
        # operator = "" is a basic equality operator e.g. ...filter(name=value)
        "scalar_filters": (ScalarFilter("targeting_status", "", "int"),),
        "range_filters": AGGREGATION_CONFIG["all"],
        "criteria": [
            f"{CriteriaTypeEnum.PLACEMENT.name}_CHANNEL", f"{CriteriaTypeEnum.PLACEMENT.name}_VIDEO",
            CriteriaTypeEnum.KEYWORD.name, CriteriaTypeEnum.VERTICAL.name,
            CriteriaTypeEnum.USER_INTEREST.name, CriteriaTypeEnum.USER_LIST.name,
        ]
    },
}


class CampaignBidStrategyTypeEnum(Enum):
    cpa = CampaignCreation.TARGET_CPA_STRATEGY
    cpv = CampaignCreation.MAX_CPV_STRATEGY
    cpm = CampaignCreation.MAX_CPM_STRATEGY
    target_cpm = CampaignCreation.TARGET_CPM_STRATEGY


AD_GROUP_TYPE_CAMPAIGN_BID_TYPE = {
    "Standard": "target_cpm",
    "Bumper": "target_cpm",
    "Display": "cpm",
    "Video discovery": "cpv",
    "In-stream": "cpv",
}
