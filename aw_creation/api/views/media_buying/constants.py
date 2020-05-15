from enum import Enum

import ads_analyzer.reports.account_targeting_report.constants as names
from aw_creation.models import CampaignCreation
from aw_reporting.models import CriteriaTypeEnum


SHARED_AGGREGATIONS = (names.AVERAGE_CPV, names.AVERAGE_CPM, names.CONTRACTED_RATE, names.COST_SHARE, names.CTR_I,
                       names.CTR_V, names.IMPRESSIONS_SHARE, names.MARGIN, names.PROFIT, names.REVENUE,
                       names.VIDEO_VIEWS_SHARE, names.VIDEO_VIEW_RATE)

AGGREGATION_CONFIG = {
    "all": SHARED_AGGREGATIONS + (names.SUM_IMPRESSIONS, names.SUM_VIDEO_VIEWS, names.SUM_CLICKS, names.SUM_COST),
}

REPORT_CONFIG = {
    "all": {
        "aggregations": AGGREGATION_CONFIG["all"],
        "sorts": ("campaign_name", "ad_group_name", "target_name") + AGGREGATION_CONFIG["all"],
        "scalar_filters": (),
        "range_filters": AGGREGATION_CONFIG["all"],
        "criteria": [
            CriteriaTypeEnum.VIDEO_CREATIVE.name, CriteriaTypeEnum.DEVICE.name,
            f"{CriteriaTypeEnum.PLACEMENT.name}_CHANNEL", f"{CriteriaTypeEnum.PLACEMENT.name}_VIDEO",
            CriteriaTypeEnum.KEYWORD.name, CriteriaTypeEnum.VERTICAL.name, CriteriaTypeEnum.AGE_RANGE.name,
            CriteriaTypeEnum.GENDER.name, CriteriaTypeEnum.PARENT.name,
            CriteriaTypeEnum.USER_INTEREST.name, CriteriaTypeEnum.USER_LIST.name,
        ]
    },
}


class CampaignBidStrategyTypeEnum(Enum):
    cpa: CampaignCreation.TARGET_CPA_STRATEGY
    cpv: CampaignCreation.MAX_CPV_STRATEGY
    cpm: CampaignCreation.MAX_CPM_STRATEGY
