import enum

from .utils import Coercers
from performiq.models.constants import AnalysisFields


class AnalyzeSection:
    PERFORMANCE_RESULT_KEY = "performance_results"
    CONTEXTUAL_RESULT_KEY = "contextual_results"
    SUITABILITY_RESULT_KEY = "suitability_results"


ANALYZE_SECTIONS = {AnalyzeSection.PERFORMANCE_RESULT_KEY, AnalyzeSection.CONTEXTUAL_RESULT_KEY,
                    AnalyzeSection.SUITABILITY_RESULT_KEY}

# Mapping of campaign data fields to function to coerce values for comparisons
COERCE_FIELD_FUNCS = {
    AnalysisFields.IMPRESSIONS: Coercers.integer,
    AnalysisFields.VIDEO_VIEWS: Coercers.integer,
    AnalysisFields.CTR: Coercers.percentage,
    AnalysisFields.CPM: Coercers.cost,
    AnalysisFields.CPV: Coercers.cost,
    AnalysisFields.COST: Coercers.cost,
    AnalysisFields.ACTIVE_VIEW_VIEWABILITY: Coercers.percentage,
    AnalysisFields.VIDEO_VIEW_RATE: Coercers.percentage,
    "channel_url": Coercers.channel_url
}


class DataSourceType(enum.IntEnum):
    GOOGLE_ADS = 0
    DV360 = 1
    CSV = 2
