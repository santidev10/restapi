import enum

from .utils import Coercers
from performiq.models.constants import AnalysisFields


class AnalyzeSection:
    PERFORMANCE_RESULT_KEY = "performance_results"
    CONTEXTUAL_RESULT_KEY = "contextual_results"
    SUITABILITY_RESULT_KEY = "suitability_results"


ANALYZE_SECTIONS = [AnalyzeSection.PERFORMANCE_RESULT_KEY, AnalyzeSection.CONTEXTUAL_RESULT_KEY,
                    AnalyzeSection.SUITABILITY_RESULT_KEY]

# Mapping of campaign data fields to function to coerce values
COERCE_FIELD_FUNCS = {
    AnalysisFields.IMPRESSIONS: Coercers.integer,
    AnalysisFields.VIDEO_VIEWS: Coercers.integer,
    AnalysisFields.CTR: Coercers.percentage,
    AnalysisFields.CPM: Coercers.float,
    AnalysisFields.CPV: Coercers.float,
    AnalysisFields.COST: Coercers.float,
    AnalysisFields.ACTIVE_VIEW_VIEWABILITY: Coercers.percentage,
    AnalysisFields.VIDEO_VIEW_RATE: Coercers.percentage,
    AnalysisFields.VIDEO_QUARTILE_100_RATE: Coercers.percentage,
    AnalysisFields.CONTENT_TYPE: Coercers.integer,
    AnalysisFields.CONTENT_QUALITY: Coercers.integer,
    "channel_url": Coercers.channel_url,
    "cpm": Coercers.float,
    "cpv": Coercers.float,
}

ADWORDS_COERCE_FIELD_FUNCS = {
    key: func for key, func in COERCE_FIELD_FUNCS.items()
}
ADWORDS_COERCE_FIELD_FUNCS.update({
    "cpm": Coercers.cost_micros,
    "cpv": Coercers.cost_micros,
    AnalysisFields.CPM: Coercers.cost_micros,
    AnalysisFields.CPV: Coercers.cost_micros,
    AnalysisFields.COST: Coercers.cost_micros,
})


class DataSourceType(enum.IntEnum):
    GOOGLE_ADS = 0
    DV360 = 1
    CSV = 2


ES_FIELD_MAPPING = {
    "general_data.primary_category": AnalysisFields.CONTENT_CATEGORIES,
    "general_data.top_lang_code": AnalysisFields.LANGUAGES,
    "task_us_data.content_quality": AnalysisFields.CONTENT_QUALITY,
    "task_us_data.content_type": AnalysisFields.CONTENT_TYPE,
    "brand_safety.overall_score": AnalysisFields.OVERALL_SCORE,
}


class ESFieldMapping:
    PRIMARY = {
        "general_data.primary_category": AnalysisFields.CONTENT_CATEGORIES,
        "general_data.top_lang_code": AnalysisFields.LANGUAGES,
        "task_us_data.content_quality": AnalysisFields.CONTENT_QUALITY,
        "task_us_data.content_type": AnalysisFields.CONTENT_TYPE,
        "brand_safety.overall_score": AnalysisFields.OVERALL_SCORE,
    }
    SECONDARY = {
        "general_data.primary_category": "general_data.iab_categories"
    }
