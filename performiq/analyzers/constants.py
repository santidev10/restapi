from .utils import Coercers
from performiq.models.constants import CampaignDataFields


class AnalyzeSection:
    PERFORMANCE_RESULT_KEY = "performance"
    CONTEXTUAL_RESULT_KEY = "contextual"
    SUITABILITY_RESULT_KEY = "suitability"


ANALYZE_SECTIONS = {AnalyzeSection.PERFORMANCE_RESULT_KEY, AnalyzeSection.CONTEXTUAL_RESULT_KEY,
                    AnalyzeSection.SUITABILITY_RESULT_KEY}

# Mapping of campaign data fields to function to coerce values for comparisons
COERCE_FIELD_FUNCS = {
    CampaignDataFields.IMPRESSIONS: Coercers.integer,
    CampaignDataFields.VIDEO_VIEWS: Coercers.integer,
    CampaignDataFields.CTR: Coercers.percentage,
    CampaignDataFields.CPM: Coercers.cost,
    CampaignDataFields.CPV: Coercers.cost,
    CampaignDataFields.ACTIVE_VIEW_VIEWABILITY: Coercers.percentage,
}