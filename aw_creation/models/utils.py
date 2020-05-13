from aw_creation.models import CampaignCreation

# Mapping of aw_reporting Campaign bidding_strategy_type to aw_creation CampainCreation bid_strategy_type
BID_STRATEGY_TYPE_MAPPING = {
    "cpv": CampaignCreation.MAX_CPV_STRATEGY,
    "cpm": CampaignCreation.MAX_CPM_STRATEGY,
    "cpa": CampaignCreation.TARGET_CPA_STRATEGY,
}