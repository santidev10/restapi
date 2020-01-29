TARGETING_TYPES = (
    "interests", "keywords", "channels", "videos", "remarketing", "topics")
DEVICE_FIELDS = (
    "device_computers", "device_mobile", "device_tablets", "device_other")
PARENT_FIELDS = (
    "parent_parent", "parent_not_parent", "parent_undetermined")
GENDER_FIELDS = ("gender_undetermined", "gender_female", "gender_male")
AGE_FIELDS = (
    "age_undetermined", "age_18_24", "age_25_34", "age_35_44", "age_45_54",
    "age_55_64", "age_65")
VIDEO_LENGTHS = (
    (0, 6),
    (6, 15),
    (15, 30),
    (30, 60),
    (60, 120),
    (120, None),
)

PLACEMENTS_ADWORDS_CAMPAIGNS_PREFIX = "placements__adwords_campaigns__"

OPPORTUNITY_VALUES_LIST = ("sf_cpv_cost", "sf_cpm_cost", "sf_cpv_units", "sf_cpm_units",
                           "name", "brand", "category_id", "apex_deal", "id")


class ModelFiltersOptions:

    class Campaign:
        prefix = ""
        filtering_or = True
        filtering_and = True

    class Opportunity:
        prefix = PLACEMENTS_ADWORDS_CAMPAIGNS_PREFIX
        filtering_or = True
        filtering_and = True
