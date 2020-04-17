from collections import namedtuple

from django.db.models import F

from aw_reporting.models.ad_words.constants import GenderOptions


AttributeConfig = namedtuple("AttributeConfig", "name target_value")
ExtractStatsConfig = namedtuple("ExtractStatsConfig", "related_model select_related_key annotate_key")


class ExtractAdGroupTargeting(object):
    AD_GROUP_TARGETING_EXISTS_KEY = ("")
    EXTRACT_FROM_STATS_CONFIG = {
        "audiences": ExtractStatsConfig("audiences", "audience", "audience__name"),
        "keywords": ExtractStatsConfig("keywords", "keyword", "keyword"),
        "remarketing": ExtractStatsConfig("remark_statistic", "remark", "remark__name"),
        "topics": ExtractStatsConfig("topics", "topic", "topic__name"),
    }
    EXTRACT_FROM_ATTRIBUTE_CONFIG = {
        "gender_undetermined": AttributeConfig("genders", GenderOptions[0].upper()),
        "gender_female": AttributeConfig("genders", GenderOptions[1].upper()),
        "gender_male": AttributeConfig("genders", GenderOptions[2].upper()),

        "parent_parent": AttributeConfig("parents", "PARENT_PARENT"),
        "parent_not_parent": AttributeConfig("parents", "PARENT_NOT_A_PARENT"),
        "parent_undetermined": AttributeConfig("parents", "PARENT_UNDETERMINED"),

        "age_undetermined": AttributeConfig("ages", "AGE_RANGE_UNDETERMINED"),
        "age_18_24": AttributeConfig("ages", "AGE_RANGE_18_24"),
        "age_25_34": AttributeConfig("ages", "AGE_RANGE_25_34"),
        "age_35_44": AttributeConfig("ages", "AGE_RANGE_35_44"),
        "age_45_54": AttributeConfig("ages", "AGE_RANGE_45_54"),
        "age_55_64": AttributeConfig("ages", "AGE_RANGE_55_64"),
        "age_65": AttributeConfig("ages", "AGE_RANGE_65_UP"),
    }

    def __init__(self, ad_group):
        self.ad_group = ad_group
        self._targeting = {
            "audiences": [],
            "keywords": [],
            "remarketing": [],
            "topics": [],
            "genders": [],
            "parents": [],
            "ages": [],
        }

    def extract(self):
        for targeting_name, config in self.EXTRACT_FROM_STATS_CONFIG.items():
            targets = set(
                getattr(self.ad_group, config.related_model)
                .select_related(config.select_related_key)
                .annotate(target=F(config.annotate_key))
                .values_list("target", flat=True).distinct()
            )
            self._targeting[targeting_name].extend(targets)
        
        for attribute, config in self.EXTRACT_FROM_ATTRIBUTE_CONFIG.items():
            if getattr(self.ad_group, attribute) is True:
                self._targeting[config.name].append(config.target_value)
        return self._targeting