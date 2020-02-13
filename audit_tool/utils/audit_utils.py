from audit_tool.models import AuditChannel
from audit_tool.models import AuditVideo
from audit_tool.models import AuditChannelMeta
from audit_tool.models import AuditVideoMeta
from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditChannelType
from audit_tool.models import AuditGender
from brand_safety.languages import LANG_CODES
from brand_safety.models import BadWordCategory
from cache.models import CacheItem
from cache.constants import CHANNEL_AGGREGATIONS_KEY
from es_components.iab_categories import IAB_TIER2_CATEGORIES_MAPPING


class AuditUtils(object):
    video_config = {
        "audit_model": AuditVideo,
        "meta_model": AuditVideoMeta,
        "vetting_model": None, # Tech debt 4.8
    }
    channel_config = {
        "audit_model": AuditChannel,
        "meta_model": AuditChannelMeta,
        "vetting_model": AuditChannelVet,
    }

    def __init__(self, audit_type=0):
        self._audit_type = audit_type
        self._config = {
            1: self.video_config,
            2: self.channel_config,
        }

    @property
    def audit_model(self):
        return self._config[self._audit_type]["audit_model"]

    @property
    def meta_model(self):
        return self._config[self._audit_type]["meta_model"]

    @property
    def vetting_model(self):
        return self._config[self._audit_type]["vetting_model"]

    @staticmethod
    def get_brand_safety_categories():
        all_categories = [{
            "id": category.id,
            "value": category.name
        } for category in BadWordCategory.objects.all()]
        return all_categories

    @staticmethod
    def get_channel_types():
        data = [{
            "id": item.id,
            "value": item.channel_type
        } for item in AuditChannelType.objects.all()]
        return data

    @staticmethod
    def get_iab_categories():
        """
        Map AuditCategories to parent child structure
        id and value fields will be identical since we are not storing in database
        :return: list
        """
        iab_categories = []
        for tier_1, tier_2_values in IAB_TIER2_CATEGORIES_MAPPING.items():
            category = {
                "id": tier_1,
                "value": tier_1,
                "children": [{
                    "value": child
                } for child in tier_2_values]
            }
            iab_categories.append(category)
        return iab_categories

    @staticmethod
    def get_genders():
        """
        Get all AuditGender values
        :return: list
        """
        genders = [{
            "id": gender.id,
            "value": gender.gender
        } for gender in AuditGender.objects.all()]
        return genders

    @staticmethod
    def get_languages():
        try:
            agg_cache = CacheItem.objects.get(key=CHANNEL_AGGREGATIONS_KEY)
            lang_str = [item["key"] for item in agg_cache.value['general_data.top_language']['buckets']]
            languages = []
            for lang in lang_str:
                try:
                    code = LANG_CODES[lang]
                except KeyError:
                    code = lang
                languages.append({"id": code, "value": lang})
        except (CacheItem.DoesNotExist, KeyError):
            languages = [
                {"id": code, "title": lang}
                for lang, code in LANG_CODES.items()
            ]
        return languages

    @staticmethod
    def get_age_groups():
        """
        Get all AuditAgeGroup values
        Returns subgroups as children keys for parent age groups
        :return: list
        """
        age_groups = AuditAgeGroup.get_by_group()
        return age_groups