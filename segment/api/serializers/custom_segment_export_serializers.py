from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField
from utils.brand_safety import map_brand_safety_score

"""
CustomSegment export serializers

Each columns tuple for all serializers are used as headers for export files
"""
class CustomSegmentExportSerializerMixin():
    def get_language(self, obj):
        try:
            language = LANGUAGES.get(obj.task_us_data.lang_code)
        except AttributeError:
            language = None
        return language

    def get_category(self, obj):
        categories = getattr(obj.task_us_data, "iab_categories", []) or []
        joined = ", ".join(categories)
        return joined

    def get_brand_safety(self,  obj):
        try:
            ids = obj.task_us_data.brand_safety
            data = BadWordCategory.objects.filter(id__in=ids)
            categories = ", ".join([item.name for item in data])
        except AttributeError:
            categories = None
        return categories

    def get_age_group(self, obj):
        try:
            age_group = AuditAgeGroup.objects.get(id=obj.task_us_data.age_group).age_group
        except (AttributeError, AuditAgeGroup.DoesNotExist):
            age_group = None
        return age_group

    def get_gender(self, obj):
        try:
            gender = AuditGender.objects.get(id=obj.task_us_data.gender).gender
        except (AttributeError, AuditGender.DoesNotExist):
            gender = None
        return gender

    def get_content_type(self, obj):
        try:
            content_type = AuditContentType.objects.get(id=obj.task_us_data.content_type).content_type
        except (AttributeError, AuditContentType.DoesNotExist):
            content_type = None
        return content_type

    def get_vetted(self, obj):
        """
        extra_data provided by base class
        :param obj:
        :return:
        """
        item_id = obj.main.id
        vetting_data = self.extra_data.get(item_id, {})
        skipped = vetting_data.get("skipped", None)
        suitability = vetting_data.get("clean", None)
        vetted_value = AuditUtils.get_vetting_value(skipped, suitability)
        return vetted_value


class CustomSegmentChannelExportSerializer(CustomSegmentExportSerializerMixin, Serializer):
    columns = (
        "URL", "Title", "Language", "Category", "Subscribers", "Overall_Score", "Vetted",
        "Brand_Safety", "Age_Group", "Gender", "Content_Type"
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Subscribers = IntegerField(source="stats.subscribers")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")

    def __init__(self, instance, *args, **kwargs):
        self.extra_data = kwargs.pop("extra_data", {})
        super().__init__(instance, *args, **kwargs)

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}"

    def get_language(self, obj):
        try:
            lang_code = getattr(obj.task_us_data, "lang_code", "")
        except Exception:
            lang_code = None
        if not lang_code:
            lang_code = getattr(obj.general_data, "top_lang_code", "")
        language = LANGUAGES.get(lang_code, lang_code)
        return language

    def get_overall_score(self, obj):
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score

    def get_category(self, obj):
        categories = getattr(obj.task_us_data, "iab_categories", [])
        if not categories:
            categories = getattr(obj.general_data, "iab_categories", [])
        joined = ", ".join(categories)
        return joined

    def get_vetted(self, obj):
        vetted = "Y" if getattr(obj.task_us_data, "created_at", None) is not None else None
        return vetted


class CustomSegmentChannelWithMonetizationExportSerializer(CustomSegmentChannelExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score", "Vetted", "Monetizable")

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)

    def __init__(self, instance, *args, **kwargs):
        super().__init__(instance, *args, **kwargs)


class CustomSegmentVideoExportSerializer(CustomSegmentExportSerializerMixin, Serializer):
    columns = (
        "URL", "Title", "Language", "Category", "Views", "Overall_Score", "Vetted",
        "Brand_Safety", "Age_Group", "Gender", "Content_Type"
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Views = IntegerField(source="stats.views")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")
    Brand_safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")

    def __init__(self, instance, *args, **kwargs):
        self.extra_data = kwargs.pop("extra_data", {})
        super().__init__(instance, *args, **kwargs)

    def get_url(self, obj):
        return f"https://www.youtube.com/watch?v={obj.main.id}"

    def get_language(self, obj):
        try:
            lang_code = getattr(obj.task_us_data, "lang_code", "")
        except Exception:
            lang_code = None
        if not lang_code:
            lang_code = getattr(obj.general_data, "lang_code", "")
        language = LANGUAGES.get(lang_code, lang_code)
        return language

    def get_overall_score(self, obj):
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score

    def get_category(self, obj):
        categories = getattr(obj.task_us_data, "iab_categories", [])
        if not categories:
            categories = getattr(obj.general_data, "iab_categories", [])
        joined = ", ".join(categories)
        return joined
