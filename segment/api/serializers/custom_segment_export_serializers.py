from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
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
            categories = self.context["brand_safety_categories"]
            ids = [int(_id) for _id in obj.task_us_data.brand_safety if _id is not None]
            categories = ", ".join([
                categories[_id] for _id in ids if _id in categories
            ])
        except (AttributeError, TypeError):
            categories = None
        return categories

    def get_age_group(self, obj):
        try:
            age_id = int(obj.task_us_data.age_group)
            age_group = self.context["age_groups"].get(age_id)
        except (AttributeError, TypeError, AuditAgeGroup.DoesNotExist):
            age_group = None
        return age_group

    def get_gender(self, obj):
        try:
            gender_id = int(obj.task_us_data.gender)
            gender = self.context["genders"].get(gender_id)
        except (AttributeError, TypeError, AuditGender.DoesNotExist):
            gender = None
        return gender

    def get_content_type(self, obj):
        try:
            content_id = int(obj.task_us_data.content_type)
            content_type = self.context["content_types"].get(content_id)
        except (AttributeError, TypeError, AuditContentType.DoesNotExist):
            content_type = None
        return content_type

    def get_vetting_result(self, obj):
        """
        context provided by base class
        :param obj:
        :return:
        """
        item_id = obj.main.id
        vetting_data = self.context.get("vetting", {}).get(item_id, {})
        skipped = vetting_data.get("skipped", None)
        suitability = vetting_data.get("clean", None)
        vetted_value = AuditUtils.get_vetting_value(skipped, suitability)
        return vetted_value

    def get_vetted(self, obj):
        vetted = "Y" \
            if getattr(obj.task_us_data, "created_at", None) is not None \
            else None
        return vetted


class CustomSegmentChannelExportSerializer(
    CustomSegmentExportSerializerMixin,
    Serializer
):
    columns = (
        "URL", "Title", "Language", "Category", "Subscribers", "Overall_Score",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type",
        "Num_Videos",
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
    Num_Videos = IntegerField(source="stats.total_videos_count")

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
        categories = [value for value in categories if value is not None]
        joined = ", ".join(categories)
        return joined


class CustomSegmentChannelWithMonetizationExportSerializer(
    CustomSegmentChannelExportSerializer
):
    columns = (
        "URL", "Title", "Language", "Category", "Subscribers", "Overall_Score",
        "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender",
        "Content_Type", "Num_Videos",
    )

    Monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    Num_Videos = IntegerField(source="stats.total_videos_count")

    def __init__(self, instance, *args, **kwargs):
        super().__init__(instance, *args, **kwargs)


class CustomSegmentVideoExportSerializer(
    CustomSegmentExportSerializerMixin,
    Serializer
):
    columns = (
        "URL", "Title", "Language", "Category", "Views", "Overall_Score",
        "Vetted", "Brand_Safety", "Age_Group", "Gender", "Content_Type",
    )

    URL = SerializerMethodField("get_url")
    Title = CharField(source="general_data.title", default="")
    Language = SerializerMethodField("get_language")
    Category = SerializerMethodField("get_category")
    Views = IntegerField(source="stats.views")
    Overall_Score = SerializerMethodField("get_overall_score")
    Vetted = SerializerMethodField("get_vetted")
    Brand_Safety = SerializerMethodField("get_brand_safety")
    Age_Group = SerializerMethodField("get_age_group")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_content_type")

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
