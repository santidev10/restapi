from rest_framework.serializers import SerializerMethodField

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditGender
from audit_tool.models import AuditContentType
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentChannelWithMonetizationExportSerializer
from segment.api.serializers.custom_segment_export_serializers import CustomSegmentVideoExportSerializer


class CustomSegmentVettedExportSerializerMixin(object):
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
        extra_data provided by inheritance
        :param obj:
        :return:
        """
        item_id = obj.main.id
        vetting_data = self.extra_data.get(item_id, {})
        skipped = vetting_data.get("skipped", None)
        suitability = vetting_data.get("clean", None)
        vetted_value = AuditUtils.get_vetting_value(skipped, suitability)
        return vetted_value


class CustomSegmentChannelVettedExportSerializer(CustomSegmentVettedExportSerializerMixin, CustomSegmentChannelWithMonetizationExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Subscribers", "Overall_Score", "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender", "Content_Type")

    Brand_Safety = SerializerMethodField("get_brand_safety")
    Language = SerializerMethodField("get_language")
    Age_Group = SerializerMethodField("get_content_type")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_age_group")


class CustomSegmentVideoVettedExportSerializer(CustomSegmentVettedExportSerializerMixin, CustomSegmentVideoExportSerializer):
    columns = ("URL", "Title", "Language", "Category", "Views", "Overall_Score", "Vetted", "Monetizable", "Brand_Safety", "Age_Group", "Gender", "Content_Type")

    Brand_Safety = SerializerMethodField("get_brand_safety")
    Language = SerializerMethodField("get_language")
    Age_Group = SerializerMethodField("get_content_type")
    Gender = SerializerMethodField("get_gender")
    Content_Type = SerializerMethodField("get_age_group")

