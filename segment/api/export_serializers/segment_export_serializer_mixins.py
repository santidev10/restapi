from datetime import datetime
import pytz

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditContentType
from audit_tool.models import AuditGender
from audit_tool.models import AuditContentQuality
from audit_tool.utils.audit_utils import AuditUtils
from brand_safety.languages import LANGUAGES
from es_components.iab_categories import HIDDEN_IAB_CATEGORIES
from utils.brand_safety import map_brand_safety_score
from es_components.constants import LAST_VETTED_AT_MIN_DATE


class SegmentExportSerializerMixin:
    """ Context provided by context kwarg in serializer instantiation """
    def get_category(self, obj):
        categories = getattr(obj.general_data, "iab_categories", [])
        joined = ", ".join([category for category in categories
                           if isinstance(category, str) and category not in HIDDEN_IAB_CATEGORIES])
        return joined

    def get_brand_safety(self, obj):
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

    def get_content_quality(self, obj):
        try:
            quality_id = int(obj.task_us_data.content_quality)
            quality_type = self.context["quality_types"].get(quality_id)
        except (AttributeError, TypeError, AuditContentQuality.DoesNotExist):
            quality_type = None
        return quality_type

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
        date_last_vetted_at = getattr(obj.task_us_data, "last_vetted_at", None)
        if not date_last_vetted_at or date_last_vetted_at < \
                pytz.utc.localize(datetime.strptime(LAST_VETTED_AT_MIN_DATE, "%Y-%m-%d")):
            return "N"
        return "Y"

    def get_overall_score(self, obj):
        score = map_brand_safety_score(obj.brand_safety.overall_score)
        return score

    def get_mismatched_language(self, obj):
        mismatched_language = getattr(obj.task_us_data, "mismatched_language", None)
        if mismatched_language:
            mismatched_language = "Y"
        elif mismatched_language is False:
            mismatched_language = "N"
        else:
            mismatched_language = None
        return mismatched_language

    def get_country(self, obj):
        """ actually returns a country code... """
        return getattr(obj.general_data, "country_code", "")

    def get_vetted_by(self, obj):
        """ Get user email that vetted item """
        item_id = obj.main.id
        vetting_data = self.context.get("vetting", {}).get(item_id, {})
        user_email = vetting_data.get("user_email", None)
        return user_email

    def get_sentiment(self, obj):
        sentiment = getattr(obj.stats, "sentiment", None)
        return int(sentiment) if sentiment else None


class SegmentVideoExportSerializerMixin(SegmentExportSerializerMixin):

    def get_language(self, obj):
        try:
            lang_code = getattr(obj.general_data, "lang_code", "")
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            lang_code = None
        language = LANGUAGES.get(lang_code, lang_code)
        return language

    def get_url(self, obj):
        return f"https://www.youtube.com/watch?v={obj.main.id}"


class SegmentChannelExportSerializerMixin(SegmentExportSerializerMixin):

    def get_language(self, obj):
        try:
            lang_code = getattr(obj.general_data, "top_lang_code", "")
        # pylint: disable=broad-except
        except Exception:
            # pylint: enable=broad-except
            lang_code = None
        language = LANGUAGES.get(lang_code, lang_code)
        return language

    def get_url(self, obj):
        return f"https://www.youtube.com/channel/{obj.main.id}"
