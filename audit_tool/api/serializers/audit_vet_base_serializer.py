from django.contrib.auth import get_user_model
from django.db.models import QuerySet
from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import ListField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from audit_tool.validators import AuditToolValidator
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory
from userprofile.constants import StaticPermissions


class AuditVetBaseSerializer(Serializer):
    """
    Base serializer for vetting models
    """
    # None values defined on child classes
    REVIEW_SCORE_THRESHOLD = 79
    data_type = None
    document_model = None
    general_data_lang_code_field = None
    es_manager = None

    # Elasticsearch fields for serialization
    age_group = IntegerField(source="task_us_data.age_group", default=None)
    brand_safety_overall_score = IntegerField(source="brand_safety.overall_score", default=None)
    content_quality = IntegerField(source="task_us_data.content_quality", default=None)
    content_type = IntegerField(source="task_us_data.content_type", default=None)
    gender = IntegerField(source="task_us_data.gender", default=None)
    iab_categories = ListField(source="task_us_data.iab_categories", default=[])
    is_monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    mismatched_language = BooleanField(source="task_us_data.mismatched_language", default=None)
    primary_category = CharField(source="general_data.primary_category")
    title = CharField(source="general_data.title", default=None)
    YT_id = CharField(source="main.id", default=None)
    brand_safety = SerializerMethodField()
    language = SerializerMethodField()

    # Postgres fields to save during deserialization
    checked_out_at = DateTimeField(required=False, allow_null=True)
    suitable = BooleanField(required=False)
    processed = DateTimeField(required=False)
    processed_by_user_id = IntegerField(required=False)
    language_code = CharField(required=False)  # Field for saving vetting item

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_brand_safety_category_ids = BadWordCategory.objects.values_list("id", flat=True)

    def get_iab_categories(self, obj):
        """ Remove None values """
        iab_categories = [val for val in obj.task_us_data.get("iab_categories", []) if val is not None]
        return iab_categories

    def validate(self, data):
        """
        Validate SerializerMethodFields
        :param data: dict
        :return: dict
        """
        data["task_us_data"].update({
            "brand_safety": self.validate_brand_safety(self.initial_data.get("brand_safety", [])),
            "language": self.validate_language_code(self.initial_data.get("lang_code", ""))
        })
        return data

    def update_brand_safety(self, *args, **kwargs):
        """ Initiate brand safety update task """
        raise NotImplementedError

    def get_url(self, *args, **kwargs):
        raise NotImplementedError

    def get_vetting_history(self, *args, **kwargs):
        raise NotImplementedError

    def _get_vetters_map(self, vetting_items: QuerySet) -> dict:
        """
        Given a vetting items QuerySet, get a user.id: formatted_user map of users for that queryset
        NOTE: The mapped value is expected to be the formatted string reprsenting the user
        :param vetting_items:
        :return: dict
        """
        vetters = get_user_model().objects.filter(id__in=[item.processed_by_user_id for item in vetting_items])
        vetters_map = {user.id: str(user) for user in vetters}
        return vetters_map

    def save(self, *args, **kwargs):
        raise NotImplementedError

    def _save_vetting_item(self, *args, **kwargs):
        raise NotImplementedError

    def get_brand_safety(self, doc):
        """
        Convert document brand_safety list strings to integers for Postgres pk queries
        :param doc: es_components.model
        :return: list -> [int, int, ...]
        """
        try:
            brand_safety = [int(item) for item in doc.task_us_data.brand_safety if item is not None]
        except AttributeError:
            brand_safety = []
        return brand_safety

    def get_language(self, doc):
        """
        Elasticsearch document language
        If item is has no task_us_data language (not vetted before), serialize general_data.language
        Else if has been vetted, use vetting task_us_data section language
        :param doc: es_components.model
        :return: str
        """
        if getattr(doc.task_us_data, "lang_code", None):
            language = doc.task_us_data.lang_code
        else:
            language = getattr(doc.general_data, self.general_data_lang_code_field, None)
        return language

    def get_segment_title(self, *_, **__):
        """
        Get segment title if available
        :return: None | str
        """
        title = getattr(self.context.get("segment", {}), "title", None)
        return title

    def validate_primary_category(self, value: str) -> str:
        """ Validate that primary category is tier 1 IAB category """
        primary_category = AuditToolValidator.validate_primary_category(value)
        return primary_category

    def validate_language_code(self, value: str) -> str:
        """
        Retrieve language from LANG_CODES. Raises ValidationError if not found
        :param value: str
        :return: AuditLanguage
        """
        try:
            LANGUAGES[value]
        except KeyError:
            raise ValidationError(f"Invalid language: {value}")
        return value

    def validate_iab_categories(self, values: list) -> list:
        """
        Retrieve AuditCategory iab_category values. Raises ValidationError if not found
        :param values: list
        :return: AuditCategory
        """
        values = list(set(values))
        if not values:
            raise ValidationError("Content categories must not be empty.")
        iab_categories = AuditToolValidator.validate_iab_categories(values)
        return iab_categories

    def validate_gender(self, value: str) -> str:
        """
        Retrieve AuditGender value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        gender = AuditToolValidator.validate_gender(int(value))
        gender_id = str(gender.id)
        return gender_id

    def validate_age_group(self, value: str) -> str:
        """
        Retrieve AuditAgeGroup value. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        age_group = AuditToolValidator.validate_age_group(int(value))
        age_group_id = str(age_group.id)
        return age_group_id

    def validate_content_type(self, value: str) -> str:
        """
        Retrieve AuditContentType value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        content_type = AuditToolValidator.validate_content_type(int(value))
        content_type_id = str(content_type.id)
        return content_type_id

    def validate_content_quality(self, value: str) -> str:
        """
        Retrieve AuditContentQuality value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        content_quality = AuditToolValidator.validate_content_quality(int(value))
        content_quality_id = str(content_quality.id)
        return content_quality_id

    def validate_category(self, value: str) -> str:
        """
        Retrieve audit_tool AuditCategory. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        category = AuditToolValidator.validate_category(value)
        category_id = str(category.id)
        return category_id

    def validate_brand_safety(self, values: list) -> list:
        categories = []
        if values:
            if type(values[0]) is str:
                key = "name"
            else:
                key = "id"
            mapping = {
                getattr(item, key): item
                for item in BadWordCategory.objects.all()
            }
            try:
                # Clean removed or non vettable categories from being saved
                categories = [mapping[val].id for val in set(values)
                              if val in mapping and mapping[val].vettable is True]
            except KeyError as e:
                raise ValidationError(f"Brand safety category not found: {e}")
        return categories

    def _get_vetted_brand_safety(self, previous_brand_safety: list) -> tuple:
        """
        Get task_us_data.brand_safety value based on vetted brand safety
            Will rescore the document if there is a change in brand safety
        :param previous_brand_safety: list of brand safety categories before current vetting
        :return: list -> Brand safety category ids
        """
        should_rescore = False
        new_vetted_brand_safety = set(
            str(s) for s in
            self.validated_data["task_us_data"].get("brand_safety", [])
        )
        # Rescore if any blacklist categories changed
        if not previous_brand_safety or new_vetted_brand_safety != set([str(s) for s in previous_brand_safety]):
            should_rescore = True
        return list(new_vetted_brand_safety), should_rescore

    def save_elasticsearch(self, item_id: str):
        """
        Save vetting data to Elasticsearch
        :param item_id: str -> video id, channel id
        :return: None
        """
        try:
            doc = self.es_manager.get([item_id])[0]
            bs_data = doc.brand_safety
            item_overall_score = bs_data.overall_score
            pre_limbo_score = bs_data.pre_limbo_score
            previous_blacklist_categories = doc.task_us_data.brand_safety
        except (IndexError, AttributeError):
            previous_blacklist_categories = []
            item_overall_score = None
            pre_limbo_score = None

        vetted_brand_safety_categories, should_rescore = self._get_vetted_brand_safety(previous_blacklist_categories)
        task_us_data = self._get_task_us_data()
        general_data = self._get_general_data(task_us_data)
        # Elasticsearch DSL does not serialize and will not save empty values: [], {}, None
        # Must use [None] as a sentinel value as elasticsearch_dsl will not serialize empty lists during update
        # https://github.com/elastic/elasticsearch-dsl-py/issues/758
        # https://github.com/elastic/elasticsearch-dsl-py/issues/460
        task_us_data["brand_safety"] = vetted_brand_safety_categories if vetted_brand_safety_categories else [None]
        brand_safety_limbo = self._get_brand_safety_limbo(task_us_data, item_overall_score, pre_limbo_score)

        # Update Elasticsearch document with vetted data
        doc = self.document_model(item_id)
        doc.populate_monetization(**self.validated_data["monetization"])
        doc.populate_task_us_data(**task_us_data)
        doc.populate_brand_safety(
            rescore=should_rescore,
            **brand_safety_limbo
        )
        doc.populate_general_data(**general_data)
        self.es_manager.upsert([doc], refresh=False)
        return doc

    def _get_general_data(self, task_us_data):
        """
        Get updated general data based on vetted task us data
        :param task_us_data: dict
        :return:
        """
        general_data = {}
        lang_code = task_us_data.get("lang_code")
        if lang_code and LANGUAGES.get(lang_code):
            general_data[self.general_data_lang_code_field] = lang_code

        general_data["iab_categories"] = task_us_data.get("iab_categories", [])
        general_data["primary_category"] = self.validated_data["general_data"].get("primary_category")
        return general_data

    def _get_brand_safety_limbo(self, task_us_data, overall_score, pre_limbo_score):
        """
        Determine if the vetting item should be reviewed.
        If task_us_data contains brand_safety data, then vetter has determined not safe by saving brand_safety
        categories.

        If the vetter is a vetting admin, accept vetting result as final
        :param task_us_data: dict -> Submitted vetted data
        :param overall_score: int -> Current brand_safety.overall_score of current vetting item
        :param pre_limbo_score: int -> brand_safety.pre_limbo_score value
        :return:
        """
        limbo_data = {}
        try:
            # If vetting admin, accept vetting result as final
            if self.context["user"].has_permission(StaticPermissions.CTL__VET_ADMIN):
                limbo_data["limbo_status"] = False
                return limbo_data
        except (KeyError, AttributeError):
            pass
        # brand safety may be saved as [None]
        safe = all(item is None for item in task_us_data.get("brand_safety"))

        # If vetting agrees with pre_limbo_score, limbo_status is resolved
        if pre_limbo_score is not None:
            if (safe and pre_limbo_score > self.REVIEW_SCORE_THRESHOLD) or (not safe and pre_limbo_score < self.REVIEW_SCORE_THRESHOLD):
                limbo_data["limbo_status"] = False
        # System scored as not safe but vet marks as safe. Because of discrepancy, mark in limbo
        elif overall_score is not None:
            if overall_score <= self.REVIEW_SCORE_THRESHOLD and safe:
                limbo_data = {
                    "limbo_status": True,
                    "pre_limbo_score": overall_score,
                }
        return limbo_data

    def _get_task_us_data(self):
        task_us_data = {
            "last_vetted_at": timezone.now(),
            "lang_code": self.validated_data["task_us_data"].pop("language", None),
            **self.validated_data["task_us_data"],
        }
        task_us_data["iab_categories"].append(self.validated_data["general_data"]["primary_category"])
        return task_us_data
