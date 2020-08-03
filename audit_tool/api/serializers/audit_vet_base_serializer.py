from django.utils import timezone
from rest_framework.exceptions import ValidationError
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import ListField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from audit_tool.validators import AuditToolValidator
from brand_safety.languages import LANGUAGES
from brand_safety.models import BadWordCategory


class AuditVetBaseSerializer(Serializer):
    """
    Base serializer for vetting models
    """
    # None values defined on child classes
    REVIEW_SCORE_THRESHOLD = 89
    data_type = None
    document_model = None
    general_data_lang_code_field = None
    es_manager = None

    # Elasticsearch fields
    age_group = IntegerField(source="task_us_data.age_group", default=None)
    content_type = IntegerField(source="task_us_data.content_type", default=None)
    content_quality = IntegerField(source="task_us_data.content_quality", default=None)
    gender = IntegerField(source="task_us_data.gender", default=None)
    iab_categories = ListField(source="task_us_data.iab_categories", default=[])
    mismatched_language = BooleanField(source="task_us_data.mismatched_language", default=None)
    is_monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    YT_id = CharField(source="main.id", default=None)
    title = CharField(source="general_data.title", default=None)
    brand_safety = SerializerMethodField()
    brand_safety_overall_score = IntegerField(source="brand_safety.overall_score", default=None)
    language = SerializerMethodField()

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

    def validate_language_code(self, value):
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

    def validate_iab_categories(self, values: list):
        """
        Retrieve AuditCategory iab_category values. Raises ValidationError if not found
        :param values: list
        :return: AuditCategory
        """
        values = list(set(values))
        iab_categories = AuditToolValidator.validate_iab_categories(values)
        return iab_categories

    def validate_gender(self, value):
        """
        Retrieve AuditGender value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        gender = AuditToolValidator.validate_gender(int(value))
        gender_id = str(gender.id)
        return gender_id

    def validate_age_group(self, value):
        """
        Retrieve AuditAgeGroup value. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        age_group = AuditToolValidator.validate_age_group(int(value))
        age_group_id = str(age_group.id)
        return age_group_id

    def validate_content_type(self, value):
        """
        Retrieve AuditContentType value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        content_type = AuditToolValidator.validate_content_type(int(value))
        content_type_id = str(content_type.id)
        return content_type_id

    def validate_content_quality(self, value):
        """
        Retrieve AuditContentQuality value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        content_quality = AuditToolValidator.validate_content_quality(int(value))
        content_quality_id = str(content_quality.id)
        return content_quality_id

    def validate_category(self, value):
        """
        Retrieve audit_tool AuditCategory. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        category = AuditToolValidator.validate_category(value)
        category_id = str(category.id)
        return category_id

    def validate_brand_safety(self, values):
        categories = []
        if values:
            if type(values[0]) is str:
                key = "name"
            else:
                key = "id"
            mapping = {
                getattr(item, key): str(item.id)
                for item in BadWordCategory.objects.all()
            }
            try:
                categories = [mapping[val] for val in set(values)]
            except KeyError as e:
                raise ValidationError(f"Brand safety category not found: {e}")
        return categories

    def save_brand_safety(self, item_id):
        """
        Save brand safety categories in BlacklistItem table
        Will rescore the video if there is a change in brand safety
        :param item_id: str -> channel or video id
        :return: list -> Brand safety category ids
        """
        blacklist_categories = set(self.validated_data["task_us_data"].get("brand_safety", []))
        new_blacklist_scores = {
            str(item): 100
            for item in blacklist_categories
        }
        blacklist_item, created = BlacklistItem.objects.get_or_create(
            item_id=item_id,
            item_type=0 if self.data_type == "video" else 1,
            defaults={
                "item_id_hash": get_hash_name(item_id),
                "blacklist_category": new_blacklist_scores,
            })
        # Trigger celery brand safety update task if any blacklist categories created or changed
        if (created is True and new_blacklist_scores) or (
            created is False and blacklist_item.blacklist_category.keys() != new_blacklist_scores.keys()):
            blacklist_item.blacklist_category = new_blacklist_scores
            blacklist_item.save()
            self.update_brand_safety(item_id)
        data = list(blacklist_item.blacklist_category.keys())
        return data

    def save_elasticsearch(self, item_id):
        """
        Save vetting data to Elasticsearch
        :param item_id: str -> video id, channel id
        :param blacklist_categories: list -> [int, ...]
        :param es_manager: BaseManager
        :return: None
        """
        try:
            bs_data = self.es_manager.get([item_id])[0].brand_safety
            item_overall_score = bs_data.overall_score
            pre_limbo_score = bs_data.pre_limbo_score
        except (IndexError, AttributeError):
            item_overall_score = None
            pre_limbo_score = None

        blacklist_categories = self.save_brand_safety(item_id)
        task_us_data = {
            "last_vetted_at": timezone.now(),
            **self.validated_data["task_us_data"],
        }
        brand_safety_category_overall_scores = self._get_brand_safety(blacklist_categories)
        task_us_data["lang_code"] = self.validated_data["task_us_data"].pop("language", None)
        general_data = self._get_general_data(task_us_data)
        task_us_data["brand_safety"] = blacklist_categories if blacklist_categories else [None]
        brand_safety_limbo = self._get_brand_safety_limbo(task_us_data, item_overall_score, pre_limbo_score)

        # Update Elasticsearch document
        doc = self.document_model(item_id)
        doc.populate_monetization(**self.validated_data["monetization"])
        doc.populate_task_us_data(**task_us_data)
        doc.populate_brand_safety(categories=brand_safety_category_overall_scores, **brand_safety_limbo)
        doc.populate_general_data(**general_data)
        self.es_manager.upsert([doc], refresh=False)

    def _get_brand_safety(self, blacklist_categories):
        """
        Get updated brand safety categories based on blacklist_categories
        If category is in blacklist_category, will have a score of 0. Else will have a score of 100
        :param blacklist_categories: list
        :return:
        """
        # Brand safety categories that are not sent with vetting data are implicitly brand safe categories
        reset_brand_safety = set(self.all_brand_safety_category_ids) - set(
            [int(category) for category in blacklist_categories])
        brand_safety_category_overall_scores = {
            str(category_id): {
                "category_score": 100 if category_id in reset_brand_safety else 0
            }
            for category_id in self.all_brand_safety_category_ids
        }
        return brand_safety_category_overall_scores

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
        # Elasticsearch DSL does not serialize and will not save empty values: [], {}, None
        # https://github.com/elastic/elasticsearch-dsl-py/issues/460
        if not task_us_data.get("iab_categories"):
            general_data["iab_categories"] = task_us_data["iab_categories"] = [None]
        else:
            general_data["iab_categories"] = task_us_data["iab_categories"]
        return general_data

    def _get_brand_safety_limbo(self, task_us_data, overall_score, pre_limbo_score):
        """
        Determine if the vetting item should be reviewed.
        If task_us_data contains brand_safety data, then vetter has determined not safe by saving brand_safety
        categories.

        If the vetter is a vetting admin, accept vetting result as final
        :param brand_safety_data: BrandSafety document section data
        :return:
        """
        limbo_data = {}
        try:
            # If vetting admin, accept vetting result as final
            if self.context["user"].has_perm("userprofile.vet_audit_admin"):
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
