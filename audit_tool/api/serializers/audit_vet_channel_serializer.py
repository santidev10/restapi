from django.utils import timezone

from rest_framework.exceptions import ValidationError
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import ListField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from audit_tool.models import AuditChannelVet
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from audit_tool.validators import AuditToolValidator
from brand_safety.models import BadWordCategory
from es_components.models import Channel
from es_components.constants import Sections


class AuditChannelVetSerializer(Serializer):
    SECTIONS = (Sections.MAIN, Sections.TASK_US_DATA, Sections.MONETIZATION)

    """
    age_group, channel_type, gender, and brand_safety values are stored as id values
    """
    age_group = CharField(source="task_us_data.age_group", default=None)
    channel_type = CharField(source="task_us_data.channel_type", default=None)
    gender = CharField(source="task_us_data.gender", default=None)
    brand_safety = ListField(source="task_us_data.brand_safety", default=[])
    language = CharField(source="task_us_data.language", default=None)
    iab_categories = ListField(source="task_us_data.iab_categories", default=[])
    is_monetizable = BooleanField(source="monetization.is_monetizable", default=None)

    vetting_history = SerializerMethodField()
    segment_title = SerializerMethodField()

    url = SerializerMethodField()
    is_checked_out = BooleanField(required=False)
    suitable = BooleanField(required=False)
    processed = DateTimeField(required=False)
    processed_by_user_id = IntegerField(required=False)

    def __init__(self, *args, **kwargs):
        try:
            self.segment = kwargs.pop("segment", None)
        except KeyError:
            pass
        super().__init__(*args, **kwargs)

    def get_url(self, doc):
        url = f"https://www.youtube.com/channel/{doc.main.id}/"
        return url

    def get_segment_title(self, *_, **__):
        """
        Get segment title if available
        :return: None | str
        """
        title = getattr(self.segment, "title", None)
        return title

    def get_vetting_history(self, doc):
        """
        Retrieve vetting history of all AuditChannelVet items with FK to AuditChannel
        Only retrieve if serializing with Elasticsearch document
        :param doc: Elasticsearch document
        :return: dict
        """
        history = None
        if hasattr(doc, "main"):
            channel_id_hash = get_hash_name(doc.main.id)
            vetting_items = AuditChannelVet.objects\
                .filter(channel__channel_id_hash=channel_id_hash, processed__isnull=False)\
                .select_related("channel__auditchannelmeta")
            history = [{
                "data": f"{item.channel.auditchannelmeta.name} - {item.processed.strftime('%b %d %Y')}",
                "suitable": item.clean
            } for item in vetting_items]
        return history

    def validate_language(self, value):
        """
        Retrieve audit_tool AuditLanguage. Raises ValidationError if not found
        :param value: str
        :return: AuditLanguage
        """
        language = AuditToolValidator.validate_language(value)
        return language

    def validate_iab_categories(self, value):
        """
        Retrieve AuditCategory iab_category values. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        iab_categories = AuditToolValidator.validate_iab_categories(value)
        return iab_categories

    def validate_gender(self, value):
        """
        Retrieve age_group enum value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        gender = AuditToolValidator.validate_gender(int(value))
        return gender

    def validate_age_group(self, value):
        """
        Retrieve age_group enum value. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        age_group = AuditToolValidator.validate_age_group(int(value))
        return age_group

    def validate_channel_type(self, value):
        """
        Retrieve channel_type enum value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        channel_type = AuditToolValidator.validate_channel_type(int(value))
        return channel_type

    def validate_category(self, value):
        """
        Retrieve audit_tool AuditCategory. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        category = AuditToolValidator.validate_category(value)
        return category

    def validate_brand_safety(self, values):
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
                categories = [mapping[val] for val in values]
            except KeyError as e:
                raise ValidationError(f"Category not found: {e}")
        return categories

    def save(self, **kwargs):
        """
        Save values on self and related model
        :param kwargs: dict
        :return:
        """
        if not self.instance:
            raise ValueError("To save serializer, must be provided instance object"
                             "during instantiation.")
        channel_meta = self.instance.channel.auditchannelmeta
        channel_id = channel_meta.channel.channel_id

        self._save_vetting_item(channel_meta)
        blacklist_categories = self._save_brand_safety(channel_id)
        self._save_elasticsearch(channel_id, blacklist_categories)

    def _save_vetting_item(self, channel_meta):
        """
        Save only required fields for database
        :param channel_meta: AuditChannelMeta
        :return: None
        """
        data = {
            "clean": self.validated_data["suitable"],
            "is_checked_out": False,
            "processed": timezone.now(),
            "processed_by_user_id": self.validated_data["processed_by_user_id"],
        }
        for key, value in data.items():
            setattr(self.instance, key, value)
        self.instance.save(update_fields=list(data.keys()))
        if self.validated_data["monetization"]["is_monetizable"] is True:
            channel_meta.monetised = True
            channel_meta.save()

    def _save_brand_safety(self, channel_id):
        """
        Save brand safety categories in BlacklistItem table
        :param channel_id: str
        :return: list -> Brand safety category ids
        """
        new_blacklist_scores = {
            str(item.id): 100
            for item in self.validated_data["task_us_data"]["brand_safety"]
        }
        blacklist_item, created = BlacklistItem.objects.get_or_create(
            item_id=channel_id,
            item_type=1,
            defaults={
                "item_id_hash": get_hash_name(channel_id),
                "blacklist_category": new_blacklist_scores,
            })
        # Update existing categories with new blacklist categories
        if created is False:
            blacklist_item.blacklist_category.update(new_blacklist_scores)
            blacklist_item.save()
        data = list(blacklist_item.blacklist_category.keys())
        return data

    def _save_elasticsearch(self, channel_id, blacklist_categories):
        """
        Save vetting data to Elasticsearch
        :param channel_id: str
        :param blacklist_categories: list -> [int, ...]
        :return: None
        """
        task_us_data = self.validated_data["task_us_data"]
        # Serialize validated data objects
        task_us_data["brand_safety"] = blacklist_categories
        task_us_data["language"] = task_us_data["language"].language
        # Update Elasticsearch document
        channel_doc = Channel(channel_id)
        channel_doc.populate_monetization(**self.validated_data["monetization"])
        channel_doc.populate_task_us_data(**task_us_data)
        self.segment.es_manager.upsert_sections = self.SECTIONS
        self.segment.es_manager.upsert([channel_doc])

    def to_representation(self, instance):
        """
        Only repr Elasticsearch documents
        :param instance:
        :return:
        """
        res = instance
        if hasattr(instance, "main"):
            res = super().to_representation(instance)
        return res
