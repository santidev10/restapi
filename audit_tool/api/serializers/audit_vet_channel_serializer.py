from rest_framework.exceptions import ValidationError
from rest_framework.serializers import BooleanField
from rest_framework.serializers import CharField
from rest_framework.serializers import DateTimeField
from rest_framework.serializers import ListField
from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField

from audit_tool.models import AuditChannelVet
from audit_tool.models import BlacklistItem
from audit_tool.models import get_hash_name
from audit_tool.validators import AuditToolValidator
from aw_reporting.models import Gender
from aw_reporting.models import AgeRange
from brand_safety.models import BadWordCategory
from es_components.models import Channel


class AuditChannelVetSerializer(Serializer):
    age_group = CharField(source="task_us_data.age_group")
    brand_safety = ListField(source="task_us_data.brand_safety")
    channel_type = CharField(source="task_us_data.channel_type")
    country = CharField(source="task_us_data.country")
    gender = CharField(source="task_us_data.gender")
    iab_categories = ListField(source="task_us_data.iab_categories")
    is_monetizable = BooleanField(source="monetization.is_monetizable")
    language = CharField(source="task_us_data.language")

    vetting_history = SerializerMethodField()
    audit_instructions = SerializerMethodField()
    segment_title = SerializerMethodField()

    url = SerializerMethodField()
    is_checked_out = BooleanField(required=False)
    suitable = BooleanField(required=False)
    processed = DateTimeField(required=False)

    def __init__(self, *args, **kwargs):
        try:
            self.channel_id = kwargs.pop("channel_id")
            self.channel_audit = kwargs.pop("audit_item")
            self.segment = kwargs.pop("segment")
        except KeyError:
            raise ValueError("kwargs for channel_id, channel_audit, and segment must be provided.")
        super().__init__(*args, **kwargs)

    def get_url(self, doc):
        url = f"https://www.youtube.com/channel/{doc.main.id}/"
        return url

    def get_segment_title(self, *_, **__):
        """
        Get segment title if available
        :return: None | str
        """
        title = self.segment.title
        return title

    def get_audit_instructions(self, *_, **__):
        """
        Get segment title if available
        :return: None | str
        """
        instructions = self.segment.audit_instructions
        return instructions

    def get_vetting_history(self, doc):
        """
        Retrieve vetting history of all AuditChannelVet items with FK to AuditChannel
        :param doc: Elasticsearch document
        :return: dict
        """
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
        try:
            value = value.upper()
            getattr(Gender, value)
        except AttributeError:
            raise ValueError(f"Invalid gender: {value}")
        return value

    def validate_age_group(self, value):
        """
        Retrieve age_group enum value. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        try:
            value = value.upper()
            getattr(AgeRange, value)
        except AttributeError:
            raise ValueError(f"Invalid age group: {value}")
        return value

    def validate_channel_type(self, value):
        """
        Retrieve channel_type enum value. Raises ValidationError if not found
        :param value: str
        :return: str
        """
        channel_type = AuditToolValidator.validate_channel_type(value)
        return channel_type

    def validate_category(self, value):
        """
        Retrieve audit_tool AuditCategory. Raises ValidationError if not found
        :param value: str
        :return: AuditCategory
        """
        category = AuditToolValidator.validate_category(value)
        return category

    def validate_country(self, value):
        """
        Retrieve audit_tool AuditCountry. Raises ValidationError if not found
        :param value: str
        :return: AuditCountry
        """
        country = AuditToolValidator.validate_country(value)
        return country

    def validate_brand_safety(self, values):
        try:
            [BadWordCategory.objects.get(name=value) for value in values]
        except BadWordCategory.DoesNotExist as e:
            raise ValidationError(f"Category not found: {e}")
        return values

    def save(self, **kwargs):
        """
        Save values on self and related model
        :param kwargs: dict
        :return:
        """
        # update channel meta monetization
        is_monetizable = self.validated_data["monetization"]["is_monetizable"]
        task_us_data = self.validated_data["task_us_data"]

        channel_meta = self.channel_audit.channel.auditchannelmeta
        channel_meta.monetised = is_monetizable
        channel_meta.save()

        blacklist_item, created = BlacklistItem.objects.get_or_create(
            item_id=self.channel_id,
            item_type=1,
            defaults={
                "item_id_hash": get_hash_name(self.channel_id),
                "blacklist_category": self.validated_data["brand_safety"],
        })
        # Update existing categories
        if created is False:
            all_categories = blacklist_item.blacklist_category + self.validated_data["brand_safety"]
            blacklist_item.blacklist_category = all_categories
            blacklist_item.save()

        # Update Elasticsearch document
        doc = Channel(**{
            "monetization": {
                "is_monetizable": is_monetizable,
            },
            "task_us_data": {
                "iab_categories": task_us_data["iab_categories"],
                "language": task_us_data["language"].language,
                "gender": task_us_data["gender"],
                "age_group": task_us_data["age_group"],
                "country": task_us_data["country"].country,
                "channel_type": task_us_data["channel_type"].channel_type,
            }
        })
        self.es_manager.upsert([doc])
