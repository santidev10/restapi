from rest_framework.serializers import Serializer
from rest_framework.serializers import SerializerMethodField
from rest_framework.serializers import ListField

from rest_framework.serializers import CharField
from rest_framework.serializers import BooleanField
from rest_framework.serializers import IntegerField



from audit_tool.models import AuditChannelVet
from audit_tool.models import get_hash_name
from audit_tool.validators import AuditToolValidator

class AuditChannelVetSerializer(Serializer):
    language = CharField(source="task_us_data.language", default=None)
    iab_categories = ListField(source="task_us_data.iab_categories", default=None)
    gender = CharField(source="task_us_data.gender", default=None)
    age_group = CharField(source="task_us_data.age_group", default=None)
    is_monetizable = BooleanField(source="monetization.is_monetizable", default=None)
    country = CharField(source="task_us_data.country", default=None)
    channel_type = CharField(source="task_us_data.channel_type", default=None)
    vetting_history = SerializerMethodField()

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
    #
    # def validate_category(self, value):
    #     """
    #     Retrieve audit_tool AuditCategory. Raises ValidationError if not found
    #     :param value: str
    #     :return: AuditCategory
    #     """
    #     category = AuditToolValidator.validate_category(value)
    #     return category
    #
    # def validate_country(self, value):
    #     """
    #     Retrieve audit_tool AuditCountry. Raises ValidationError if not found
    #     :param value: str
    #     :return: AuditCountry
    #     """
    #     country = AuditToolValidator.validate_country(value)
    #     return country

    # def save(self, **kwargs):
    #     """
    #     Save values on self and related model
    #     :param kwargs: dict
    #     :return:
    #     """
    #     obj = self.obj
    #     self.channel_metadata.age_group = self.age_group
    #     self.channel_metadata.language = self.language
    #     self.channel_metadata.category = self.category
    #     self.channel_metadata.country = self.country
    #     super().save(**kwargs)
