from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import RelatedField

from audit_tool.models import AuditChannelVet
from audit_tool.validators import AuditToolValidator
from aw_reporting.validators import AwReportingValidator


class AuditChannelVetSerializer(ModelSerializer):

    language = RelatedField(source="language", read_only=True)
    country = RelatedField(source="country", read_only=True)
    category = RelatedField(source="category.category_display_iab", read_only=True)

    class Meta:
        model = AuditChannelVet
        fields = (
            "id",
            "created_at",
            "updated_at",
            "checked_out",
            "vetted",
            "approved",

            "suitable",
            "channel_type",
            "is_monetized",

            "age_group",
            "category",
            "country",
            "gender",
            "language",
            "channel_type",
        )

    def validate_gender(self, value):
        """
        Retrieve aw_reporting GenderConstant. Raises ValidationError if not found
        :param value: int | str
            int ID, str gender name
        :return: GenderConstant
        """
        gender = AwReportingValidator.validate_gender(value)
        return gender

    def validate_age_group(self, value):
        """
        Retrieve aw_reporting AgeGroupConstant. Raises ValidationError if not found
        :param value: int | str
        :return: AgeGroupConstant
        """
        age_group = AwReportingValidator.validate_age_group(value)
        return age_group

    def validate_language(self, value):
        """
        Retrieve audit_tool AuditLanguage. Raises ValidationError if not found
        :param value: str
        :return: AuditLanguage
        """
        language = AuditToolValidator.validate_language(value)
        return language

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

    def validate_channel_type(self, value):
        """
        Retrieve audit_tool ChannelType. Raises ValidationError if not found
        :param value: str
        :return: ChannelType
        """
        channel_type = AuditToolValidator.validate_channel_type(value)
        return channel_type

    def save(self, **kwargs):
        """
        Save values on self and related model
        :param kwargs: dict
        :return:
        """
        obj = self.obj
        self.channel_metadata.age_group = self.age_group
        self.channel_metadata.language = self.language
        self.channel_metadata.category = self.category
        self.channel_metadata.country = self.country
        super().save(**kwargs)
