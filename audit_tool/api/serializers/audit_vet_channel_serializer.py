from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import RelatedField

from audit_tool.models import AuditChannelVet
from audit_tool.models import AuditVideoVet
from audit_tool.validators import AuditToolValidator
from aw_reporting.validators import AwReportingValidator


class AuditVideoChannelSerializer(ModelSerializer):

    language = RelatedField(source="language")
    country = RelatedField(source="country")
    category = RelatedField(source="category.category_display_iab")

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
        Retrieve GenderConstant. Raises ValidationError if not found
        :param value: int | str
            int ID, str gender name
        :return: GenderConstant
        """
        gender = AwReportingValidator.validate_gender(value)
        return gender

    def validate_age_group(self, value):
        """
        Retrieve AgeGroupConstant. Raises ValidationError if not found
        :param value: int | str
        :return: AgeGroupConstant
        """
        age_group = AwReportingValidator.validate_age_group(value)
        return age_group

    def validate_language(self, value):
        language = AuditToolValidator.validate_language(value)
        return language

    def validate_category(self, value):
        category = AuditToolValidator.validate_category(value)
        return category

    def validate_country(self, value):
        country = AuditToolValidator.validate_country(value)
        return country

    def validate_channel_type(self, value):
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
