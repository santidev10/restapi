from rest_framework.exceptions import ValidationError

from audit_tool.models import AuditAgeGroup
from audit_tool.models import AuditCategory
from audit_tool.models import AuditContentType
from audit_tool.models import AuditCountry
from audit_tool.models import AuditGender
from audit_tool.models import AuditLanguage
from es_components.iab_categories import IAB_TIER2_SET


class AuditToolValidator(object):

    @staticmethod
    def validate_category(value, should_raise=True):
        """
        Validate category values
        :param value: int -> pk
        :param should_raise: bool
        """
        category = None
        try:
            category_id = int(value)
            try:
                category = AuditCategory.objects.get(pk=category_id)
            except AuditCategory.DoesNotExist:
                if should_raise:
                    raise ValidationError("Category with ID: {} does not exist. Please enter a valid category ID."
                                          .format(value))
                else:
                    category = None
        except ValueError:
            if should_raise:
                raise ValidationError("Expected Category ID value. Received: {}".format(value))
        return category

    @staticmethod
    def validate_language(value, should_raise=True):
        """
        Validate language values
        :param value: str
        :param should_raise: bool
        """
        language = None
        try:
            language = AuditLanguage.from_string(str(value).strip())
        except (ValueError, TypeError):
            if should_raise:
                raise ValidationError("Unable to process language: {}".format(value))
        return language

    @staticmethod
    def validate_country(value, should_raise=True):
        """
        Validate category values
        :param value: int -> pk
        :param should_raise: bool
        """
        country = None
        try:
            country = AuditCountry.objects.get(country=value)
        except AuditCountry.DoesNotExist:
            if should_raise:
                raise ValidationError(f"Country: {value} not found.")
        return country

    @staticmethod
    def validate_iab_categories(values, should_raise=True):
        """
        Check for values in IAB_TIER2_SET
        Must check against JSON since we are not saving values in database
        :param values: list
        :param should_raise: bool
        :return: None | values
        """
        for category in values:
            if category not in IAB_TIER2_SET:
                if should_raise:
                    raise ValidationError(f"IAB category not found: {category}")
        return values

    @staticmethod
    def validate_content_type(value, should_raise=True):
        """
        Validate category values
        :param value: int | str
        :param should_raise: bool
        :return: AuditContentType
        """
        content_type = None
        try:
            content_type = AuditContentType.get(value)
        except (KeyError, AuditContentType.DoesNotExist):
            if should_raise:
                if type(value) is str:
                    message = f"AuditContentType with channel_type: {value} not found."
                else:
                    message = f"AuditContentType with id: {value} not found."
                raise ValidationError(message)
        return content_type

    @staticmethod
    def validate_age_group(value, should_raise=True):
        """
        Validate age_group values
        :param value: int | str
        :param should_raise: bool
        :return: AuditAgeGroup
        """
        age_group = None
        try:
            age_group = AuditAgeGroup.get(value)
        except (KeyError, AuditAgeGroup.DoesNotExist):
            if should_raise:
                if type(value) is str:
                    message = f"AuditAgeGroup with age_group: {value} not found."
                else:
                    message = f"AuditAgeGroup with id: {value} not found."
                raise ValidationError(message)
        return age_group

    @staticmethod
    def validate_gender(value, should_raise=True):
        """
        Validate AuditGender values
        :param value: int | str
        :param should_raise: bool
        :return: AuditGender
        """
        gender = None
        try:
            gender = AuditGender.get(value)
        except (KeyError, AuditGender.DoesNotExist):
            if should_raise:
                if type(value) is str:
                    message = f"AuditGender with gender: {value} not found."
                else:
                    message = f"AuditGender with id: {value} not found."
                raise ValidationError(message)
        return gender
