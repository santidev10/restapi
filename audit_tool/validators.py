from rest_framework.exceptions import ValidationError

from audit_tool.models import AuditCategory
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage


class AuditToolValidator(object):

    @staticmethod
    def validate_category(value, should_raise=True):
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
        language = None
        try:
            language = AuditLanguage.from_string(str(value).strip())
        except (ValueError, TypeError):
            if should_raise:
                raise ValidationError("Unable to process language: {}".format(value))
        return language

    @staticmethod
    def validate_country(value, should_raise=True):
        country = None
        try:
            country = AuditCountry.objects.get(country=value)
        except AuditCountry.DoesNotExist:
            if should_raise:
                raise ValidationError(f"Country: {value} not found.")
        return country
