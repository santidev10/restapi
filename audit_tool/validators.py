from rest_framework.exceptions import ValidationError

from audit_tool.models import AuditCategory
from audit_tool.models import AuditCountry
from audit_tool.models import AuditLanguage

from audit_tool.models import ChannelType


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

    @staticmethod
    def validate_iab_categories(values, should_raise=True):
        if type(values) is str:
            values = values[0]
        try:
            [AuditCategory.objects.filter(category_display_iab=val).exists for val in values]
        except AuditCategory.DoesNotExist as e:
            if should_raise:
                raise ValidationError(f"Category does not exist: {e}")
        return values

    @staticmethod
    def validate_channel_type(value, should_raise=True):
        channel_type = None
        item_id = value
        try:
            if type(item_id) is str:
                item_id = ChannelType.to_int.get(item_id.lower())
            channel_type = ChannelType.objects.get(id=item_id)
        except ChannelType.DoesNotExist:
            if should_raise:
                raise ValueError(f"ChannelType: {value} not found.")
        return channel_type
