from distutils.util import strtobool

from rest_framework.serializers import CharField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError

from audit_tool.models import AuditLanguage
from brand_safety.models import BadWord
from brand_safety.models import BadWordCategory
from utils.lang import is_english


class BadWordSerializer(ModelSerializer):
    category = CharField(max_length=80)
    language = CharField(max_length=80, required=False, default=BadWord.DEFAULT_LANGUAGE)

    def validate_name(self, value):
        try:
            name = str(value).strip().lower()
            if is_english(name):
                if len(name) < 3:
                    raise ValidationError("Keywords must be at least 3 characters long.")
            return name
        except (ValueError, TypeError):
            raise ValidationError("Unable to process name: {}".format(value))

    def validate_category(self, value):
        try:
            category_id = int(value)
            try:
                category = BadWordCategory.objects.get(pk=category_id)
            except BadWordCategory.DoesNotExist:
                raise ValidationError("Category with ID: {} does not exist. Please enter a valid category ID."
                                      .format(value))
            return category
        except ValueError:
            raise ValidationError("Expected Category ID value. Received: {}".format(value))

    def validate_negative_score(self, value):
        try:
            score_val = int(value)
            if score_val < 1 or score_val > 4:
                raise ValidationError("Negative_score value: {} is out of range. Must be between 1-4.".format(value))
            return score_val
        except ValueError:
            raise ValidationError("Negative_score must be Integer with value between 1-4. Received: {}".format(value))

    def validate_language(self, value):
        try:
            language = AuditLanguage.from_string(str(value).strip())
        except (ValueError, TypeError):
            raise ValidationError("Unable to process language: {}".format(value))
        return language

    def validate_meta_scoring(self, value):
        if isinstance(value, bool):
            return value
        try:
            meta_scoring = strtobool(value)
        except (ValueError, TypeError):
            raise ValidationError("Unable to process meta_scoring: {}".format(value))
        return meta_scoring

    def validate_comment_scoring(self, value):
        if isinstance(value, bool):
            return value
        try:
            comment_scoring = strtobool(value)
        except (ValueError, TypeError):
            raise ValidationError("Unable to process comment_scoring: {}".format(value))
        return comment_scoring

    class Meta:
        model = BadWord
        fields = ("id", "name", "category", "negative_score", "language", "meta_scoring", "comment_scoring")
