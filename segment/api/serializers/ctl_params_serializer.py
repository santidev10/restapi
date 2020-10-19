"""
Module to handle validating CTL parameters for retrieving estimates from Elasticsearch and CTL creation.

Typical usage will be using CTLParamsSerializer to first provide default values and validate all possible params, then
using this validated data with CustomSegmentSerializer, in which only some fields defined on the CustomSegmentSerializer
will be used for actual creation but the rest of the data will be passed as context for other processes (e.g. creating
source file, creating audit, invoking export task, etc.)
"""
from datetime import datetime

from django.core.validators import MinValueValidator
from rest_framework import serializers
from rest_framework.serializers import empty
from rest_framework.exceptions import ValidationError

from es_components.iab_categories import IAB_TIER2_SET
from segment.models.constants import SegmentTypeEnum
from segment.utils.utils import validate_all_in
from audit_tool.models import AuditContentQuality
from audit_tool.models import AuditContentType


class NullableDictField(serializers.DictField):
    """ Provide default dict for null / empty values """
    def __init__(self):
        super().__init__(allow_null=True, allow_empty=True)

    def run_validation(self, data=None):
        super().run_validation(data)
        if not data or data is empty:
            data = {}
        return data


class NullableListField(serializers.ListField):
    """ Provide default list for null / empty values """
    def __init__(self):
        super().__init__(allow_null=True, allow_empty=True)

    def run_validation(self, data=None):
        super().run_validation(data)
        if not data or data is empty:
            data = []
        return data


class AdsPerformanceRangeField(serializers.CharField):
    """ Field to validate for range query values """
    def __init__(self):
        super().__init__(allow_null=True, allow_blank=True)

    def run_validation(self, data=None):
        super().run_validation(data)
        if data and data is not empty:
            data = self.validate_stats_field(data)
        return data

    def validate_stats_field(self, val):
        bounds = val.replace(" ", "").split(",")
        numeric_bounds = [bound for bound in bounds if bound.isnumeric()]
        if len(bounds) != 2 or len(numeric_bounds) not in range(1, 3):
            val = None
        return val


class EmptyCharDateField(serializers.CharField):
    """
    Validate date formatted strings
    We don't use a Date field here since the date string is not being used for deserialization for db row creation, but
    for Elasticsearch date queries. We only need to validate that the date format is what Elasticsearch expects.
    """
    def __init__(self):
        super().__init__(allow_null=True, allow_blank=True)

    def run_validation(self, data=None):
        super().run_validation(data)
        if data and data is not empty:
            try:
                datetime.strptime(data, "%Y-%m-%d")
            except ValueError:
                raise ValidationError("Date format must be YYYY-mm-dd")
        return data


class NullableNumeric(serializers.CharField):
    def __init__(self):
        super().__init__(allow_null=True)

    def run_validation(self, data=None):
        if data and data is not empty and isinstance(data, int) is False:
            formatted = data.replace(",", "")
            try:
                data = int(formatted)
            except ValueError:
                raise ValidationError(f"The value: '{formatted}' is not a valid number.")
        return data


class NonRequiredBooleanField(serializers.BooleanField):
    def __init__(self, *_, **kwargs):
        super().__init__(allow_null=True, required=False, **kwargs)


class CoerceListMemberField(serializers.Field):
    def __init__(self, *args, **kwargs):
        self.valid_values = kwargs.pop("valid_values")
        super().__init__(*args, **kwargs)

    """ Coerce incoming value to list """
    def run_validation(self, data=None):
        if data is None or data is empty:
            data = []
        elif not isinstance(data, list):
            data = [data]
        validated = validate_all_in(data, self.valid_values)
        return validated


class CTLParamsSerializer(serializers.Serializer):
    age_groups = NullableListField()
    average_cpm = AdsPerformanceRangeField()
    average_cpv = AdsPerformanceRangeField()
    content_categories = NullableListField()
    content_quality = CoerceListMemberField(valid_values=set(AuditContentQuality.to_str.keys()))
    content_type = CoerceListMemberField(valid_values=set(AuditContentType.to_str.keys()))
    countries = NullableListField()
    ctr = AdsPerformanceRangeField()
    ctr_v = AdsPerformanceRangeField()
    exclude_content_categories = NullableListField()
    exclusion_hit_threshold = serializers.IntegerField(required=False, validators=[MinValueValidator(1)])
    gender = serializers.IntegerField(allow_null=True)
    ias_verified_date = EmptyCharDateField()
    inclusion_hit_threshold = serializers.IntegerField(required=False, validators=[MinValueValidator(1)])
    is_vetted = serializers.NullBooleanField(required=False)
    languages = NullableListField()
    last_30day_views = AdsPerformanceRangeField()
    last_upload_date = EmptyCharDateField()
    maximum_duration = serializers.IntegerField(required=False, allow_null=True)
    minimum_duration = serializers.IntegerField(required=False, allow_null=True)
    minimum_subscribers = NullableNumeric()
    minimum_videos = NullableNumeric()
    minimum_views = NullableNumeric()
    score_threshold = serializers.IntegerField()
    segment_type = serializers.IntegerField(allow_null=True)
    sentiment = serializers.IntegerField()
    severity_filters = NullableDictField()
    vetted_after = EmptyCharDateField()
    vetting_status = serializers.IntegerField(allow_null=True)
    video_quartile_100_rate = AdsPerformanceRangeField()
    video_view_rate = AdsPerformanceRangeField()

    ads_stats_include_na = NonRequiredBooleanField()
    age_groups_include_na = NonRequiredBooleanField()
    countries_include_na = NonRequiredBooleanField()
    minimum_subscribers_include_na = NonRequiredBooleanField()
    minimum_videos_include_na = NonRequiredBooleanField()
    minimum_views_include_na = NonRequiredBooleanField()
    mismatched_language = NonRequiredBooleanField()

    def validate(self, data):
        validated_data = super().validate(data)
        self._validate_categories(data)
        return validated_data

    def validate_segment_type(self, data):
        try:
            SegmentTypeEnum(data)
        except ValueError:
            raise ValidationError(f"Invalid list_type: {data}. 0 = video, 1 = channel.")
        return data

    def _validate_categories(self, data):
        content_categories = data["content_categories"]
        exclude_content_categories = data["exclude_content_categories"]
        if content_categories or exclude_content_categories:
            unique_content_categories = set(content_categories + exclude_content_categories)
            bad_content_categories = list(unique_content_categories - IAB_TIER2_SET)
            if bad_content_categories:
                comma_separated = ", ".join(str(item) for item in bad_content_categories)
                raise ValidationError(detail=f"The following content_categories are invalid: '{comma_separated}'")