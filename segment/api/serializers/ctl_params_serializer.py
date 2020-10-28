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
from utils.serializers.fields.coerce_time_to_seconds_field import CoerceTimeToSecondsField


class NullableDictField(serializers.DictField):
    """ Provide default dict for null / empty values """
    def __init__(self, *_, **kwargs):
        super().__init__(allow_null=True, allow_empty=True, **kwargs)

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
        # coerce ",", ", " to None
        if not [bound for bound in bounds if bound]:
            return None
        # ensure bound is numeric
        numeric_bounds = [bound for bound in list(map(self.is_number, bounds)) if bound is not False]
        if len(bounds) != 2 or len(numeric_bounds) not in range(1, 3):
            msg = f"Range must be a numeric range string following the format 'x, y', 'x,', ',x'"
            raise ValidationError(msg)
        if len(numeric_bounds) == 2:
            lower_bound, upper_bound = numeric_bounds
            if lower_bound >= upper_bound:
                msg = f"The lower bound ({lower_bound}) must be lower than the upper bound ({upper_bound})"
                raise ValidationError(msg)

        return val

    @staticmethod
    def is_number(value: str):
        if value.isnumeric():
            return value
        try:
            float(value)
        except ValueError:
            return False
        return value


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


class NullableCharNumeric(serializers.CharField):
    """
    Validate integer values represented as comma formatted strings e.g. 1,000,000
    """
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
    """
    Serializer to handle validating CTL parameters for retrieving estimates from Elasticsearch and CTL creation.

    Typical usage will be using CTLParamsSerializer to validate CTL params through SegmentCreateApiView
    or SegmentCreateOptionsApiView and passing as serializer context for creates / updates.
    """
    age_groups = NullableListField()
    average_cpm = AdsPerformanceRangeField()
    average_cpv = AdsPerformanceRangeField()
    content_categories = NullableListField()
    content_quality = CoerceListMemberField(valid_values=set(AuditContentQuality.to_str_with_unknown.keys()))
    content_type = CoerceListMemberField(valid_values=set(AuditContentType.to_str_with_unknown.keys()))
    countries = NullableListField()
    ctr = AdsPerformanceRangeField()
    ctr_v = AdsPerformanceRangeField()
    exclude_content_categories = NullableListField()
    exclusion_hit_threshold = serializers.IntegerField(required=False, allow_null=True, default=1,
                                                       validators=[MinValueValidator(1)])
    gender = NullableListField()
    ias_verified_date = EmptyCharDateField()
    inclusion_hit_threshold = serializers.IntegerField(required=False, allow_null=True, default=1,
                                                       validators=[MinValueValidator(1)])
    is_vetted = serializers.NullBooleanField(required=False)
    languages = NullableListField()
    last_30day_views = AdsPerformanceRangeField()
    last_upload_date = EmptyCharDateField()
    maximum_duration = CoerceTimeToSecondsField(required=False, allow_null=True)
    minimum_duration = CoerceTimeToSecondsField(required=False, allow_null=True)
    minimum_subscribers = NullableCharNumeric()
    minimum_videos = NullableCharNumeric()
    minimum_views = NullableCharNumeric()
    score_threshold = serializers.IntegerField()
    segment_type = serializers.IntegerField(allow_null=True)
    sentiment = serializers.IntegerField()
    severity_filters = NullableDictField(required=False)
    vetted_after = EmptyCharDateField()
    vetting_status = NullableListField()
    video_quartile_100_rate = AdsPerformanceRangeField()
    video_view_rate = AdsPerformanceRangeField()

    ads_stats_include_na = NonRequiredBooleanField()
    countries_include_na = NonRequiredBooleanField()
    languages_include_na = NonRequiredBooleanField()
    minimum_subscribers_include_na = NonRequiredBooleanField()
    minimum_videos_include_na = NonRequiredBooleanField()
    minimum_views_include_na = NonRequiredBooleanField()
    mismatched_language = NonRequiredBooleanField()

    def validate(self, data: dict) -> dict:
        validated_data = super().validate(data)
        # Only validate if content_categories was passed in
        try:
            self._validate_categories(data)
        except KeyError:
            pass
        return validated_data

    def validate_segment_type(self, data: int) -> int:
        try:
            SegmentTypeEnum(data)
        except ValueError:
            raise ValidationError(f"Invalid list_type: {data}. 0 = video, 1 = channel.")
        return data

    def _validate_categories(self, data: dict) -> None:
        content_categories = data["content_categories"]
        exclude_content_categories = data["exclude_content_categories"]
        if content_categories or exclude_content_categories:
            unique_content_categories = set(content_categories + exclude_content_categories)
            bad_content_categories = list(unique_content_categories - IAB_TIER2_SET)
            if bad_content_categories:
                comma_separated = ", ".join(str(item) for item in bad_content_categories)
                raise ValidationError(detail=f"The following content_categories are invalid: '{comma_separated}'")