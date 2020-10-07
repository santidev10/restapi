from rest_framework import serializers
from rest_framework.exceptions import ValidationError

from es_components.iab_categories import IAB_TIER2_SET
from segment.models.constants import SegmentTypeEnum


class NullableDictField(serializers.DictField):
    def __init__(self):
        super().__init__(allow_null=True, allow_empty=True)

    def run_validation(self, data=None):
        super().run_validation(data)
        if not data:
            data = {}
        return data


class NullableListField(serializers.ListField):
    def __init__(self):
        super().__init__(allow_null=True, allow_empty=True)

    def run_validation(self, data=None):
        super().run_validation(data)
        if not data:
            data = []
        return data


class AdsPerformanceRangeField(serializers.CharField):
    def __init__(self):
        super().__init__(allow_null=True, allow_blank=True)

    def run_validation(self, data=None):
        super().run_validation(data)
        if data:
            data = self.validate_stats_field(data)
        return data

    def validate_stats_field(self, val):
        bounds = val.replace(" ", "").split(",")
        numeric_bounds = [bound for bound in bounds if bound.isnumeric()]
        if len(bounds) != 2 or len(numeric_bounds) not in range(1, 3):
            val = None
        return val


class EmptyDateField(serializers.DateField):
    def __init__(self):
        super().__init__(format="%Y-%m-%d", allow_null=True)

    def run_validation(self, data=None):
        if data:
            data = super().run_validation(data)
        return data


class NullableNumeric(serializers.CharField):
    def __init__(self):
        super().__init__(allow_null=True)

    def run_validation(self, data=None):
        if data:
            formatted = data.replace(",", "")
            try:
                data = int(formatted)
            except ValueError:
                raise ValidationError(f"The value: '{formatted}' is not a valid number.")
        return data


class CTLParamsSerializer(serializers.Serializer):
    age_groups = NullableListField()
    average_cpv = AdsPerformanceRangeField()
    average_cpm = AdsPerformanceRangeField()
    content_categories = NullableListField()
    content_quality = serializers.IntegerField()
    content_type = serializers.IntegerField()
    countries = NullableListField()
    ctr = AdsPerformanceRangeField()
    ctr_v = AdsPerformanceRangeField()
    exclude_content_categories = NullableListField()
    exclusion_file = serializers.FileField(write_only=True, required=False)
    languages = NullableListField()
    last_30day_views = AdsPerformanceRangeField()
    last_upload_date = EmptyDateField()
    ias_verified_date = EmptyDateField()
    inclusion_file = serializers.FileField(write_only=True, required=False)
    minimum_subscribers = NullableNumeric()
    minimum_views = NullableNumeric()
    minimum_videos = NullableNumeric()
    segment_type = serializers.IntegerField()
    sentiment = serializers.IntegerField()
    severity_filters = NullableDictField()
    score_threshold = serializers.IntegerField()
    source_file = serializers.FileField(write_only=True, required=False)
    title = serializers.CharField()
    vetted_after = EmptyDateField()
    video_quartile_100_rate = AdsPerformanceRangeField()
    video_view_rate = AdsPerformanceRangeField()

    def validate_content_type(self, data):
        if data == -1:
            data = None
        return data

    def validate_content_quality(self, data):
        if data == -1:
            data = None
        return data

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