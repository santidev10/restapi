from django.db.models import Subquery
from rest_framework.serializers import CharField
from rest_framework.serializers import IntegerField
from rest_framework.serializers import ReadOnlyField

from django.db.models import Case
from django.db.models import F
from django.db.models import Q
from django.db.models import FloatField as DBFloatField
from django.db.models import OuterRef
from django.db.models import QuerySet
from django.db.models import Subquery
from django.db.models import Sum
from django.db.models import When
from rest_framework.fields import BooleanField
from rest_framework.fields import CharField
from rest_framework.fields import DateField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.fields import ReadOnlyField
from rest_framework.fields import SerializerMethodField
from rest_framework.serializers import ModelSerializer

from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import AdGroup
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableTopicSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableKeywordSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import DemoGenderTableSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import TargetTableAudienceSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import DemoAgeRangeTableSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import DevicesTableSerializer
from aw_reporting.models import TopicStatistic
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.models import get_ctr_v
from utils.db.get_exists import get_exists
from ads_analyzer.reports.account_targeting_report.constants import BASE_SERIALIZER_FIELDS
from ads_analyzer.reports.account_targeting_report.base_serializer import BaseSerializer
from aw_reporting.models import CriterionType


class AgeTargetingSerializer(BaseSerializer):
    criterion_name = CriterionType.AGE

    class Meta(BaseSerializer.Meta):
        model = AgeRangeStatistic
        group_by = ("ad_group__id", "age_range_id")


class VideoCreativeTableSerializer(BaseSerializer):
    
    name = CharField(source="ad__creative_name")
    id = CharField(source="creative__id")
    # rate_type = GoalTypeField(source="ad__ad_group__campaign__salesforce_placement__goal_type_id")
    contracted_rate = FloatField(source="ad__ad_group__campaign__salesforce_placement__ordered_rate")

    class Meta(BaseSerializer.Meta):
        model = VideoCreativeStatistic
        group_by = ("creative__id",)


class DeviceSerializer(BaseSerializer):

    class Meta(BaseSerializer.Meta):
        model = AdGroupStatistic
        group_by = ("ad_group__id", "device_id")


class GenderSerializer(BaseSerializer):
    criterion_name = CriterionType.GENDER
    
    class Meta(BaseSerializer.Meta):
        model = GenderStatistic
        group_by = ("ad_group__id", "gender_id")


class KeywordTargetingSerializer(BaseSerializer):
    criterion_name = CriterionType.KEYWORD
    
    name = CharField(source="keyword")
    type = ReadOnlyField(default="Keyword")

    class Meta(BaseSerializer.Meta):
        model = KeywordStatistic
        group_by = ("ad_group__id", "keyword")


class TopicTargetingSerializer(BaseSerializer):
    criterion_name = CriterionType.VERTICAL
    
    name = CharField(source="topic__name")
    topic_id = IntegerField(source="topic__id")
    type = ReadOnlyField(default="Topic")

    class Meta(BaseSerializer.Meta):
        fields = BaseSerializer.Meta.fields + ("topic_id",)
        model = TopicStatistic
        group_by = ("topic__id", "topic__name")


class PlacementChannelTargetingSerializer(BaseSerializer):
    criterion_name = CriterionType.PLACEMENT
    
    name = CharField(source="yt_id")
    type = ReadOnlyField(default="Channel")

    class Meta(BaseSerializer.Meta):
        model = YTChannelStatistic
        group_by = ("ad_group__id", "yt_id")


class PlacementVideoTargetingSerializer(BaseSerializer):
    criterion_name = CriterionType.PLACEMENT
    
    name = CharField(source="yt_id")
    type = ReadOnlyField(default="Video")

    class Meta(BaseSerializer.Meta):
        model = YTVideoStatistic
        group_by = ("ad_group__id", "yt_id")


class AudienceTargetingSerializer(BaseSerializer):
    criterion_name = CriterionType.USER_INTEREST_LIST

    name = CharField(source="audience__name")
    type = ReadOnlyField(default="Audience")

    class Meta(BaseSerializer.Meta):
        model = AudienceStatistic
        group_by = ("ad_group__id", "audience_id", "audience__name", "audience__type")
