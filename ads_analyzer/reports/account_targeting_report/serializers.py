from rest_framework.fields import CharField
from rest_framework.fields import IntegerField
from rest_framework.fields import ReadOnlyField
from rest_framework.fields import SerializerMethodField

from ads_analyzer.reports.account_targeting_report.base_serializer import BaseSerializer
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import ParentStatistic
from aw_reporting.models import age_range_str
from aw_reporting.models import device_str
from aw_reporting.models import gender_str
from aw_reporting.models import parent_str
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import CriteriaTypeEnum


class AdGroupSerializer(BaseSerializer):
    type = ReadOnlyField(default="AdGroup")
    target_name = SerializerMethodField()

    class Meta(BaseSerializer.Meta):
        model = AdGroupStatistic
        group_by = []

    def get_target_name(self, obj):
        name = obj["ad_group__name"]
        return name


class AgeTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.AGE_RANGE.value)
    type_name = CharField(default=CriteriaTypeEnum.AGE_RANGE.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source="age_range_id")

    class Meta(BaseSerializer.Meta):
        model = AgeRangeStatistic
        group_by = ("ad_group__id", "age_range_id")

    def get_target_name(self, obj):
        age_range = age_range_str(obj["age_range_id"])
        return age_range

    def get_criteria(self, obj):
        pass


class GenderTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.GENDER.value)
    type_name = CharField(default=CriteriaTypeEnum.GENDER.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source="gender_id")

    class Meta(BaseSerializer.Meta):
        model = GenderStatistic
        group_by = ("ad_group__id", "gender_id")

    def get_target_name(self, obj):
        gender = gender_str(obj["gender_id"])
        return gender


class KeywordTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.KEYWORD.value)
    type_name = CharField(default=CriteriaTypeEnum.KEYWORD.name)
    target_name = CharField(source="keyword")
    criteria = CharField(source="keyword")

    class Meta(BaseSerializer.Meta):
        model = KeywordStatistic
        group_by = ("ad_group__id", "keyword")


class TopicTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.VERTICAL.value)
    type_name = CharField(default=CriteriaTypeEnum.VERTICAL.name)
    target_name = CharField(source="topic__name")
    topic_id = IntegerField(source="topic__id")
    criteria = IntegerField(source="topic__id")

    class Meta(BaseSerializer.Meta):
        model = TopicStatistic
        fields = BaseSerializer.Meta.fields + ("topic_id",)
        group_by = ("topic__id", "topic__name")


class PlacementChannelTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.YOUTUBE_CHANNEL.value)
    type_name = CharField(default=CriteriaTypeEnum.YOUTUBE_CHANNEL.name)
    target_name = CharField(source="yt_id")
    criteria = CharField(source="yt_id")

    class Meta(BaseSerializer.Meta):
        model = YTChannelStatistic
        group_by = ("ad_group__id", "yt_id")


class PlacementVideoTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.YOUTUBE_VIDEO.value)
    type_name = CharField(default=CriteriaTypeEnum.YOUTUBE_VIDEO.name)
    target_name = CharField(source="yt_id")
    criteria = CharField(source="yt_id")

    class Meta(BaseSerializer.Meta):
        model = YTVideoStatistic
        group_by = ("ad_group__id", "yt_id")


class AudienceTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.USER_INTEREST.value)
    type_name = CharField(default=CriteriaTypeEnum.USER_INTEREST.name)
    target_name = CharField(source="audience__name")
    criteria = IntegerField(source="audience_id")

    class Meta(BaseSerializer.Meta):
        model = AudienceStatistic
        group_by = ("ad_group__id", "audience_id", "audience__name", "audience__type")


class RemarketTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.USER_LIST.value)
    type_name = CharField(default=CriteriaTypeEnum.USER_LIST.name)
    target_name = CharField(source="remark__name")
    criteria = IntegerField(source="remark_id")

    class Meta(BaseSerializer.Meta):
        model = RemarkStatistic
        group_by = ("remark__name", "remark_id")


class ParentTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.PARENT.value)
    type_name = CharField(default=CriteriaTypeEnum.PARENT.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source="parent_status_id")

    class Meta(BaseSerializer.Meta):
        model = ParentStatistic
        group_by = ("parent_status_id",)

    def get_target_name(self, obj):
        parent = parent_str(obj["parent_status_id"])
        return parent


class DeviceTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.DEVICE.value)
    type_name = CharField(default=CriteriaTypeEnum.DEVICE.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source="device_id")

    class Meta(BaseSerializer.Meta):
        model = AdGroupStatistic
        group_by = ("device_id",)

    def get_target_name(self, obj):
        device = device_str(obj["device_id"])
        return device


class VideoCreativeTargetingSerializer(BaseSerializer):
    type = IntegerField(default=CriteriaTypeEnum.VIDEO_CREATIVE.value)
    type_name = CharField(default=CriteriaTypeEnum.VIDEO_CREATIVE.name)
    criteria = CharField(source="creative_id")
    target_name = CharField(source="creative_id")

    class Meta(BaseSerializer.Meta):
        model = VideoCreativeStatistic
        group_by = ("creative_id",)
