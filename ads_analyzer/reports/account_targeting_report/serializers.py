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
    criteria_field = "age_range_id"
    type_id = CriteriaTypeEnum.AGE_RANGE.value

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.AGE_RANGE.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = AgeRangeStatistic
        group_by = ("ad_group_id", "age_range_id")

    def get_target_name(self, obj):
        age_range = age_range_str(obj["age_range_id"])
        return age_range

    def get_criteria(self, obj):
        pass


class GenderTargetingSerializer(BaseSerializer):
    criteria_field = "gender_id"
    type_id = CriteriaTypeEnum.GENDER.value

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.GENDER.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = GenderStatistic
        group_by = ("ad_group_id", "gender_id")

    def get_target_name(self, obj):
        gender = gender_str(obj["gender_id"])
        return gender


class KeywordTargetingSerializer(BaseSerializer):
    criteria_field = "keyword"
    type_id = CriteriaTypeEnum.KEYWORD.value

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.KEYWORD.name)
    target_name = CharField(source="keyword")
    criteria = CharField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = KeywordStatistic
        group_by = ("ad_group_id", "keyword")


class TopicTargetingSerializer(BaseSerializer):
    criteria_field = "topic__id"
    type_id = CriteriaTypeEnum.VERTICAL.value

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.VERTICAL.name)
    target_name = CharField(source="topic__name")
    topic_id = IntegerField(source="topic__id")
    criteria = IntegerField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = TopicStatistic
        fields = BaseSerializer.Meta.fields + ("topic_id",)
        group_by = ("topic__id", "topic__name")


class PlacementChannelTargetingSerializer(BaseSerializer):
    type_id = CriteriaTypeEnum.PLACEMENT.value
    criteria_field = "yt_id"

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.YOUTUBE_CHANNEL.name)
    target_name = CharField(source="yt_id")
    criteria = CharField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = YTChannelStatistic
        group_by = ("ad_group_id", "yt_id")


class PlacementVideoTargetingSerializer(BaseSerializer):
    type_id = CriteriaTypeEnum.PLACEMENT.value
    criteria_field = "yt_id"

    type = IntegerField(default=CriteriaTypeEnum.YOUTUBE_VIDEO.value)
    type_name = CharField(default=CriteriaTypeEnum.YOUTUBE_VIDEO.name)
    target_name = CharField(source="yt_id")
    criteria = CharField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = YTVideoStatistic
        group_by = ("ad_group_id", "yt_id")


class AudienceTargetingSerializer(BaseSerializer):
    type_id = CriteriaTypeEnum.USER_INTEREST.value
    criteria_field = "audience_id"

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.USER_INTEREST.name)
    target_name = CharField(source="audience__name")
    criteria = IntegerField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = AudienceStatistic
        group_by = ("ad_group_id", "audience_id", "audience__name", "audience__type")


class RemarketTargetingSerializer(BaseSerializer):
    type_id = CriteriaTypeEnum.USER_LIST.value
    criteria_field = "remark_id"

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.USER_LIST.name)
    target_name = CharField(source="remark__name")
    criteria = IntegerField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = RemarkStatistic
        group_by = ("remark__name", "remark_id")


class ParentTargetingSerializer(BaseSerializer):
    type_id = CriteriaTypeEnum.PARENT.value
    criteria_field = "parent_status_id"

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.PARENT.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = ParentStatistic
        group_by = ("parent_status_id",)

    def get_target_name(self, obj):
        parent = parent_str(obj["parent_status_id"])
        return parent


class DeviceTargetingSerializer(BaseSerializer):
    type_id = CriteriaTypeEnum.DEVICE.value
    criteria_field = "device_id"

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.DEVICE.name)
    target_name = SerializerMethodField()
    criteria = IntegerField(source=criteria_field)

    class Meta(BaseSerializer.Meta):
        model = AdGroupStatistic
        group_by = ("device_id",)

    def get_target_name(self, obj):
        device = device_str(obj["device_id"])
        return device


class VideoCreativeTargetingSerializer(BaseSerializer):
    type_id = CriteriaTypeEnum.VIDEO_CREATIVE.value
    criteria_field = "creative_id"

    type = IntegerField(default=type_id)
    type_name = CharField(default=CriteriaTypeEnum.VIDEO_CREATIVE.name)
    criteria = CharField(source=criteria_field)
    target_name = CharField(source="creative_id")

    class Meta(BaseSerializer.Meta):
        model = VideoCreativeStatistic
        group_by = ("creative_id",)
