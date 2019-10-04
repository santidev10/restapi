from rest_framework.fields import BooleanField
from rest_framework.fields import CharField
from rest_framework.fields import DateField
from rest_framework.fields import FloatField
from rest_framework.fields import IntegerField
from rest_framework.fields import ReadOnlyField
from rest_framework.serializers import ModelSerializer

from aw_reporting.models import KeywordStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import goal_type_str


class GoalTypeField(CharField):
    def to_representation(self, goal_type_id):
        goal_type = goal_type_str(goal_type_id)
        return super(GoalTypeField, self).to_representation(goal_type)


class TargetTableSerializer(ModelSerializer):
    name = ReadOnlyField(default="N/A")
    type = ReadOnlyField(default="N/A")
    campaign_name = CharField(source="ad_group__campaign__name")
    ad_group_name = CharField(source="ad_group__name")
    placement_name = CharField(source="ad_group__campaign__salesforce_placement__name")
    placement_start = DateField(source="ad_group__campaign__salesforce_placement__start")
    placement_end = DateField(source="ad_group__campaign__salesforce_placement__end")
    margin_cap = ReadOnlyField(default="N/A")
    cannot_roll_over = BooleanField(source="ad_group__campaign__salesforce_placement__opportunity__cannot_roll_over")
    rate_type = GoalTypeField(source="ad_group__campaign__salesforce_placement__goal_type_id")
    contracted_rate = FloatField(source="ad_group__campaign__salesforce_placement__ordered_rate")
    impressions = IntegerField(source="sum_impressions")
    video_views = IntegerField(source="sum_video_views")
    clicks = IntegerField(source="sum_clicks")
    cost = FloatField(source="sum_cost")

    class Meta:
        model = None
        fields = (
            "name",
            "type",
            "campaign_name",
            "ad_group_name",
            "placement_name",
            "placement_start",
            "placement_end",
            "margin_cap",
            "cannot_roll_over",
            "rate_type",
            "contracted_rate",
            "impressions",
            "video_views",
            "clicks",
            "cost",
        )


class TargetTableTopicSerializer(TargetTableSerializer):
    name = CharField(source="topic__name")
    type = ReadOnlyField(default="Topic")

    class Meta(TargetTableSerializer.Meta):
        model = TopicStatistic


class TargetTableKeywordSerializer(TargetTableSerializer):
    name = CharField(source="keyword")
    type = ReadOnlyField(default="Keyword")

    class Meta(TargetTableSerializer.Meta):
        model = KeywordStatistic
