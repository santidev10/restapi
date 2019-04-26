from django.db.models import F

from aw_creation.api.serializers.analytics.base_account_creation_serializer import BaseAccountCreationSerializer
from aw_creation.models import AccountCreation
from utils.serializers.fields import ParentDictValueField


class AccountStatisticField(ParentDictValueField):
    def __init__(self):
        super(AccountStatisticField, self).__init__("account_statistic")


class AccountCreationPerformanceTargetingListSerializer(BaseAccountCreationSerializer):
    ad_count = AccountStatisticField()
    channel_count = AccountStatisticField()
    video_count = AccountStatisticField()
    interest_count = AccountStatisticField()
    topic_count = AccountStatisticField()
    keyword_count = AccountStatisticField()

    def __init__(self, *args, **kwargs):
        super(AccountCreationPerformanceTargetingListSerializer, self).__init__(*args, **kwargs)
        self.account_statistic = {}
        ids = self._get_ids(*args, **kwargs)
        if ids:
            self.account_statistic = self._account_statistic(ids)

    def _account_statistic(self, account_creation_ids):
        annotates = dict(
            ad_count=F("account__ad_count"),
            channel_count=F("account__channel_count"),
            video_count=F("account__video_count"),
            interest_count=F("account__interest_count"),
            topic_count=F("account__topic_count"),
            keyword_count=F("account__keyword_count"),
        )
        struck_data = AccountCreation.objects \
            .filter(id__in=account_creation_ids) \
            .annotate(**annotates) \
            .values()
        return {item["id"]: item for item in struck_data}

    class Meta(BaseAccountCreationSerializer.Meta):
        fields = (
            "account",
            "ad_count",
            "average_cpm",
            "average_cpv",
            "channel_count",
            "clicks",
            "cost",
            "ctr",
            "ctr_v",
            "end",
            "from_aw",
            "id",
            "impressions",
            "interest_count",
            "is_changed",
            "is_disapproved",
            "is_editable",
            "is_managed",
            "keyword_count",
            "name",
            "plan_cpm",
            "plan_cpv",
            "start",
            "status",
            "thumbnail",
            "topic_count",
            "updated_at",
            "video_count",
            "video_view_rate",
            "video_views",
            "weekly_chart",
        )
