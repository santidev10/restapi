from collections import defaultdict

from django.db.models import Count

from aw_creation.api.serializers.analytics.base_account_creation_serializer import BaseAccountCreationSerializer
from aw_creation.api.serializers.common.struck_field import StruckField
from aw_creation.models import AccountCreation


class AccountCreationPerformanceTargetingListSerializer(BaseAccountCreationSerializer):
    ad_count = StruckField()
    channel_count = StruckField()
    video_count = StruckField()
    interest_count = StruckField()
    topic_count = StruckField()
    keyword_count = StruckField()

    def __init__(self, *args, **kwargs):
        super(AccountCreationPerformanceTargetingListSerializer, self).__init__(*args, **kwargs)
        self.struck = {}
        ids = self._get_ids(*args, **kwargs)
        if ids:
            self.struck = self._get_struck(ids)

    def _get_struck(self, account_creation_ids):
        annotates = dict(
            ad_count=Count("account__campaigns__ad_groups__ads",
                           distinct=True),
            channel_count=Count(
                "account__campaigns__ad_groups__channel_statistics__yt_id",
                distinct=True),
            video_count=Count(
                "account__campaigns__ad_groups__managed_video_statistics__yt_id",
                distinct=True),
            interest_count=Count(
                "account__campaigns__ad_groups__audiences__audience_id",
                distinct=True),
            topic_count=Count(
                "account__campaigns__ad_groups__topics__topic_id",
                distinct=True),
            keyword_count=Count(
                "account__campaigns__ad_groups__keywords__keyword",
                distinct=True),
        )
        struck = defaultdict(dict)
        for annotate, aggr in annotates.items():
            struck_data = AccountCreation.objects \
                .filter(id__in=account_creation_ids) \
                .values("id") \
                .order_by("id") \
                .annotate(**{annotate: aggr})
            for d in struck_data:
                struck[d['id']][annotate] = d[annotate]
        return struck

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
