import logging
from collections import defaultdict

from django.http import Http404
from rest_framework.response import Response
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_creation.models import TargetingItem
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import BASE_STATS
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import base_stats_aggregator
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import dict_norm_base_stats
from es_components.managers import ChannelManager
from es_components.managers import VideoManager

logger = logging.getLogger(__name__)


class PerformanceTargetingReportAPIView(APIView):
    channel_manager = ChannelManager()
    es_fields_to_load_channel_info = ("main.id", "general_data.title", "general_data.thumbnail_image_url",)

    video_manager = VideoManager()
    es_fields_to_load_video_info = ("main.id", "general_data.title", "general_data.thumbnail_image_url",)

    def get_object(self):
        pk = self.kwargs["pk"]
        user = self.request.user
        try:
            item = AccountCreation.objects.user_related(user).get(pk=pk)
        except AccountCreation.DoesNotExist:
            raise Http404
        else:
            return item

    def get_items(self, targeting, account):
        items = []
        for target in targeting:
            method = "get_%s_items" % target
            if hasattr(self, method):
                items.extend(getattr(self, method)(account))
        return items

    def get_negative_targeting_items(self, targeting, account_creation):
        queryset = TargetingItem.objects.filter(
            ad_group_creation__campaign_creation__account_creation=account_creation,
            ad_group_creation__ad_group__isnull=False,
            type__in=targeting,
            is_negative=True,
        )
        data = self.request.data
        if data.get("ad_groups"):
            queryset = queryset.filter(
                ad_group_creation__ad_group_id__in=data["ad_groups"])
        if data.get("campaigns"):
            queryset = queryset.filter(
                ad_group_creation__ad_group__campaign_id__in=data["campaigns"])

        items = defaultdict(lambda: defaultdict(set))
        for e in queryset.values("type", "criteria",
                                 "ad_group_creation__ad_group_id"):
            items[
                e["type"]
            ][
                e["ad_group_creation__ad_group_id"]
            ].add(
                e["criteria"]
            )
        return items

    def post(self, request, **_):
        item = self.get_object()
        group_by = request.data.get("group_by", "")
        targeting = request.data.get("targeting", [])
        items = self.get_items(targeting, item.account)
        negative_items = self.get_negative_targeting_items(targeting, item)

        reports = []
        if group_by == "campaign":
            items_by_campaign = defaultdict(list)
            for i in items:
                uid = (i["campaign"]["name"], i["campaign"]["id"])
                items_by_campaign[uid].append(i)
            items_by_campaign = [dict(label=k[0], id=k[1], items=v) for k, v in
                                 items_by_campaign.items()]
            reports.extend(
                sorted(items_by_campaign, key=lambda el: el["label"]))
        else:
            reports.append(dict(label="All campaigns", items=items, id=None))
        for report in reports:
            # get calculated fields
            stat_fields = BASE_STATS + ("video_impressions",)
            summary = {k: 0 for k in stat_fields}
            for i in report["items"]:
                dict_norm_base_stats(i)
                for k, v in i.items():
                    if k in stat_fields and v:
                        summary[k] += v
                dict_add_calculated_stats(i)
                del i["video_impressions"]

                # add status field
                targeting_type = i["targeting"].lower()[:-1]
                ad_group_id = i["ad_group"]["id"]
                i["is_negative"] = str(i["item"]["id"]) in \
                                   negative_items[targeting_type][ad_group_id]

            dict_add_calculated_stats(summary)
            del summary["video_impressions"]
            report.update(summary)

            report["kpi"] = self.get_kpi_limits(report["items"])

        data = dict(
            reports=reports,
        )
        return Response(data=data)

    @staticmethod
    def get_kpi_limits(items):
        kpi = dict(
            average_cpv=[],
            average_cpm=[],
            ctr=[],
            ctr_v=[],
            video_view_rate=[],
        )
        for item in items:
            for key, values in kpi.items():
                value = item[key]
                if value is not None:
                    values.append(value)

        kpi_limits = dict()
        for key, values in kpi.items():
            kpi_limits[key] = dict(min=min(values) if values else None,
                                   max=max(values) if values else None)
        return kpi_limits

    def filter_queryset(self, qs):
        data = self.request.data
        if data.get("ad_groups"):
            qs = qs.filter(ad_group_id__in=data["ad_groups"])
        if data.get("campaigns"):
            qs = qs.filter(ad_group__campaign_id__in=data["campaigns"])
        if data.get("start_date"):
            qs = qs.filter(date__gte=data["start_date"])
        if data.get("end_date"):
            qs = qs.filter(date__lte=data["end_date"])
        return qs

    _annotate = base_stats_aggregator("ad_group__")
    _values = ("ad_group__id", "ad_group__name", "ad_group__campaign__id",
               "ad_group__campaign__name",
               "ad_group__campaign__status")

    @staticmethod
    def _set_group_and_campaign_fields(el):
        el["ad_group"] = dict(id=el["ad_group__id"], name=el["ad_group__name"])
        el["campaign"] = dict(id=el["ad_group__campaign__id"],
                              name=el["ad_group__campaign__name"],
                              status=el["ad_group__campaign__status"])
        del el["ad_group__id"], el["ad_group__name"], el[
            "ad_group__campaign__id"]
        del el["ad_group__campaign__name"], el["ad_group__campaign__status"]

    def get_channel_items(self, account):
        qs = YTChannelStatistic.objects.filter(
            ad_group__campaign__account=account
        )
        qs = self.filter_queryset(qs)
        items = qs.values("yt_id", *self._values) \
            .order_by("yt_id", "ad_group_id") \
            .annotate(**self._annotate)

        info = {}
        ids = {i["yt_id"] for i in items}
        if ids:
            try:
                items = self.channel_manager.search(
                    filters=self.channel_manager.ids_query(ids)
                ). \
                    source(includes=list(self.es_fields_to_load_channel_info)).execute().hits
                info = {r.main.id: r for r in items}
            except BaseException as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i["yt_id"])
            i["item"] = dict(id=i["yt_id"],
                             name=item_details.general_data.title if item_details else i["yt_id"],
                             thumbnail=item_details.general_data.thumbnail_image_url if item_details else i["yt_id"])
            del i["yt_id"]

            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Channels"
        return items

    def get_video_items(self, account):
        qs = YTVideoStatistic.objects.filter(
            ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("yt_id", *self._values) \
            .order_by("yt_id", "ad_group_id") \
            .annotate(**self._annotate)

        info = {}
        ids = {i["yt_id"] for i in items}
        if ids:
            try:
                items = self.video_manager.search(
                    filters=self.video_manager.ids_query(ids)
                ). \
                    source(includes=list(self.es_fields_to_load_video_info)).execute().hits
                info = {r.main.id: r for r in items}
            except BaseException as e:
                logger.error(e)

        for i in items:
            item_details = info.get(i["yt_id"])
            i["item"] = dict(id=i["yt_id"],
                             name=item_details.general_data.title if item_details else i["yt_id"],
                             thumbnail=item_details.general_data.thumbnail_image_url if item_details else None)
            del i["yt_id"]

            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Videos"
        return items

    def get_keyword_items(self, account):
        qs = KeywordStatistic.objects.filter(
            ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("keyword", *self._values) \
            .order_by("keyword", "ad_group_id") \
            .annotate(**self._annotate)

        for i in items:
            i["item"] = dict(id=i["keyword"], name=i["keyword"])
            del i["keyword"]
            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Keywords"
        return items

    def get_interest_items(self, account):
        qs = AudienceStatistic.objects.filter(
            ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("audience__id", "audience__name", *self._values) \
            .order_by("audience__id", "ad_group_id") \
            .annotate(**self._annotate)

        for i in items:
            i["item"] = dict(id=i["audience__id"], name=i["audience__name"])
            del i["audience__id"], i["audience__name"]
            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Interests"
        return items

    def get_topic_items(self, account):
        qs = TopicStatistic.objects.filter(ad_group__campaign__account=account)
        qs = self.filter_queryset(qs)
        items = qs.values("topic__id", "topic__name", *self._values).order_by(
            "topic__id", "ad_group_id").annotate(**self._annotate)

        for i in items:
            i["item"] = dict(id=i["topic__id"], name=i["topic__name"])
            del i["topic__id"], i["topic__name"]
            self._set_group_and_campaign_fields(i)
            i["targeting"] = "Topics"
        return items
