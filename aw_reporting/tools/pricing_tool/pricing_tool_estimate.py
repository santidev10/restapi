from collections import defaultdict
from datetime import timedelta
from functools import reduce
from operator import itemgetter

from django.db.models import BooleanField
from django.db.models import Case
from django.db.models import FloatField
from django.db.models import Max
from django.db.models import Q
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When

from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import OpPlacement
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import get_average_cpm
from aw_reporting.models import get_average_cpv

AD_GROUP_COSTS_ANNOTATE = dict(
    sum_cost=Sum("cost"),
    views_cost=Sum(
        Case(
            When(
                ad_group__video_views__gt=0,
                then="cost",
            ),
            output_field=FloatField(),
        )
    ),
    impressions=Sum("impressions"),
    video_views=Sum("video_views"),
)


class PricingToolEstimate:
    def __init__(self, kwargs, opportunities=None):
        self.kwargs = kwargs
        self.set_opportunities(opportunities or [])

    def set_opportunities(self, opportunities):
        self.opportunities = []
        self.campaigns_ids = []
        for opportunity_data in opportunities:
            _id, campaign_ids = opportunity_data
            self.opportunities.append(_id)
            self.campaigns_ids.extend(campaign_ids)

    def estimate(self):
        queryset = self._get_ad_group_statistic_queryset()
        summary = queryset.aggregate(
            **AD_GROUP_COSTS_ANNOTATE)
        average_cpv = get_average_cpv(cost=summary["views_cost"], **summary)
        average_cpm = get_average_cpm(cost=summary["sum_cost"], **summary)
        margin = self.kwargs["margin"]

        margin_rate = 1 - margin / 100
        response = dict(
            average_cpv=average_cpv, average_cpm=average_cpm,
            suggested_cpv=average_cpv / margin_rate if average_cpv and margin_rate else None,
            suggested_cpm=average_cpm / margin_rate if average_cpm and margin_rate else None,
            margin=margin,
            charts=self._get_charts(),
        )
        return response

    def _get_ad_group_statistic_queryset(self):
        """
        Retrieve ad group statistics to aggregate based on filters
        :return:
        """
        try:
            product_types = self.kwargs["product_types"]
            queryset = AdGroupStatistic.objects.filter(
                ad_group__type__in=product_types,
                ad_group__campaign_id__in=self.campaigns_ids
            )
        except KeyError:
            queryset = AdGroupStatistic.objects.filter(
                ad_group__campaign_id__in=self.campaigns_ids,
            )
        queryset = self._filter_out_hidden_data(queryset)
        queryset = self._filter_excluded_items(queryset)
        queryset = self._filter_specified_date_range(queryset)
        return queryset

    def _get_charts(self):
        compare_yoy = self.kwargs.get("compare_yoy")

        queryset = self._get_ad_group_statistic_queryset()
        queryset = queryset.filter(cost__gt=0)

        data = queryset.values("date").order_by("date").annotate(
            **AD_GROUP_COSTS_ANNOTATE).values("date", *AD_GROUP_COSTS_ANNOTATE.keys())
        cpv_lines = defaultdict(list)
        cpm_lines = defaultdict(list)

        for point in data:
            average_cpv = get_average_cpv(cost=point["views_cost"], **point)
            average_cpm = get_average_cpm(cost=point["sum_cost"], **point)
            date = point["date"]

            if average_cpv:
                if compare_yoy:
                    line = "{}".format(date.year)
                else:
                    line = "CPV"

                cpv_lines[line].append(
                    dict(
                        label=date, value=average_cpv
                    )
                )

            if average_cpm:
                if compare_yoy:
                    line = "{}".format(date.year)
                else:
                    line = "CPM"

                cpm_lines[line].append(
                    dict(
                        label=date, value=average_cpm
                    )
                )

        planned_cpm, planned_cpv = self._get_planned_rates()
        cpm_lines.update(planned_cpm)
        cpv_lines.update(planned_cpv)

        cpv_chart = cpm_chart = None
        if cpv_lines:
            cpv_chart = dict(
                data=list(sorted(
                    (dict(label=label, trend=trend) for label, trend in
                     cpv_lines.items()),
                    key=itemgetter("label"),
                )),
                title="CPV",
            )
        if cpm_lines:
            cpm_chart = dict(
                data=list(sorted(
                    (dict(label=label, trend=trend) for label, trend in
                     cpm_lines.items()),
                    key=itemgetter("label"),
                )),
                title="CPM",
            )
        return dict(cpv=cpv_chart, cpm=cpm_chart)

    def _get_planned_rates(self):
        periods = self.kwargs["periods"]
        compare_yoy = self.kwargs.get("compare_yoy", False)

        if len(periods) == 0:
            return [], []
        placements_data = self._get_placements_queryset() \
            .values("start", "end", "total_cost", "ordered_units",
                    "goal_type_id")

        return _planned_stats(placements_data, periods, compare_yoy)

    def _get_placements_queryset(self):
        periods = self.kwargs["periods"]
        date_filter = reduce(lambda r, p: r | Q(start__lte=p[1], end__gte=p[0]),
                             periods,
                             Q())
        queryset = OpPlacement.objects \
            .filter(date_filter,
                    opportunity__in=self.opportunities)

        exclude_campaigns = self.kwargs.get("exclude_campaigns")
        if exclude_campaigns is not None:
            safe_exclude = exclude_campaigns or [-1]
            queryset = queryset.annotate(campaign_count=Max(
                Case(When(~Q(adwords_campaigns__id__in=safe_exclude),
                          then=Value(1)),
                     output_field=BooleanField(),
                     default=Value(0))))
            queryset = queryset.filter(campaign_count=Value(1))

        exclude_opportunities = self.kwargs.get("exclude_opportunities")
        if exclude_opportunities is not None:
            queryset = queryset.exclude(
                opportunity_id__in=exclude_opportunities)
        return queryset

    def _filter_specified_date_range(self, queryset):
        periods = self.kwargs["periods"]
        query = Q()

        if queryset.model is Campaign:
            for period_start, period_end in periods:
                query |= Q(
                    max_stat_date__gte=period_start,
                    min_stat_date__lte=period_end,
                )

        else:
            for period_start, period_end in periods:
                query |= Q(
                    date__gte=period_start, date__lte=period_end,
                )

        queryset = queryset.filter(query)
        return queryset

    @staticmethod
    def _filter_out_hidden_data(queryset):
        if queryset.model is CampaignStatistic:
            key = "campaign__salesforce_placement__goal_type_id"
        else:
            key = "ad_group__campaign__salesforce_placement__goal_type_id"
        queryset = queryset.exclude(
            date__gte="2015-01-01", date__lt="2015-07-01", **{key: 1}
        )
        return queryset

    def _filter_excluded_items(self, queryset):
        exclude_campaigns = self.kwargs.get("exclude_campaigns")
        if exclude_campaigns:
            queryset = queryset.exclude(
                ad_group__campaign_id__in=exclude_campaigns)

        exclude_opportunities = self.kwargs.get("exclude_opportunities")
        if exclude_opportunities:
            opportunity_id_ref = "ad_group__campaign__salesforce_placement" \
                                 "__opportunity_id__in"
            queryset = queryset.exclude(
                **{opportunity_id_ref: exclude_opportunities})

        return queryset


def _planned_stats(all_placements, periods, compare_yoy=False):
    planned_cpm = defaultdict(list)
    planned_cpv = defaultdict(list)
    for start, end in periods:
        days = (end - start).days + 1
        for day in range(days):
            dt = start + timedelta(days=day)
            cpm, cpv = _planned_stats_for_date(all_placements, dt)
            if cpm is not None:
                label = _stats_label(dt, compare_yoy, "CPM")
                planned_cpm[label].append(dict(label=dt, value=cpm))

            if cpv is not None:
                label = _stats_label(dt, compare_yoy, "CPV")
                planned_cpv[label].append(dict(label=dt, value=cpv))

    return planned_cpm, planned_cpv


def _stats_label(dt, compare_yoy, default):
    suffix = dt.year if compare_yoy else default
    return "Planned {}".format(suffix)


def _planned_stats_for_date(all_placements, dt):
    placements = [p for p in all_placements
                  if p["start"] <= dt <= p["end"]]
    cpm_cost = sum([p["total_cost"] for p in placements
                    if p["goal_type_id"] == SalesForceGoalType.CPM])
    cpv_cost = sum([p["total_cost"] for p in placements
                    if p["goal_type_id"] == SalesForceGoalType.CPV])
    cpm_units = sum([p["ordered_units"] for p in placements
                     if p["goal_type_id"] == SalesForceGoalType.CPM])
    cpv_units = sum([p["ordered_units"] for p in placements
                     if p["goal_type_id"] == SalesForceGoalType.CPV])
    planned_cpm = cpm_cost * 1000. / cpm_units if cpm_units != 0 else None
    planned_cpv = cpv_cost * 1. / cpv_units if cpv_units != 0 else None
    return planned_cpm, planned_cpv
