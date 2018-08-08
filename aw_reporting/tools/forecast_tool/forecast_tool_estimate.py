from collections import defaultdict
from operator import itemgetter

from django.db.models import FloatField
from django.db.models import Q, When, Case, Sum

from aw_reporting.models import Campaign, CampaignStatistic, AdGroupStatistic, get_average_cpv, get_average_cpm

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


class ForecastToolEstimate:
    CPV_BUFFER = 0.015
    CPM_BUFFER = 2

    def __init__(self, kwargs, opportunities):
        self.kwargs = kwargs
        self.opportunities = opportunities

    def estimate(self):
        queryset = self._get_ad_group_statistic_queryset()
        summary = queryset.aggregate(
            **AD_GROUP_COSTS_ANNOTATE)
        average_cpv = get_average_cpv(cost=summary["views_cost"], **summary)
        if average_cpv is not None:
            average_cpv += self.CPV_BUFFER
        average_cpm = get_average_cpm(cost=summary["sum_cost"], **summary)
        if average_cpm is not None:
            average_cpm += self.CPM_BUFFER
        response = dict(
            average_cpv=average_cpv,
            average_cpm=average_cpm,
            charts=self._get_charts(),
        )
        return response

    def _get_ad_group_statistic_queryset(self):
        queryset = AdGroupStatistic.objects.filter(
            ad_group__campaign__salesforce_placement__opportunity__in=self.opportunities,
        )
        queryset = self._filter_out_hidden_data(queryset)
        queryset = self._filter_excluded_items(queryset)
        queryset = self._filter_specified_date_range(queryset)
        return queryset

    def _get_charts(self):
        compare_yoy = self.kwargs.get("compare_yoy")

        queryset = self._get_ad_group_statistic_queryset()
        queryset = queryset.filter(cost__gt=0)

        data = queryset.values('date').order_by('date').annotate(
            **AD_GROUP_COSTS_ANNOTATE)
        cpv_lines = defaultdict(list)
        cpm_lines = defaultdict(list)

        for point in data:
            average_cpv = get_average_cpv(cost=point["views_cost"], **point)
            average_cpm = get_average_cpm(cost=point["sum_cost"], **point)
            date = point['date']

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
