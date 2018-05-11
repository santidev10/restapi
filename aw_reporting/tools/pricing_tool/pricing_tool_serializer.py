from collections import defaultdict
from functools import reduce

from django.db.models import FloatField
from django.db.models import Q, F, Min, Value, When, Case, Max, \
    BooleanField, Sum, IntegerField

from aw_reporting.models import Campaign, Opportunity, AgeRanges, Genders, \
    SalesForceGoalType, Devices, VideoCreative, get_margin, GeoTarget, \
    CampaignStatistic, AdGroup
from aw_reporting.tools.pricing_tool.constants import TARGETING_TYPES, \
    AGE_FIELDS, GENDER_FIELDS, DEVICE_FIELDS
from utils.datetime import as_date, now_in_default_tz
from utils.query import merge_when


class PricingToolSerializer:
    def __init__(self, kwargs):
        self.kwargs = kwargs

    def get_opportunities_data(self, opportunities: list):
        ids = [opp.id for opp in opportunities]

        campaign_thumbs = self._get_campaign_thumbnails(ids)

        campaign_groups = self._prepare_campaigns(ids)
        opportunities_annotated = Opportunity.objects.filter(id__in=ids) \
            .annotate(**self._opportunity_annotation())
        return [
            self._get_opportunity_data(opp, campaign_groups[opp.id],
                                       campaign_thumbs)
            for opp in opportunities_annotated]

    def _get_opportunity_data(self, opportunity, campaigns, campaign_thumbs):
        periods = self.kwargs.get("periods", [])
        date_filter = statistic_date_filter(periods)
        date_f = reduce(lambda x, f: x | Q(**f), date_filter, Q())
        stats_queryset = CampaignStatistic.objects \
            .filter(campaign__salesforce_placement__opportunity=opportunity) \
            .filter(date_f) \
            .annotate(
            ordered_rate=F("campaign__salesforce_placement__ordered_rate"))
        total_statistic = stats_queryset.aggregate(
            aw_clicks=Sum("clicks"),
            video_views_100_quartile=Sum("video_views_100_quartile")
        )
        cpv_stats = stats_queryset \
            .filter(
            campaign__salesforce_placement__goal_type_id=SalesForceGoalType.CPV) \
            .annotate(cpv_client_cost=F("video_views") * F("ordered_rate")) \
            .aggregate(aw_impressions=Sum("impressions"),
                       aw_video_views=Sum("video_views"),
                       cpv_client_cost_sum=Sum("cpv_client_cost",
                                               output_field=FloatField()),
                       aw_cpv_cost=Sum("cost"))
        cpm_stats = stats_queryset \
            .filter(
            campaign__salesforce_placement__goal_type_id=SalesForceGoalType.CPM) \
            .annotate(
            cpm_client_cost=F("impressions") / Value(1000.) * F("ordered_rate")) \
            .aggregate(aw_impressions=Sum("impressions"),
                       cpm_client_cost_sum=Sum("cpm_client_cost",
                                               output_field=FloatField()),
                       aw_cpm_cost=Sum("cost"))
        cpm_client_cost = cpm_stats["cpm_client_cost_sum"] or 0
        cpv_client_cost = cpv_stats["cpv_client_cost_sum"] or 0
        aw_cpv_cost = cpv_stats["aw_cpv_cost"] or 0
        aw_cpm_cost = cpm_stats["aw_cpm_cost"] or 0
        sf_cpv_cost = opportunity.sf_cpv_cost or 0
        sf_cpm_cost = opportunity.sf_cpm_cost or 0
        sf_hard_cost_total_cost = opportunity.sf_hard_cost_total_cost or 0
        sf_hard_cost_our_cost = opportunity.sf_hard_cost_our_cost or 0
        sf_cpv_units = opportunity.sf_cpv_units or 0
        sf_cpm_units = opportunity.sf_cpm_units or 0
        sf_cpm = sf_cpm_cost / sf_cpm_units * 1000 if sf_cpm_units > 0 else None
        sf_cpv = sf_cpv_cost / sf_cpv_units if sf_cpv_units > 0 else None
        aw_cost = aw_cpm_cost + aw_cpv_cost + sf_hard_cost_our_cost

        client_cost = cpv_client_cost + cpm_client_cost + sf_hard_cost_total_cost
        total_cost = sf_cpv_cost + sf_cpm_cost + sf_hard_cost_total_cost

        cpm_impressions = cpm_stats["aw_impressions"] or 0
        cpv_impressions = cpv_stats["aw_impressions"] or 0

        total_clicks = total_statistic["aw_clicks"] or 0
        total_impressions = cpm_impressions + cpv_impressions
        cpv_video_views = cpv_stats["aw_video_views"] or 0

        ctr = (total_clicks / total_impressions) * 100 \
            if total_impressions else 0
        ctr_v = (total_clicks / cpv_video_views) * 100 if cpv_video_views else 0
        view_rate = (cpv_video_views / cpv_impressions) * 100 \
            if cpv_impressions else 0
        average_cpm = aw_cpm_cost / cpm_impressions * 1000 \
            if cpm_impressions != 0 else 0
        average_cpv = aw_cpv_cost / cpv_video_views \
            if cpv_video_views != 0 else 0
        targeting = [t for t in TARGETING_TYPES
                     if getattr(opportunity, "has_" + t)]
        ages = set([AgeRanges[i] for i, a in enumerate(AGE_FIELDS) if
                    getattr(opportunity, a)])
        genders = set([Genders[i] for i, a in enumerate(GENDER_FIELDS) if
                       getattr(opportunity, a)])
        devices = set([Devices[i] for i, d in enumerate(DEVICE_FIELDS)
                       if getattr(opportunity, d)])
        margin = get_margin(plan_cost=total_cost, cost=aw_cost,
                            client_cost=client_cost)

        if margin is not None:
            margin *= 100

        start_date = opportunity.start_date
        end_date = opportunity.end_date
        relevant_date_range = self._get_relevant_date_range(start_date,
                                                            end_date)

        ad_group_types = AdGroup.objects.filter(
            campaign__salesforce_placement__opportunity=opportunity) \
            .values_list("type", flat=True) \
            .order_by() \
            .distinct()
        creative = VideoCreative.objects.filter(
            statistics__ad_group__campaign__salesforce_placement__opportunity=opportunity) \
            .values_list("duration", flat=True) \
            .order_by() \
            .distinct()

        geographic = GeoTarget.objects.filter(
            geo_performance__campaign__salesforce_placement__opportunity=opportunity) \
            .values_list("name", flat=True) \
            .order_by() \
            .distinct()

        video_views_100_quartile = total_statistic["video_views_100_quartile"] \
                                   or 0
        video100rate = video_views_100_quartile * 100. / total_impressions \
            if total_impressions > 0 else 0

        return dict(
            id=opportunity.id,
            name=opportunity.name,
            brand=opportunity.brand,
            vertical=opportunity.category_id,
            apex_deal=opportunity.apex_deal,
            campaigns=[self._get_campaign_data(c, campaign_thumbs) for c in
                       campaigns],
            products=list(ad_group_types),
            targeting=targeting,
            demographic=ages | genders,
            creative_lengths=list(creative),
            average_cpv=average_cpv,
            average_cpm=average_cpm,
            margin=margin,
            start_date=start_date,
            end_date=end_date,
            budget=aw_cost,
            devices=devices,
            relevant_date_range=relevant_date_range,
            sf_cpm=sf_cpm,
            sf_cpv=sf_cpv,
            geographic=list(geographic),
            ctr=ctr,
            ctr_v=ctr_v,
            view_rate=view_rate,
            video100rate=video100rate
        )

    def _get_campaign_data(self, campaign, campaign_thumbs):
        campaign_id = campaign["id"]
        cost = campaign["cost"]

        impressions = campaign["impressions"] or 0
        video_views = campaign["video_views"] or 0
        clicks = campaign["clicks"] or 0

        ctr = (clicks / video_views) * 100 if video_views else 0
        ctr_v = (clicks / impressions) * 100 if impressions else 0
        average_cpm = cost / impressions * 1000 if impressions > 0 else 0
        average_cpv = cost / video_views if video_views > 0 else 0

        goal_type_id = campaign["salesforce_placement__goal_type_id"]
        is_cpm = goal_type_id == SalesForceGoalType.CPM
        is_cpv = goal_type_id == SalesForceGoalType.CPV
        ordered_rate = campaign["salesforce_placement__ordered_rate"] or 0
        if is_cpm:
            client_cost = ordered_rate * impressions / 1000.
        elif is_cpv:
            client_cost = ordered_rate * video_views
        else:
            client_cost = 0
        margin = get_margin(cost=cost, client_cost=client_cost, plan_cost=None)

        devices = set([Devices[i] for i, d in enumerate(DEVICE_FIELDS)
                       if campaign[d]])
        targeting = [t for t in TARGETING_TYPES
                     if campaign["has_" + t]]

        ages = [AgeRanges[i] for i, a in enumerate(AGE_FIELDS)
                if campaign[a]]
        genders = [Genders[i] for i, a in enumerate(GENDER_FIELDS)
                   if campaign[a]]

        thumbnail = campaign_thumbs.get(campaign_id, None)

        campaign_start, campaign_end = get_campaign_start_end(campaign)
        relevant_date_range = self._get_relevant_date_range(campaign_start,
                                                            campaign_end)

        ad_group_types = AdGroup.objects.filter(
            campaign_id=campaign_id) \
            .values_list("type", flat=True) \
            .order_by() \
            .distinct()
        creative = VideoCreative.objects.filter(
            statistics__ad_group__campaign_id=campaign_id) \
            .values_list("duration", flat=True) \
            .order_by() \
            .distinct()

        geo_targeting = GeoTarget.objects.filter(
            geo_performance__campaign_id=campaign_id) \
            .values_list("name", flat=True) \
            .order_by() \
            .distinct()

        campaign_data = dict(
            id=campaign_id,
            budget=cost,
            name=campaign["name"],
            thumbnail=thumbnail,
            creative_lengths=list(creative),
            products=list(ad_group_types),
            devices=devices,
            demographic=ages + genders,
            targeting=targeting,
            cost=cost,
            average_cpm=average_cpm,
            average_cpv=average_cpv,
            margin=margin,
            start_date=campaign["start_date"],
            end_date=campaign["end_date"],
            vertical=campaign["salesforce_placement__opportunity__category_id"],
            geographic=list(geo_targeting),
            brand=campaign["salesforce_placement__opportunity__brand"],
            apex_deal=campaign["salesforce_placement__opportunity__apex_deal"],
            sf_cpm=ordered_rate if is_cpm else None,
            sf_cpv=ordered_rate if is_cpv else None,
            relevant_date_range=relevant_date_range,
            ctr=ctr,
            ctr_v=ctr_v,
        )

        campaign_start, campaign_end = get_campaign_start_end(campaign)
        if None not in [self.kwargs.get("start", None),
                        self.kwargs.get("end", None),
                        campaign_start, campaign_end]:
            campaign_data["relevant_date_range"] = dict(
                start=max(as_date(self.kwargs['start']), campaign_start),
                end=min(as_date(self.kwargs['end']), campaign_end)
            )
        return campaign_data

    def _get_relevant_date_range(self, start, end):
        starts = [as_date(s) for s in [start, self.kwargs.get("start")]
                  if s is not None]
        ends = [as_date(e) for e in [end, self.kwargs.get("end")]
                if e is not None]
        relevant_start = max(starts) if len(starts) > 0 else None
        relevant_end = min(ends) if len(ends) > 0 else None
        return dict(
            start=relevant_start,
            end=relevant_end
        )

    def _get_campaign_thumbnails(self, opp_ids):
        campaign_id_ref = "statistics__ad_group__campaign_id"
        creatives = VideoCreative.objects \
            .filter(
            statistics__ad_group__campaign__salesforce_placement__opportunity_id__in=opp_ids) \
            .distinct(campaign_id_ref) \
            .values(campaign_id_ref, "id")
        build_thumb = lambda \
                id: "https://i.ytimg.com/vi/{}/hqdefault.jpg".format(id)

        return dict((c[campaign_id_ref], build_thumb(c["id"]))
                    for c in creatives)

    def _opportunity_annotation(self):
        today = now_in_default_tz().date()
        targeting_aggregate = dict(
            ("has_" + t, Max(Case(When(
                **{"placements__adwords_campaigns__has_" + t: True,
                   "then": Value(1)}),
                output_field=BooleanField(),
                default=Value(0))))
            for t in TARGETING_TYPES)
        ages_aggregate = dict(
            (a, Max(Case(When(**{"placements__adwords_campaigns__" + a: True,
                                 "then": Value(1)}),
                         output_field=BooleanField(),
                         default=Value(0))))
            for a in AGE_FIELDS)

        genders_aggregate = dict(
            (a, Max(Case(When(**{"placements__adwords_campaigns__" + a: True,
                                 "then": Value(1)}),
                         output_field=BooleanField(),
                         default=Value(0))))
            for a in GENDER_FIELDS)

        devices_aggregate = dict(
            (a, Max(Case(When(**{"placements__adwords_campaigns__" + a: True,
                                 "then": Value(1)}),
                         output_field=BooleanField(),
                         default=Value(0))))
            for a in DEVICE_FIELDS)
        periods = self.kwargs.get("periods", [])
        placements_date_filter = placement_date_filter(periods)
        flights_date_filter = flight_date_filter(periods, today)
        return dict(
            start_date=Min("placements__adwords_campaigns__start_date"),
            end_date=Max("placements__adwords_campaigns__end_date"),
            sf_cpm_cost=Sum(Case(
                *merge_when(placements_date_filter,
                            placements__goal_type_id=SalesForceGoalType.CPM,
                            then="placements__total_cost"),
                output_field=FloatField(),
                default=0
            )),
            sf_cpv_cost=Sum(Case(
                *merge_when(placements_date_filter,
                            placements__goal_type_id=SalesForceGoalType.CPV,
                            then="placements__total_cost"),
                output_field=FloatField(),
                default=0
            )),
            sf_hard_cost_total_cost=Sum(Case(
                *merge_when(flights_date_filter,
                            placements__goal_type_id=SalesForceGoalType.HARD_COST,
                            then="placements__flights__total_cost"),
                output_field=FloatField(),
                default=0
            )),
            sf_hard_cost_our_cost=Sum(Case(
                *merge_when(flights_date_filter,
                            placements__goal_type_id=SalesForceGoalType.HARD_COST,
                            then="placements__flights__cost"),
                output_field=FloatField(),
                default=0
            )),
            sf_cpm_units=Sum(Case(
                *merge_when(placements_date_filter,
                            placements__goal_type_id=SalesForceGoalType.CPM,
                            then="placements__ordered_units"),
                output_field=IntegerField(),
                default=0
            )),
            sf_cpv_units=Sum(Case(
                *merge_when(placements_date_filter,
                            placements__goal_type_id=SalesForceGoalType.CPV,
                            then="placements__ordered_units"),
                output_field=IntegerField(),
                default=0
            )),
            **targeting_aggregate,
            **ages_aggregate,
            **genders_aggregate,
            **devices_aggregate,
        )

    def _prepare_campaigns(self, opportunity_ids):
        opp_id_key = "salesforce_placement__opportunity_id"
        campaigns = Campaign.objects.visible_campaigns() \
            .filter(**{opp_id_key + "__in": opportunity_ids}) \
            .values("id", opp_id_key, "cost", "impressions", "video_views",
                    "start_date", "end_date", "name",
                    "salesforce_placement__goal_type_id",
                    "salesforce_placement__ordered_rate",
                    "salesforce_placement__opportunity__apex_deal",
                    "salesforce_placement__opportunity__brand",
                    "salesforce_placement__opportunity__category_id",
                    "clicks",
                    *DEVICE_FIELDS,
                    *(("has_" + t) for t in TARGETING_TYPES),
                    *AGE_FIELDS,
                    *GENDER_FIELDS,
                    )
        return reduce(
            lambda r, c: add_to_key(r, c[opp_id_key], c),
            campaigns, defaultdict(list))


def add_to_key(d: defaultdict, key, item):
    d[key] += [item]
    return d


def statistic_date_filter(periods):
    return [dict(
        date__gte=start,
        date__lte=end)
               for start, end in periods
           ] or [dict()]


def get_campaign_start_end(campaign):
    """
    Returns `start_date` and `end_date` from campaign if exists,
    else returns min and max date for the campaign statistic
    """
    campaign_start = campaign.get("start_date", None)
    campaign_end = campaign.get("end_date", None)
    if campaign_start is not None and campaign_end is not None:
        return campaign_start, campaign_end

    statistic = CampaignStatistic.objects \
        .filter(campaign_id=campaign["id"], date__isnull=False) \
        .aggregate(min=Min("date"), max=Max("date"))
    return as_date(campaign_start or statistic["min"]), \
           as_date(campaign_end or statistic["max"])


def placement_date_filter(periods):
    return [dict(
        placements__start__lte=end,
        placements__end__gte=start)
               for start, end in periods
           ] or [dict()]


def flight_date_filter(periods, max_start_date=None):
    return [dict(
        placements__flights__start__lte=min(end, max_start_date or end),
        placements__flights__end__gte=start)
               for start, end in periods
           ] or [dict()]
