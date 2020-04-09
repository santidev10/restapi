from functools import reduce

from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import BooleanField
from django.db.models import Case
from django.db.models import ExpressionWrapper
from django.db.models import F
from django.db.models import FloatField
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When

from aw_reporting.models import AdGroup
from aw_reporting.models import AgeRanges
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Genders
from aw_reporting.models import GeoTarget
from aw_reporting.models import Opportunity
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import VideoCreative
from aw_reporting.models import device_str
from aw_reporting.models import get_margin
from aw_reporting.tools.pricing_tool.constants import AGE_FIELDS
from aw_reporting.tools.pricing_tool.constants import DEVICE_FIELDS
from aw_reporting.tools.pricing_tool.constants import GENDER_FIELDS
from aw_reporting.tools.pricing_tool.constants import OPPORTUNITY_VALUES_LIST
from aw_reporting.tools.pricing_tool.constants import TARGETING_TYPES
from utils.datetime import as_date
from utils.datetime import now_in_default_tz
from utils.query import OR
from utils.query import merge_when


class PricingToolSerializer:
    def __init__(self, kwargs={}):
        self.kwargs = kwargs

    def get_campaigns_data(self, campaigns_ids):
        return PricingToolCampaignSerializer(self.kwargs, campaigns_ids).get_data()

    def get_opportunities_data(self, opportunities: list):
        return PricingToolOpportunitySerializer(self.kwargs, opportunities).get_data()


class PricingToolSerializarBase:
    def __init__(self, kwargs):
        self.kwargs = kwargs

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


class PricingToolCampaignSerializer(PricingToolSerializarBase):
    def __init__(self, kwargs, campaigns_ids: list):
        self.kwargs = kwargs
        self.campaigns_ids = campaigns_ids

    def get_data(self):
        self.__prepare_campaign_thumbnails()
        self.__prepare_campaign_extra_data()

        campaigns = self.__prepare_campaigns()

        return [self.__get_campaign_data(campaign) for campaign in campaigns]

    def __get_campaign_data(self, campaign):
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
        if margin is not None:
            margin *= 100

        devices = set([device_str(i) for i, d in enumerate(DEVICE_FIELDS)
                       if campaign[d]])
        targeting = [t for t in TARGETING_TYPES
                     if campaign["has_" + t]]

        ages = [AgeRanges[i] for i, a in enumerate(AGE_FIELDS)
                if campaign[a]]
        genders = [Genders[i] for i, a in enumerate(GENDER_FIELDS)
                   if campaign[a]]

        thumbnail = self.campaign_thumbs.get(campaign_id, None)

        campaign_start, campaign_end = get_campaign_start_end(campaign)
        relevant_date_range = self._get_relevant_date_range(campaign_start,
                                                            campaign_end)

        campaign_data = dict(
            id=campaign_id,
            budget=cost,
            name=campaign["name"],
            thumbnail=thumbnail,
            creative_lengths=list(self.creatives.get(campaign_id, [])),
            products=list(self.ad_group_types.get(campaign_id, [])),
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
            geographic=list(self.geographics.get(campaign_id, [])),
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

    def __prepare_campaign_extra_data(self):
        ad_group_types = AdGroup.objects.filter(
            campaign_id__in=self.campaigns_ids) \
            .exclude(type="") \
            .values("campaign") \
            .annotate(types=ArrayAgg("type", distinct=True)) \
            .values_list("campaign", "types")

        self.ad_group_types = dict(ad_group_types)

        creatives = VideoCreative.objects.filter(
            statistics__ad_group__campaign_id__in=self.campaigns_ids) \
            .values("statistics__ad_group__campaign") \
            .annotate(durations=ArrayAgg("duration", distinct=True)) \
            .values_list("statistics__ad_group__campaign", "durations")

        self.creatives = dict(creatives)

        geographics = GeoTarget.objects.filter(geo_performance__campaign_id__in=self.campaigns_ids) \
            .order_by() \
            .values("geo_performance__campaign") \
            .annotate(names=ArrayAgg("name", distinct=True)) \
            .values_list("geo_performance__campaign", "names")

        self.geographics = dict(geographics)

    def __prepare_campaign_thumbnails(self):
        campaign_id_ref = "statistics__ad_group__campaign_id"
        creatives = VideoCreative.objects.filter(
            statistics__ad_group__campaign_id__in=self.campaigns_ids) \
            .distinct(campaign_id_ref) \
            .values(campaign_id_ref, "id")
        build_thumb = lambda _id: "https://i.ytimg.com/vi/{}/hqdefault.jpg".format(_id)

        self.campaign_thumbs = dict((c[campaign_id_ref], build_thumb(c["id"]))
                                    for c in creatives)

    def __prepare_campaigns(self):
        opp_id_key = "salesforce_placement__opportunity_id"
        campaigns = Campaign.objects.filter(id__in=self.campaigns_ids) \
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
        return campaigns


class PricingToolOpportunitySerializer(PricingToolSerializarBase):
    def __init__(self, kwargs, opportunities: list):
        self.kwargs = kwargs
        self.opportunities_ids = []
        self.campaigns_ids_map = {}
        for opportunity_data in opportunities:
            _id, campaign_ids = opportunity_data
            self.opportunities_ids.append(_id)
            self.campaigns_ids_map[_id] = campaign_ids or []

    def get_data(self):
        self.__prepare_hard_cost_flights()
        opportunities_annotated = Opportunity.objects.filter(id__in=self.opportunities_ids) \
            .annotate(**self.__opportunity_annotation()).values_list(*OPPORTUNITY_VALUES_LIST, named=True)

        return [self.__get_opportunity_data(opp) for opp in opportunities_annotated]

    def __prepare_campaign_data(self, campaigns_ids):
        annotation = dict(
            start_date=Min("start_date"),
            end_date=Max("end_date"),
            ids=ArrayAgg("id"),
            **Aggregation.TARGETING,
            **Aggregation.AGES,
            **Aggregation.GENDERS,
            **Aggregation.DEVICES,
        )
        campaigns = Campaign.objects \
            .filter(id__in=campaigns_ids) \
            .aggregate(**annotation)
        return campaigns

    def __opportunity_annotation(self):
        periods = self.kwargs.get("periods", [])
        placements_date_filter = placement_date_filter(periods)

        return dict(
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
        )

    def __prepare_hard_cost_flights(self):
        annotation = dict(
            sf_hard_cost_total_cost=Sum("placements__flights__total_cost"),
            sf_hard_cost_our_cost=Sum("placements__flights__cost")
        )
        today = now_in_default_tz().date()
        periods = self.kwargs.get("periods", [])
        data_filter = OR(*flight_date_filter(periods, today))
        general_filters = Q(id__in=self.opportunities_ids,
                            placements__goal_type_id=SalesForceGoalType.HARD_COST)
        opportunities = Opportunity.objects \
            .filter(data_filter & general_filters) \
            .annotate(**annotation) \
            .values("id", *annotation.keys())
        hard_cost_dict = {o["id"]: o for o in opportunities}
        empty_stats = {k: 0 for k in annotation.keys()}
        self.hard_cost_flights = {uid: hard_cost_dict.get(uid, empty_stats)
                                  for uid in self.opportunities_ids}

    def __prepare_campaign_statistics(self, campaigns_ids):
        periods = self.kwargs.get("periods", [])
        date_filter = statistic_date_filter(periods)
        date_f = reduce(lambda x, f: x | Q(**f), date_filter, Q())

        stats_queryset = CampaignStatistic.objects \
            .filter(campaign_id__in=campaigns_ids) \
            .filter(date_f) \
            .annotate(
            ordered_rate=F("campaign__salesforce_placement__ordered_rate"))

        total_statistic = stats_queryset.aggregate(
            aw_clicks=Sum("clicks"),
            video_views_100_quartile=Sum("video_views_100_quartile")
        )

        cpv_stats = stats_queryset \
            .filter(campaign__salesforce_placement__goal_type_id=SalesForceGoalType.CPV) \
            .annotate(
                cpv_client_cost=ExpressionWrapper(
                        F("video_views") * F("ordered_rate"),
                        output_field=FloatField()
                )
        ) \
            .aggregate(aw_impressions=Sum("impressions"),
                       aw_video_views=Sum("video_views"),
                       cpv_client_cost_sum=Sum("cpv_client_cost",
                                               output_field=FloatField()),
                       aw_cpv_cost=Sum("cost"))
        cpm_stats = stats_queryset \
            .filter(campaign__salesforce_placement__goal_type_id=SalesForceGoalType.CPM) \
            .annotate(
                cpm_client_cost=ExpressionWrapper(
                        F("impressions") / Value(1000.) * F("ordered_rate"),
                        output_field=FloatField()
                )
        ) \
            .aggregate(aw_impressions=Sum("impressions"),
                       cpm_client_cost_sum=Sum("cpm_client_cost",
                                               output_field=FloatField()),
                       aw_cpm_cost=Sum("cost"))

        return total_statistic, cpv_stats, cpm_stats

    def __get_opportunity_data(self, opportunity):
        hard_cost_data = self.hard_cost_flights.get(opportunity.id)

        campaign_ids = self.campaigns_ids_map.get(opportunity.id)

        total_statistic, cpv_stats, cpm_stats = self.__prepare_campaign_statistics(campaign_ids)
        ad_group_types, creative, geographic = self.__prepare_campaign_extra_data(campaign_ids)
        campaigns_data = self.__prepare_campaign_data(campaign_ids)

        cpm_client_cost = cpm_stats.get("cpm_client_cost_sum") or 0
        cpv_client_cost = cpv_stats.get("cpv_client_cost_sum") or 0
        aw_cpv_cost = cpv_stats.get("aw_cpv_cost") or 0
        aw_cpm_cost = cpm_stats.get("aw_cpm_cost") or 0
        sf_cpv_cost = opportunity.sf_cpv_cost or 0
        sf_cpm_cost = opportunity.sf_cpm_cost or 0
        sf_hard_cost_total_cost = hard_cost_data["sf_hard_cost_total_cost"] or 0
        sf_hard_cost_our_cost = hard_cost_data["sf_hard_cost_our_cost"] or 0
        sf_cpv_units = opportunity.sf_cpv_units or 0
        sf_cpm_units = opportunity.sf_cpm_units or 0
        sf_cpm = sf_cpm_cost / sf_cpm_units * 1000 if sf_cpm_units > 0 else None
        sf_cpv = sf_cpv_cost / sf_cpv_units if sf_cpv_units > 0 else None
        aw_cost = aw_cpm_cost + aw_cpv_cost + sf_hard_cost_our_cost

        client_cost = cpv_client_cost + cpm_client_cost + sf_hard_cost_total_cost
        total_cost = sf_cpv_cost + sf_cpm_cost + sf_hard_cost_total_cost

        cpm_impressions = cpm_stats.get("aw_impressions") or 0
        cpv_impressions = cpv_stats.get("aw_impressions") or 0

        total_clicks = total_statistic.get("aw_clicks") or 0
        total_impressions = cpm_impressions + cpv_impressions
        cpv_video_views = cpv_stats.get("aw_video_views") or 0

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
                     if campaigns_data.get("has_" + t)]
        ages = set([AgeRanges[i] for i, a in enumerate(AGE_FIELDS) if
                    campaigns_data.get(a)])
        genders = set([Genders[i] for i, g in enumerate(GENDER_FIELDS) if
                       campaigns_data.get(g)])
        devices = set([device_str(i) for i, d in enumerate(DEVICE_FIELDS)
                       if campaigns_data.get(d)])
        margin = get_margin(plan_cost=total_cost, cost=aw_cost,
                            client_cost=client_cost)

        if margin is not None:
            margin *= 100

        start_date = campaigns_data.get("start_date")
        end_date = campaigns_data.get("end_date")
        relevant_date_range = self._get_relevant_date_range(start_date,
                                                            end_date)

        video_views_100_quartile = total_statistic.get("video_views_100_quartile") \
                                   or 0
        video100rate = video_views_100_quartile * 100. / total_impressions \
            if total_impressions > 0 else 0

        return dict(
            id=opportunity.id,
            name=opportunity.name,
            brand=opportunity.brand,
            vertical=opportunity.category_id,
            apex_deal=opportunity.apex_deal,
            campaigns=campaigns_data.get("ids"),
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

    def __prepare_campaign_extra_data(self, campaigns_ids):
        ad_group_types = AdGroup.objects.filter(
            campaign_id__in=campaigns_ids) \
            .exclude(type="") \
            .values_list("type", flat=True) \
            .order_by() \
            .distinct()
        creative = VideoCreative.objects.filter(
            statistics__ad_group__campaign_id__in=campaigns_ids) \
            .values_list("duration", flat=True) \
            .order_by() \
            .distinct()

        geographic = GeoTarget.objects.filter(
            geo_performance__campaign_id__in=campaigns_ids) \
            .values_list("name", flat=True) \
            .order_by() \
            .distinct()
        return ad_group_types, creative, geographic


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


class Aggregation:
    TARGETING = dict(
        ("has_" + t, Max(Case(When(
            **{
                "has_" + t: True,
                "then": Value(1)
            }),
            output_field=BooleanField(),
            default=Value(0))))
        for t in TARGETING_TYPES)
    AGES = dict(
        (a, Max(Case(When(**{
            a: True,
            "then": Value(1)
        }),
                     output_field=BooleanField(),
                     default=Value(0))))
        for a in AGE_FIELDS)

    GENDERS = dict(
        (a, Max(Case(When(**{
            a: True,
            "then": Value(1)
        }),
                     output_field=BooleanField(),
                     default=Value(0))))
        for a in GENDER_FIELDS)

    DEVICES = dict(
        (a, Max(Case(When(**{
            a: True,
            "then": Value(1)
        }),
                     output_field=BooleanField(),
                     default=Value(0))))
        for a in DEVICE_FIELDS)
