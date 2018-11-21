from collections import defaultdict
from functools import reduce

from django.db.models import BooleanField
from django.db.models import Case
from django.db.models import F
from django.db.models import FloatField
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When

from aw_reporting.calculations.cost import get_client_cost
from aw_reporting.models import AdGroup
from aw_reporting.models import AgeRanges
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
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
from aw_reporting.tools.pricing_tool.constants import TARGETING_TYPES
from userprofile.models import UserProfile
from utils.datetime import as_date
from utils.datetime import now_in_default_tz
from utils.query import merge_when


class PricingToolSerializer:
    def __init__(self, kwargs):
        self.kwargs = kwargs

    def get_opportunities_data(self, opportunities: list, user: UserProfile):
        ids = [opp.id for opp in opportunities]

        campaign_thumbs = self._get_campaign_thumbnails(ids)

        campaign_groups = self._prepare_campaigns(ids, user=user)
        campaigns_data = self._prepare_campaign_data(ids)
        opportunities_annotated = Opportunity.objects.filter(id__in=ids) \
            .annotate(**self._opportunity_annotation())
        return [
            self._get_opportunity_data(opp, campaign_groups[opp.id],
                                       campaigns_data[opp.id],
                                       campaign_thumbs)
            for opp in opportunities_annotated]

    def _get_opportunity_data(self, opportunity, campaigns,
                              campaigns_data, campaign_thumbs):
        today = now_in_default_tz().date()
        periods = self.kwargs.get("periods", [])

        opportunity_filter = Q(placement__opportunity=opportunity)
        period_filter = statistic_date_filter(periods)
        flight_date_range_filter = Q(
            placement__adwords_campaigns__statistics__date__gte=F("start"),
            placement__adwords_campaigns__statistics__date__lte=F("end"),
        )
        flight_is_hard_cost = Q(placement__goal_type_id=SalesForceGoalType.HARD_COST)
        statistic_filter = (period_filter & flight_date_range_filter) | flight_is_hard_cost
        start_filter = Q(start__lte=today)

        all_filters = opportunity_filter & start_filter & statistic_filter
        flights = Flight.objects.filter(all_filters) \
            .annotate(
            goal_type_id=F("placement__goal_type_id"),
            dynamic_placement=F("placement__dynamic_placement"),
            placement_type=F("placement__placement_type"),
            ordered_rate=F("placement__ordered_rate"),
            tech_fee=F("placement__tech_fee"),
            impressions=Sum("placement__adwords_campaigns__statistics__impressions"),
            video_views=Sum("placement__adwords_campaigns__statistics__video_views"),
            clicks=Sum("placement__adwords_campaigns__statistics__clicks"),
            video_views_100_quartile=Sum("placement__adwords_campaigns__statistics__video_views_100_quartile"),
            aw_cost=Sum("placement__adwords_campaigns__statistics__cost"),
        )
        client_cost = our_cost = 0
        cpm_impressions = cpv_impressions = 0
        aw_cpm_cost = aw_cpv_cost = 0
        cpv_video_views = 0
        total_clicks = 0
        video_views_100_quartile = 0
        for flight in flights:
            goal_type_id = flight.goal_type_id
            if goal_type_id == SalesForceGoalType.HARD_COST:
                client_cost += flight.total_cost or 0
                our_cost += flight.cost or 0
            else:
                client_cost += get_client_cost(
                    goal_type_id=flight.goal_type_id,
                    dynamic_placement=flight.dynamic_placement,
                    placement_type=flight.placement_type,
                    ordered_rate=flight.ordered_rate,
                    total_cost=flight.total_cost,
                    tech_fee=flight.tech_fee,
                    start=flight.start,
                    end=flight.end,
                    impressions=flight.impressions,
                    video_views=flight.video_views,
                    aw_cost=flight.aw_cost,
                )
                our_cost += flight.aw_cost
                total_clicks += flight.clicks
                video_views_100_quartile += flight.video_views_100_quartile
            if goal_type_id == SalesForceGoalType.CPM:
                cpm_impressions += flight.impressions
                aw_cpm_cost += flight.aw_cost
            if goal_type_id == SalesForceGoalType.CPV:
                cpv_impressions += flight.impressions
                cpv_video_views += flight.video_views
                aw_cpv_cost += flight.aw_cost

        sf_cpv_cost = opportunity.sf_cpv_cost or 0
        sf_cpm_cost = opportunity.sf_cpm_cost or 0
        sf_cpv_units = opportunity.sf_cpv_units or 0
        sf_cpm_units = opportunity.sf_cpm_units or 0
        sf_cpm = sf_cpm_cost / sf_cpm_units * 1000 if sf_cpm_units > 0 else None
        sf_cpv = sf_cpv_cost / sf_cpv_units if sf_cpv_units > 0 else None

        total_impressions = cpm_impressions + cpv_impressions

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
        margin = get_margin(cost=our_cost, client_cost=client_cost, plan_cost=None)
        if margin is not None:
            margin *= 100

        start_date = campaigns_data.get("start_date")
        end_date = campaigns_data.get("end_date")
        relevant_date_range = self._get_relevant_date_range(start_date,
                                                            end_date)

        ad_group_types = AdGroup.objects.filter(
            campaign__salesforce_placement__opportunity=opportunity) \
            .exclude(type="") \
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
            budget=our_cost,
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

    def _prepare_campaign_data(self, opportunity_ids):
        annotation = dict(
            start_date=Min("placements__adwords_campaigns__start_date"),
            end_date=Max("placements__adwords_campaigns__end_date"),
            **Aggregation.TARGETING,
            **Aggregation.AGES,
            **Aggregation.GENDERS,
            **Aggregation.DEVICES,
        )
        opportunities = Opportunity.objects \
            .filter(Q(id__in=opportunity_ids)) \
            .annotate(**annotation) \
            .values("id", *annotation.keys())
        campaign_map = {o["id"]: o for o in opportunities}
        return {uid: campaign_map.get(uid, dict()) for uid in opportunity_ids}

    def _prepare_campaigns(self, opportunity_ids, user):
        opp_id_key = "salesforce_placement__opportunity_id"
        campaigns = Campaign.objects.get_queryset_for_user(user) \
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
    dicts = [
                dict(
                    placement__adwords_campaigns__statistics__date__gte=start,
                    placement__adwords_campaigns__statistics__date__lte=end,
                )
                for start, end in periods
            ] or [dict()]
    return reduce(lambda x, f: x | Q(**f), dicts, Q())


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


class Aggregation:
    TARGETING = dict(
        ("has_" + t, Max(Case(When(
            **{
                "placements__adwords_campaigns__has_" + t: True,
                "then": Value(1)
            }),
            output_field=BooleanField(),
            default=Value(0))))
        for t in TARGETING_TYPES)
    AGES = dict(
        (a, Max(Case(When(**{
            "placements__adwords_campaigns__" + a: True,
            "then": Value(1)
        }),
                     output_field=BooleanField(),
                     default=Value(0))))
        for a in AGE_FIELDS)

    GENDERS = dict(
        (a, Max(Case(When(**{
            "placements__adwords_campaigns__" + a: True,
            "then": Value(1)
        }),
                     output_field=BooleanField(),
                     default=Value(0))))
        for a in GENDER_FIELDS)

    DEVICES = dict(
        (a, Max(Case(When(**{
            "placements__adwords_campaigns__" + a: True,
            "then": Value(1)
        }),
                     output_field=BooleanField(),
                     default=Value(0))))
        for a in DEVICE_FIELDS)
