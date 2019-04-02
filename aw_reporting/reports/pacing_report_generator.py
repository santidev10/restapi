from datetime import timedelta
from collections import defaultdict

from django.contrib.auth import get_user_model
from django.db.models import F

from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.models.salesforce_constants import goal_type_str

from aw_reporting.reports.pacing_report import PacingReport
from aw_reporting.reports.pacing_report import get_chart_data
from aw_reporting.reports.pacing_report import get_pacing_from_flights
from aw_reporting.reports.pacing_report import populate_daily_delivery_data
from aw_reporting.reports.pacing_report import get_flight_delivery_annotate
from aw_reporting.reports.pacing_report import _get_dynamic_placements_summary


class PacingReportGenerator(PacingReport):

    # ## OPPORTUNITIES ## #
    def get_opportunities(self, get, user=None):
        queryset = self.get_opportunities_queryset(get, user)

        # get raw opportunity data
        opportunities = queryset.values(
            "id", "name", "start", "end", "cannot_roll_over",
            "category", "notes", "ad_ops_manager__name",
            "ad_ops_manager__email", "account_manager__name",
            "sales_manager__name", "apex_deal",
            "billing_server",
            "territory", "margin_cap_required",
            "cpm_buffer", "cpv_buffer"
        )

        # prepare response
        for o in opportunities:

            if o['ad_ops_manager__email']:
                ad_ops_email = o['ad_ops_manager__email']
                user_rows = get_user_model().objects.filter(
                    email=ad_ops_email,
                    profile_image_url__isnull=False,
                ).exclude(profile_image_url="").values("email",
                                                       "profile_image_url")
                thumbnails = {r['email']: r['profile_image_url'] for r in
                              user_rows}
            else:
                thumbnails = {}

            today = self.today
            if o["start"] is None or o["end"] is None:
                status = "undefined"
            elif o["start"] > today:
                status = "upcoming"
            elif o["end"] < today:
                status = "completed"
            else:
                status = "active"

            placements = self.get_placements_data(opportunity_id=o['id'])
            o["flights"] = flights = self.get_flights_data(placement__opportunity_id=o['id'])

            o["status"] = status
            goal_type_ids = sorted(filter(
                lambda g: g is not None,
                set([p["goal_type_id"] for p in placements])
            ))
            o['goal_type_ids'] = goal_type_ids

            delivery_stats = self.get_delivery_stats_from_flights(flights)
            o.update(delivery_stats)

            plan_stats = self.get_plan_stats_from_flights(flights)
            o.update(plan_stats)

            o["pacing"] = get_pacing_from_flights(flights)
            o["margin"] = self.get_margin_from_flights(flights, o["cost"], o["current_cost_limit"])

            o['thumbnail'] = thumbnails.get(o['ad_ops_manager__email'])

            o['ad_ops'] = o['ad_ops_manager__name']
            del o['ad_ops_manager__name'], o['ad_ops_manager__email']

            o['am'] = o['account_manager__name']
            del o['account_manager__name']

            o['sales'] = o['sales_manager__name']
            del o['sales_manager__name']

            o['region'] = o["territory"]

            self.add_calculated_fields(o)
            o.update(_get_dynamic_placements_summary(placements))

            yield o

    # ## PLACEMENTS ## #
    def get_placements(self, opportunity):
        queryset = opportunity.placements.all().order_by("name", "start")

        # get raw  data
        placements = queryset.values(
            "id", "name", "start", "end", "goal_type_id", "ordered_units",
            "dynamic_placement", "tech_fee"
        ).annotate(plan_cost=F("total_cost"))

        # plan stats
        pl_key = "placement_id"
        flights_data = self.get_flights_data(
            placement__opportunity=opportunity)
        populate_daily_delivery_data(flights_data)
        all_flights = defaultdict(list)
        for f in flights_data:
            all_flights[f[pl_key]].append(f)

        for p in placements:
            goal_type_id = p["goal_type_id"]
            p.update(
                is_completed=p["end"] < self.today if p["end"] else None,
                is_upcoming=p["start"] > self.today if p["start"] else None,
                tech_fee=float(p["tech_fee"]) if p["tech_fee"] else None
            )
            flights = all_flights[p["id"]]

            delivery_stats = self.get_delivery_stats_from_flights(flights)
            p.update(delivery_stats)

            plan_stats = self.get_plan_stats_from_flights(flights)
            p.update(plan_stats)

            p["pacing"] = get_pacing_from_flights(flights)
            p["margin"] = self.get_margin_from_flights(flights, p["cost"],
                                                       p["current_cost_limit"])

            self.add_calculated_fields(p)
            del p["ordered_units"]

            chart_data = get_chart_data(
                flights=flights,
                today=self.today,
                cpm_buffer=opportunity.cpm_buffer,
                cpv_buffer=opportunity.cpv_buffer
            )

            p.update(chart_data)

            if goal_type_id == SalesForceGoalType.HARD_COST:
                self._set_none_hard_cost_properties(p)
            p.update(goal_type=goal_type_str(goal_type_id))

            yield p

        # return placements

    # ## FLIGHTS ## #
    def get_flights(self, placement):
        flights_data = self.get_flights_data(placement=placement)
        populate_daily_delivery_data(flights_data)

        id_field = "campaign__salesforce_placement__flights__id"
        campaign_stats_qs = CampaignStatistic.objects.filter(
            date=self.yesterday - timedelta(days=1),
            date__gte=F("campaign__salesforce_placement__flights__start"),
            date__lte=F("campaign__salesforce_placement__flights__end"),
            campaign__salesforce_placement=placement)
        all_aw_before_yesterday_stats = campaign_stats_qs.values(
            id_field).order_by(id_field).annotate(
            **get_flight_delivery_annotate(
                ("sum_video_views", "sum_impressions", "sum_cost"),
            )
        )
        all_aw_before_yesterday_stats = {i[id_field]: i for i in
                                         all_aw_before_yesterday_stats}

        flights = []
        for f in flights_data:
            tech_fee = float(f["placement__tech_fee"]) \
                if f["placement__tech_fee"] else None
            dynamic_placement = f["placement__dynamic_placement"]
            flight = dict(
                id=f["id"], name=f["name"], start=f["start"], end=f["end"],
                plan_cost=f["total_cost"], margin=None, pacing=None,
                dynamic_placement=dynamic_placement,
                tech_fee=tech_fee, goal_type_id=f["placement__goal_type_id"]
            )
            f_data = [f]

            plan_stats = self.get_plan_stats_from_flights(f_data)
            flight.update(plan_stats)

            delivery_stats = self.get_delivery_stats_from_flights(f_data)
            flight.update(delivery_stats)

            flight["pacing"] = get_pacing_from_flights(f_data)
            flight["margin"] = self.get_margin_from_flights(f_data,
                                                            flight["cost"],
                                                            flight["current_cost_limit"])

            # chart data
            before_yesterday_stats = all_aw_before_yesterday_stats.get(f['id'],
                                                                       {})
            chart_data = get_chart_data(
                flights=[f],
                today=self.today,
                before_yesterday_stats=before_yesterday_stats,
                cpm_buffer=placement.opportunity.cpm_buffer,
                cpv_buffer=placement.opportunity.cpv_buffer
            )
            flight.update(chart_data)

            self.add_calculated_fields(flight)

            flight['budget'] = f['budget']
            flights.append(flight)

            yield flight

    # ## CAMPAIGNS ## #
    def get_campaigns(self, flight):
        queryset = Campaign.objects.filter(
            salesforce_placement__flights=flight)
        campaigns = queryset.values(
            "id", "name", "goal_allocation",
        ).order_by('name').annotate(start=F("start_date"), end=F("end_date"))

        # get allocations
        if campaigns:
            if abs(sum(c['goal_allocation'] for c in campaigns) - 100) > 0.1:
                def_allocation = 100 / len(campaigns)  # 50 for two campaigns

        # flights for plan (we shall use them for all the plan stats calculations)
        # we take them all so the over-delivery is calculated
        flights_data = self.get_flights_data(
            placement=flight.placement,
            id=flight.id
        )
        # flights_data = [f for f in all_placement_flights if
        #                 f["id"] == flight.id]
        populate_daily_delivery_data(flights_data)

        for c in campaigns:
            c['goal_allocation'] = def_allocation
            allocation_ko = c['goal_allocation'] / 100
            kwargs = dict(allocation_ko=allocation_ko, campaign_id=c["id"])

            plan_stats = self.get_plan_stats_from_flights(flights_data,
                                                          **kwargs)
            c.update(plan_stats)

            delivery_stats = self.get_delivery_stats_from_flights(flights_data,
                                                                  campaign_id=
                                                                  c["id"])
            c.update(delivery_stats)

            c["pacing"] = get_pacing_from_flights(flights_data, **kwargs)
            c["margin"] = self.get_margin_from_flights(flights_data, c["cost"],
                                                       c["current_cost_limit"],
                                                       **kwargs)

            chart_data = get_chart_data(
                flights=flights_data,
                today=self.today,
                cpv_buffer=flight.placement.opportunity.cpv_buffer,
                cpm_buffer=flight.placement.opportunity.cpm_buffer,
                **kwargs
            )

            c.update(chart_data)

            self.add_calculated_fields(c)

            c['flight_budget'] = flight.budget

            yield c