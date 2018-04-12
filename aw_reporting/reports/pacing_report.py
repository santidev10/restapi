from collections import defaultdict
from datetime import timedelta
from math import ceil

from django.contrib.auth import get_user_model

from aw_reporting.calculations.margin import get_days_run_and_total_days, \
    get_margin_from_flights
from aw_reporting.models import OpPlacement, Flight, get_ctr_v, get_ctr, \
    get_average_cpv, get_average_cpm, get_video_view_rate, dict_calculate_stats, \
    Opportunity, Campaign, CampaignStatistic, get_margin
from aw_reporting.models.salesforce_constants import SalesForceGoalType, \
    SalesForceGoalTypes, goal_type_str, SalesForceRegions, \
    DYNAMIC_PLACEMENT_TYPES, DynamicPlacementType
from aw_reporting.settings import InstanceSettings
from aw_reporting.utils import get_dates_range
from utils.datetime import now_in_default_tz
from django.db.models import F, Sum, Case, When, Value, FloatField, IntegerField


class PacingReport:
    DEFAULT_AVERAGE_CPV = .04
    DEFAULT_AVERAGE_CPM = 6.25

    borders = dict(
        margin=(.4, .29),
        pacing=((.8, .9), (1.1, 1.2)),
        video_view_rate=(0.20, 0.30),
        ctr=(.005, .0075),
    )

    big_budget_border = 500000
    goal_factor = 1.02
    big_goal_factor = 1.01

    def __init__(self, today=None):
        self.today = today or now_in_default_tz().date()
        self.yesterday = self.today - timedelta(days=1)

        self.name = "{title}-{timestamp}".format(
            title=self.__class__.__name__,
            timestamp=self.today.strftime("%Y%m%d"),
        )

    def get_goal_items_factor(self, budget):
        """
        Add 2(or 1)% to overall views goal which would trickle down to the lower levels (placements, flights, etc.)
        :param budget:
        :return:
        """
        if budget > self.big_budget_border:
            return self.big_goal_factor
        else:
            return self.goal_factor

    def get_days_run_and_total_days(self, f):
        return get_days_run_and_total_days(f, self.yesterday)

    def get_flights_delivery_annotate(self):
        in_flight_dates_criteria = dict(
            placement__adwords_campaigns__statistics__date__gte=F("start"),
            placement__adwords_campaigns__statistics__date__lte=F("end"),
        )
        flights_delivery_annotate = dict(
            delivery=Sum(
                Case(
                    When(
                        then=Case(
                            When(
                                placement__goal_type_id=Value(
                                    SalesForceGoalType.CPM),
                                then=F(
                                    "placement__adwords_campaigns__statistics__impressions"),
                            ),
                            When(
                                placement__goal_type_id=Value(
                                    SalesForceGoalType.CPV),
                                then=F(
                                    "placement__adwords_campaigns__statistics__video_views"),
                            ),
                            When(
                                placement__dynamic_placement__in=[
                                    DynamicPlacementType.BUDGET,
                                    DynamicPlacementType.RATE_AND_TECH_FEE],
                                then=F(
                                    "placement__adwords_campaigns__statistics__cost"),
                            ),
                            output_field=FloatField(),
                        ),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            yesterday_cost=Sum(
                Case(
                    When(
                        placement__adwords_campaigns__statistics__date=self.yesterday,
                        then=F(
                            "placement__adwords_campaigns__statistics__cost"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            yesterday_delivery=Sum(
                Case(
                    When(
                        placement__adwords_campaigns__statistics__date=self.yesterday,
                        then=Case(
                            When(
                                placement__goal_type_id=Value(
                                    SalesForceGoalType.CPM),
                                then=F(
                                    "placement__adwords_campaigns__statistics__impressions"),
                            ),
                            When(
                                placement__goal_type_id=Value(
                                    SalesForceGoalType.CPV),
                                then=F(
                                    "placement__adwords_campaigns__statistics__video_views"),
                            ),
                            When(
                                placement__dynamic_placement__in=[
                                    DynamicPlacementType.BUDGET,
                                    DynamicPlacementType.RATE_AND_TECH_FEE],
                                then=F(
                                    "placement__adwords_campaigns__statistics__cost"),
                            ),
                            output_field=FloatField(),
                        ),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            impressions=Sum(
                Case(
                    When(
                        then=F(
                            "placement__adwords_campaigns__statistics__impressions"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            video_impressions=Sum(
                Case(
                    When(
                        placement__adwords_campaigns__video_views__gt=Value(0),
                        then=F(
                            "placement__adwords_campaigns__statistics__impressions"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            video_clicks=Sum(
                Case(
                    When(
                        placement__adwords_campaigns__video_views__gt=Value(0),
                        then=F(
                            "placement__adwords_campaigns__statistics__clicks"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            video_cost=Sum(
                Case(
                    When(
                        placement__adwords_campaigns__video_views__gt=Value(0),
                        then=F(
                            "placement__adwords_campaigns__statistics__cost"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            video_views=Sum(
                Case(
                    When(
                        then=F(
                            "placement__adwords_campaigns__statistics__video_views"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            clicks=Sum(
                Case(
                    When(
                        then=F(
                            "placement__adwords_campaigns__statistics__clicks"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
            sum_cost=Sum(
                Case(
                    When(
                        then=F(
                            "placement__adwords_campaigns__statistics__cost"),
                        **in_flight_dates_criteria
                    ),
                ),
            ),
        )
        return flights_delivery_annotate

    @staticmethod
    def get_delivery_field_name(flight_dict):
        if flight_dict[
            "placement__goal_type_id"] == SalesForceGoalType.HARD_COST and \
                flight_dict[
                    "placement__dynamic_placement"] \
                in (DynamicPlacementType.BUDGET,
                    DynamicPlacementType.RATE_AND_TECH_FEE):
            return "cost"
        elif flight_dict["placement__goal_type_id"] == SalesForceGoalType.CPM:
            return "impressions"
        elif flight_dict["placement__goal_type_id"] == SalesForceGoalType.CPV:
            return "video_views"

    def get_placements_data(self, **filters):
        queryset = OpPlacement.objects.filter(**filters)
        placement_fields = ("id", "dynamic_placement", "opportunity_id")
        raw_data = queryset.values(*placement_fields)
        return raw_data

    def get_flights_data(self, **filters):
        queryset = Flight.objects.filter(
            start__isnull=False,
            end__isnull=False,
            **filters
        )

        campaign_id_key = "placement__adwords_campaigns__id"
        group_by = ("id", campaign_id_key)

        annotate = self.get_flights_delivery_annotate()
        flight_fields = (
            "id", "name", "start", "end", "total_cost", "ordered_units",
            "cost", "placement_id",
            "placement__goal_type_id", "placement__placement_type",
            "placement__opportunity_id",
            "placement__opportunity__cannot_roll_over",
            "placement__opportunity__budget",
            "placement__dynamic_placement", "placement__ordered_rate",
            "placement__tech_fee", "placement__tech_fee_type",
            "placement__total_cost",
        )
        raw_data = queryset.values(
            campaign_id_key,  # segment by campaigns
            *flight_fields
        ).order_by(*group_by).annotate(**annotate)

        annotate_keys = tuple(annotate.keys())
        data = {}
        for row in raw_data:
            if row["id"] in data:
                fl_data = data[row["id"]]
            else:
                fl_data = {k: row[k] for k in flight_fields}
                fl_data["campaigns"] = {}
                data[row["id"]] = fl_data

            fl_data["campaigns"][
                row[campaign_id_key]
            ] = {k: row[k] or 0 for k in annotate_keys}

            for f in annotate_keys:
                fl_data[f] = fl_data.get(f, 0) + (row[f] or 0)

        data = sorted(data.values(), key=lambda el: (el["start"], el["name"]))

        # daily delivery for charts
        placement_ids = set(f["placement_id"] for f in data)
        campaign_stats_qs = CampaignStatistic.objects.filter(
            campaign__salesforce_placement_id__in=placement_ids)
        fl_id_field = 'campaign__salesforce_placement__flights__id'
        raw_aw_daily_stats = campaign_stats_qs.values(fl_id_field,
                                                      "campaign_id",
                                                      "date").order_by(
            fl_id_field,
            "campaign_id",
            "date").annotate(
            **{k: v for k, v in self.get_flight_delivery_annotate().items()
               if k in ("sum_video_views", "sum_impressions", "sum_cost")}
        )
        all_aw_daily_stats = defaultdict(list)
        for row in raw_aw_daily_stats:
            all_aw_daily_stats[row[fl_id_field]].append(row)

        for fl in data:
            fl["daily_delivery"] = [
                dict(
                    date=e["date"], campaign_id=e["campaign_id"],
                    cost=e["sum_cost"], impressions=e["sum_impressions"],
                    video_views=e["sum_video_views"],
                ) for e in all_aw_daily_stats[fl["id"]]
            ]

            start, end = fl["start"], fl["end"]
            fl["days"] = (end - start).days + 1 if end and start else 0

            if fl["placement__opportunity__budget"] > self.big_budget_border:
                goal_factor = self.big_goal_factor
            else:
                goal_factor = self.goal_factor

            fl["plan_units"] = 0
            if fl["placement__goal_type_id"] == SalesForceGoalType.HARD_COST:
                if fl["placement__dynamic_placement"] == DynamicPlacementType.BUDGET:
                    fl["plan_units"] = fl["total_cost"] or 0

                elif fl["placement__dynamic_placement"] == DynamicPlacementType.RATE_AND_TECH_FEE:
                    fl["plan_units"] = self.get_budget_to_spend_from_added_fee_flight(fl)
            else:
                fl["plan_units"] = fl["ordered_units"] * goal_factor if fl[
                    "ordered_units"] else 0

            for field in (
                    "delivery", "yesterday_cost", "yesterday_delivery",
                    "impressions",
                    "video_impressions", "video_views", "clicks"):
                fl[field] = fl[field] or 0

        # we need to check  "cannot_roll_over" option
        # if it's False, the over-delivery from completed flights should be spread between future ones
        # IMPORTANT: the code below supposes that flights sorted by start date ASC (older first)
        flights_by_placement = defaultdict(list)
        for fl in data:
            if None not in (fl["start"], fl["end"]):
                flights_by_placement[fl["placement_id"]].append(fl)

        for placement_flights in flights_by_placement.values():
            cannot_roll_over = placement_flights[0][
                "placement__opportunity__cannot_roll_over"]
            goal_type_id = placement_flights[0]["placement__goal_type_id"]
            if cannot_roll_over is True \
                    or goal_type_id not in (
                    0,
                    1):  # Use re-allocation for CPV and CPM placements only for now
                continue

            # first get the over delivery
            over_delivery = 0
            for f in placement_flights:
                diff = f["delivery"] - f["plan_units"]
                if diff > 0:  # over-delivery
                    over_delivery += diff
                elif diff < 0 and f[
                    "end"] <= self.yesterday:  # under delivery for an ended flight
                    # if we have an ended under-delivered flight,
                    # it can consume his under-delivery from the total over-delivery amount
                    abs_diff = abs(diff)
                    reallocate_to_flight = min(abs_diff, over_delivery)
                    over_delivery -= reallocate_to_flight
                    f["plan_units"] -= reallocate_to_flight

            # then reassign between flights that haven't finished
            if over_delivery:
                not_finished_flights = [f for f in placement_flights if
                                        f["end"] > self.yesterday]
                if not_finished_flights:

                    # recalculate reassignment
                    for fl in not_finished_flights:
                        flight_can_consume = fl["plan_units"] - fl["delivery"]
                        if flight_can_consume > 0:  # if it hasn't reached the plan yet
                            total_days = sum(
                                f["days"] for f in not_finished_flights if
                                f["start"] >= fl["start"])
                            daily_reassignment = over_delivery / total_days
                            assigned_over_delivery = daily_reassignment * fl[
                                "days"]
                            # a flight cannot consume more than (plan_units - delivery)
                            assigned_over_delivery = min(
                                assigned_over_delivery, flight_can_consume)
                            # reassign items
                            fl["plan_units"] -= assigned_over_delivery
                            over_delivery -= assigned_over_delivery

        return data

    def get_budget_to_spend_from_added_fee_flight(self, f, allocation_ko=1,
                                                  campaign_id=None):
        stats_total = get_stats_from_flight(f, campaign_id=campaign_id)
        stats_3days = get_stats_from_flight(
            f, start=self.today - timedelta(days=3),
            end=self.today - timedelta(days=1),
            campaign_id=campaign_id,
        )
        #
        tech_fee = float(f["placement__tech_fee"] or 0)
        if f["placement__tech_fee_type"] == OpPlacement.TECH_FEE_CPV_TYPE:
            video_views = stats_total["video_views"] or 0
            total_cpv = self.DEFAULT_AVERAGE_CPV \
                if stats_total["cpv"] is None \
                else stats_total["cpv"]
            three_days_cpv = self.DEFAULT_AVERAGE_CPV \
                if stats_3days["cpv"] is None \
                else stats_3days["cpv"]
            client_cost_spent = video_views * (total_cpv + tech_fee)
            spend_kf = three_days_cpv / (three_days_cpv + tech_fee)

        elif f["placement__tech_fee_type"] == OpPlacement.TECH_FEE_CPM_TYPE:
            impressions = stats_total["impressions"] or 0
            total_cpm = self.DEFAULT_AVERAGE_CPM \
                if stats_total["cpm"] is None \
                else stats_total["cpm"]
            three_days_cpm = self.DEFAULT_AVERAGE_CPM if stats_3days[
                                                             "cpm"] is None else \
                stats_3days["cpm"]
            client_cost_spent = impressions / 1000 * (total_cpm + tech_fee)
            spend_kf = three_days_cpm / (three_days_cpm + tech_fee)
        else:
            client_cost_spent = spend_kf = 0

        total_cost = f["total_cost"] or 0
        client_cost_remaining = total_cost * allocation_ko \
                                - client_cost_spent
        spent_remaining = spend_kf * client_cost_remaining
        spent = stats_total["cost"] or 0
        return spent + spent_remaining

    @staticmethod
    def get_delivery_stats_from_flights(flights, campaign_id=None):
        impressions = video_views = cost = clicks = 0
        video_impressions = video_clicks = video_cost = 0
        goal_type_ids = set()
        for f in flights:
            goal_type_ids.add(f["placement__goal_type_id"])

            stats = f["campaigns"][campaign_id] if campaign_id else f

            impressions += stats["impressions"] or 0
            video_impressions += stats["video_impressions"] or 0
            video_clicks += stats["video_clicks"] or 0
            video_views += stats["video_views"] or 0
            video_cost += stats["video_cost"] or 0
            clicks += stats["clicks"] or 0
            cost += stats["sum_cost"] or 0

        if 1 in goal_type_ids:
            ctr = get_ctr_v(video_clicks, video_views)
        else:
            ctr = get_ctr(clicks, impressions)

        goal_type_id = SalesForceGoalType.CPV
        if 0 in goal_type_ids:
            if 1 in goal_type_ids:
                goal_type_id = SalesForceGoalType.CPM_AND_CPV
            else:
                goal_type_id = SalesForceGoalType.CPM

        stats = dict(
            impressions=impressions, video_views=video_views,
            cpv=get_average_cpv(video_cost, video_views),
            cpm=get_average_cpm(cost, impressions),
            ctr=ctr,
            video_view_rate=get_video_view_rate(video_views,
                                                video_impressions),
            goal_type=SalesForceGoalTypes[goal_type_id],
        )
        return stats

    def get_plan_stats_from_flights(self, flights, allocation_ko=1,
                                    campaign_id=None):
        plan_impressions = plan_video_views = None
        impressions = video_views = cpm_cost = cpv_cost = 0
        sum_total_cost = sum_delivery = sum_spent_cost = 0

        for f in flights:
            goal_type_id = f["placement__goal_type_id"]
            dynamic_placement = f["placement__dynamic_placement"]
            placement_type = f["placement__placement_type"]
            plan_units = f["plan_units"] * allocation_ko
            ordered_units = (f["ordered_units"] or 0) * allocation_ko
            total_cost = (f["total_cost"] or 0) * allocation_ko

            if campaign_id:
                stats = f["campaigns"][campaign_id]
            else:
                stats = f

            sum_delivery += stats["delivery"] or 0
            aw_cost = stats["sum_cost"] or 0

            if dynamic_placement == DynamicPlacementType.BUDGET:
                sum_spent_cost += aw_cost
                flight_count = Flight.objects.filter(
                    placement_id=f["placement_id"]).count()
                total_cost = (f["placement__total_cost"] or 0) * allocation_ko
                sum_total_cost += total_cost / flight_count
            elif placement_type == OpPlacement.OUTGOING_FEE_TYPE:
                days_run, total_days = self.get_days_run_and_total_days(f)
                if days_run and total_days and f["cost"]:
                    cost = f["cost"] or 0
                    sum_spent_cost += cost * days_run / total_days

                sum_total_cost += total_cost

            elif goal_type_id == SalesForceGoalType.CPV:
                plan_video_views = (plan_video_views or 0) + plan_units
                video_views += ordered_units
                cpv_cost += total_cost

                sum_spent_cost += aw_cost
                sum_total_cost += total_cost

            elif goal_type_id == SalesForceGoalType.CPM:
                plan_impressions = (plan_impressions or 0) + plan_units
                impressions += ordered_units
                cpm_cost += total_cost

                sum_spent_cost += aw_cost
                sum_total_cost += total_cost

            elif goal_type_id == SalesForceGoalType.HARD_COST:
                sum_spent_cost += aw_cost
                sum_total_cost += total_cost

        result = dict(
            plan_impressions=plan_impressions,
            plan_video_views=plan_video_views,
            plan_cpv=cpv_cost / video_views if video_views else None,
            plan_cpm=cpm_cost / impressions * 1000 if impressions else None,
            cost=sum_spent_cost,
            plan_cost=sum_total_cost,
        )
        return result

    def get_pacing_from_flights(self, flights, allocation_ko=1,
                                campaign_id=None):
        goal_type_ids = list(set(f["placement__goal_type_id"] for f in flights))
        dynamic_placements = list(
            set(f["placement__dynamic_placement"] for f in flights))
        if len(goal_type_ids) == 1 \
                and goal_type_ids[0] == SalesForceGoalType.HARD_COST \
                and len(dynamic_placements) == 1 and \
                dynamic_placements[0] == DynamicPlacementType.SERVICE_FEE:
            pacing = 1
        else:
            units_by_yesterday = sum_delivery = 0
            for f in flights:
                stats = f["campaigns"][campaign_id] if campaign_id else f
                sum_delivery += stats["delivery"] or 0
                days_run, total_days = self.get_days_run_and_total_days(f)
                if days_run and total_days:
                    plan_units = f["plan_units"] * allocation_ko
                    units_by_yesterday += plan_units * days_run / total_days
            pacing = sum_delivery / units_by_yesterday \
                if units_by_yesterday else None
        return pacing

    def get_margin_from_flights(self, flights, cost, plan_cost,
                                allocation_ko=1, campaign_id=None):
        return get_margin_from_flights(flights, cost, plan_cost, allocation_ko,
                                       campaign_id, yesterday=self.yesterday)

    def get_pacing_goal_for_date(self, flight, date, allocation_ko=1,
                                 campaign_id=None):
        stats_total = get_stats_from_flight(flight, campaign_id=campaign_id,
                                            end=date - timedelta(days=1))
        last_day = max(min(date, self.today), flight["start"])
        goal_type_id = flight["placement__goal_type_id"]
        dynamic_placement = flight["placement__dynamic_placement"]
        today_budget = today_units = 0
        total_cost = flight["total_cost"] or 0

        if dynamic_placement == DynamicPlacementType.BUDGET:
            today_budget = self.get_today_goal(
                total_cost * allocation_ko,
                stats_total["cost"], flight["end"], last_day)
        elif dynamic_placement == DynamicPlacementType.RATE_AND_TECH_FEE:
            last_day = min(date, self.today)
            stats_3days = get_stats_from_flight(
                flight, campaign_id=campaign_id,
                start=last_day - timedelta(days=3),
                end=last_day - timedelta(days=1),
            )

            tech_fee = float(flight["placement__tech_fee"] or 0)
            if goal_type_id == SalesForceGoalType.CPV:
                video_views = stats_total["video_views"] or 0
                total_cpv = self.DEFAULT_AVERAGE_CPV \
                    if stats_total["cpv"] is None \
                    else stats_total["cpv"]
                three_days_cpv = self.DEFAULT_AVERAGE_CPV \
                    if stats_3days["cpv"] is None \
                    else stats_3days["cpv"]
                client_cost_spent = video_views * (total_cpv + tech_fee)
                spend_kf = three_days_cpv / (three_days_cpv + tech_fee)

            elif goal_type_id == SalesForceGoalType.CPM:
                impressions = stats_total["impressions"] or 0
                total_cpm = self.DEFAULT_AVERAGE_CPM \
                    if stats_total["cpm"] is None \
                    else stats_total["cpm"]
                three_days_cpm = self.DEFAULT_AVERAGE_CPM \
                    if stats_3days["cpm"] is None \
                    else stats_3days["cpm"]
                client_cost_spent = impressions / 1000 * (total_cpm + tech_fee)
                spend_kf = three_days_cpm / (three_days_cpm + tech_fee)
            else:
                client_cost_spent = spend_kf = 0

            client_cost_remaining = total_cost * allocation_ko \
                                    - client_cost_spent

            days_remain = (flight["end"] - last_day).days + 1
            if days_remain > 0:
                today_budget = spend_kf * client_cost_remaining / days_remain
        elif goal_type_id in (SalesForceGoalType.CPV, SalesForceGoalType.CPM):
            delivery_field = "video_views" \
                if goal_type_id == SalesForceGoalType.CPV else "impressions"
            today_units = self.get_today_goal(flight["plan_units"] * allocation_ko,
                                              stats_total[delivery_field],
                                              flight["end"],
                                              last_day)

            yesterday = last_day - timedelta(days=1)
            yesterdays_stats = get_stats_from_flight(flight,
                                                     campaign_id=campaign_id,
                                                     start=yesterday,
                                                     end=yesterday)
            if goal_type_id == SalesForceGoalType.CPV:
                cpv = self.DEFAULT_AVERAGE_CPV if yesterdays_stats[
                                                      "cpv"] is None else \
                    yesterdays_stats["cpv"]
                today_budget = cpv * today_units
            else:
                cpm = self.DEFAULT_AVERAGE_CPM \
                    if yesterdays_stats["cpm"] is None \
                    else yesterdays_stats["cpm"]
                today_budget = cpm * today_units / 1000

        return today_units, today_budget

    def get_chart_data(self, *_, flights, before_yesterday_stats=None,
                       allocation_ko=1, campaign_id=None):

        flights = [f for f in flights if None not in (
            f["start"], f["end"], f["placement__goal_type_id"])]
        if not flights:
            goal_type_id = None
        else:
            goal_types = list(
                set(f["placement__goal_type_id"] for f in flights))
            assert len(
                goal_types) == 1, "We must have a chart set for every goal type"
            goal_type_id = goal_types[0]

        #
        sum_today_budget = yesterday_cost = 0
        targeting = dict(impressions=0, video_views=0, clicks=0,
                         video_impressions=0)

        yesterday_views = yesterday_impressions = 0
        today_goal_views = today_goal_impressions = 0
        for f in flights:
            stats = f["campaigns"][campaign_id] if campaign_id else f

            if f["start"] <= self.today <= f["end"]:
                today_units, today_budget = self.get_pacing_goal_for_date(
                    f, self.today, allocation_ko=allocation_ko,
                    campaign_id=campaign_id)

                if goal_type_id == SalesForceGoalType.CPV:
                    today_goal_views += today_units
                elif goal_type_id == SalesForceGoalType.CPM:
                    today_goal_impressions += today_units

                sum_today_budget += today_budget

            if goal_type_id == SalesForceGoalType.CPV:
                yesterday_views += stats["yesterday_delivery"]
            elif goal_type_id == SalesForceGoalType.CPM:
                yesterday_impressions += stats["yesterday_delivery"]

            yesterday_cost += stats["yesterday_cost"]

            for k, v in targeting.items():
                targeting[k] = v + stats[k]

        yesterday_units = yesterday_views + yesterday_impressions
        sum_today_units = today_goal_views + today_goal_impressions

        dict_calculate_stats(targeting)
        del targeting['average_cpv'], targeting['average_cpm']

        if goal_type_id == SalesForceGoalType.HARD_COST:
            sum_today_budget = None
            sum_today_units = None
            yesterday_units = None

        data = dict(
            today_goal=sum_today_units,
            today_goal_views=today_goal_views,
            today_goal_impressions=today_goal_impressions,
            today_budget=sum_today_budget,
            yesterday_budget=yesterday_cost,
            yesterday_delivered=yesterday_units,
            yesterday_delivered_views=yesterday_views,
            yesterday_delivered_impressions=yesterday_impressions,
            charts=self.get_flight_charts(flights, allocation_ko,
                                          goal_type_id=goal_type_id,
                                          campaign_id=campaign_id),
            targeting=targeting,


        )

        if before_yesterday_stats is not None:
            before_yesterday_views = before_yesterday_impressions = 0
            if goal_type_id == SalesForceGoalType.CPV:
                before_yesterday_views = before_yesterday_stats.get(
                    "sum_video_views")
            elif goal_type_id == SalesForceGoalType.CPM:
                before_yesterday_impressions = before_yesterday_stats.get(
                    "sum_impressions")
            before_yesterday_units = before_yesterday_impressions \
                                     or before_yesterday_views
            data.update(
                before_yesterday_delivered=before_yesterday_units,
                before_yesterday_budget=before_yesterday_stats.get('sum_cost'),
                before_yesterday_delivered_views=before_yesterday_views,
                before_yesterday_delivered_impressions=before_yesterday_impressions,
            )
        return data

    def get_flight_charts(self, flights, allocation_ko=1, campaign_id=None,
                          goal_type_id=None):
        if goal_type_id is None \
                or goal_type_id == SalesForceGoalType.HARD_COST:
            return None
        charts = []
        if not flights:
            return charts

        min_start = min(f["start"] for f in flights)
        max_end = max(f["end"] for f in flights)

        # set local daily goal for every flight for every day
        for flight in flights:
            flight["daily_goal"] = {}

            dynamic_placement = flight["placement__dynamic_placement"]
            budget_is_goal = dynamic_placement in (
                DynamicPlacementType.BUDGET,
                DynamicPlacementType.RATE_AND_TECH_FEE)

            if dynamic_placement == DynamicPlacementType.RATE_AND_TECH_FEE:
                goal = self.get_budget_to_spend_from_added_fee_flight(
                    flight, allocation_ko=allocation_ko,
                    campaign_id=campaign_id)
            else:
                goal = (flight["plan_units"] or 0) * allocation_ko

            delivery_field_name = self.get_delivery_field_name(flight)
            daily_delivery = defaultdict(int)
            if delivery_field_name:
                for row in flight["daily_delivery"]:
                    date = row["date"]
                    if campaign_id is None or row["campaign_id"] == campaign_id:
                        daily_delivery[date] += row[delivery_field_name] or 0

            sum_pacing = 0
            sum_delivered = 0
            delivery_variance = 0
            for date in get_dates_range(flight["start"], flight["end"]):
                today_units, today_budget = self.get_pacing_goal_for_date(
                    flight, date, allocation_ko=allocation_ko,
                    campaign_id=campaign_id)
                initial_daily_goal = today_budget if budget_is_goal else today_units

                daily_goal = initial_daily_goal + delivery_variance
                if daily_goal + sum_pacing > goal:
                    daily_goal = goal - sum_pacing

                flight["daily_goal"][date] = daily_goal
                sum_pacing += daily_goal

                delivered = daily_delivery.get(date, 0)

                if delivered:
                    delivery_variance = delivered - initial_daily_goal
                    sum_delivered += delivered
                else:
                    delivery_variance = 0
                    sum_delivered += initial_daily_goal

        delivered_chart = []
        pacing_chart = []
        total_pacing = 0
        total_delivered = 0
        for date in get_dates_range(min_start, max_end):
            # plan cumulative chart
            current_flights = [f for f in flights if
                               f["start"] <= date <= f["end"]]
            if current_flights:
                goal_for_today = sum(
                    f["daily_goal"].get(date, 0) for f in current_flights)
            else:
                goal_for_today = 0

            total_pacing += goal_for_today
            pacing_chart.append(
                dict(
                    label=date,
                    value=total_pacing,
                )
            )

            # delivered cumulative chart
            delivered = 0
            for f in current_flights:
                delivery_field_name = self.get_delivery_field_name(f)
                if delivery_field_name:
                    for row in f["daily_delivery"]:
                        if date == row["date"]:
                            if campaign_id is None \
                                    or row["campaign_id"] == campaign_id:
                                delivered += row[delivery_field_name]

            if delivered:
                total_delivered += delivered
                delivered_chart.append(
                    dict(
                        label=date,
                        value=total_delivered,
                    )
                )

        if pacing_chart:
            charts.append(
                dict(
                    title="Ideal Pacing",
                    data=pacing_chart,
                )
            )
        if delivered_chart:
            charts.append(
                dict(
                    title="Daily Deviation",
                    data=delivered_chart,
                )
            )

        return charts

    def get_today_goal(self, goal_items, delivered_items, end, today):
        goal = 0
        days_left = (end - today).days + 1 if end else 0
        if days_left > 0:
            goal = (goal_items - delivered_items) / days_left
            if goal < 0:
                goal = 0
        return goal

    @staticmethod
    def get_flight_delivery_annotate(fields=None, **additional_filters):
        pre_flight_cond = dict(
            campaign__salesforce_placement__flights__start__gt=F("date"),
        )
        flight_cond = dict(
            campaign__salesforce_placement__flights__start__lte=F("date"),
            campaign__salesforce_placement__flights__end__gte=F("date"),
            **additional_filters
        )
        annotate = dict(
            pre_video_views=Sum(
                Case(
                    When(
                        then='video_views',
                        **pre_flight_cond
                    ),
                    output_field=IntegerField()
                )
            ),
            pre_impressions=Sum(
                Case(
                    When(
                        then='impressions',
                        **pre_flight_cond
                    ),
                    output_field=IntegerField()
                )
            ),
            sum_video_views=Sum(
                Case(
                    When(
                        then='video_views',
                        **flight_cond
                    ),
                    output_field=IntegerField()
                )
            ),
            video_impressions=Sum(
                Case(
                    When(
                        campaign__video_views__gt=0,
                        then='impressions',
                        **flight_cond
                    ),
                    output_field=IntegerField()
                )
            ),
            sum_cost=Sum(
                Case(
                    When(
                        then="cost",
                        **flight_cond
                    ),
                    output_field=FloatField()
                )
            ),
            sum_impressions=Sum(
                Case(
                    When(
                        then='impressions',
                        **flight_cond
                    ),
                    output_field=IntegerField()
                )
            ),
            sum_clicks=Sum(
                Case(
                    When(
                        then='clicks',
                        **flight_cond
                    ),
                    output_field=IntegerField()
                )
            ),
            video_clicks=Sum(
                Case(
                    When(
                        campaign__video_views__gt=0,
                        then='clicks',
                        **flight_cond
                    ),
                    output_field=IntegerField()
                )
            ),
        )
        if fields:
            annotate = {k: v for k, v in annotate.items() if k in fields}
        return annotate

    def add_calculated_fields(self, report):
        # border signals
        border = self.borders['margin']
        margin = report["margin"]
        if margin is None or margin >= border[0]:
            margin_quality = 2
            margin_direction = 0
        elif margin >= border[1]:
            margin_quality = 1
            margin_direction = 1
        else:
            margin_quality = 0
            margin_direction = 1

        # pacing=((.8, .9), (1.1, 1.2)),
        low, high = self.borders['pacing']
        pacing = report["pacing"]
        if pacing is None or high[0] >= pacing >= low[1]:
            pacing_quality = 2
            pacing_direction = 0
        elif low[1] > pacing > low[0] or high[1] > pacing > high[0]:
            pacing_quality = 1
            pacing_direction = 1 if pacing < low[1] else -1
        else:
            pacing_quality = 0
            pacing_direction = 1 if pacing <= low[0] else -1

        video_view_rate_quality = 2
        low, high = self.borders['video_view_rate']
        video_view_rate = report['video_view_rate']
        if video_view_rate is not None and video_view_rate < high:
            if video_view_rate < low:
                video_view_rate_quality = 0
            else:
                video_view_rate_quality = 1

        ctr_quality = 2
        low, high = self.borders['ctr']
        ctr = report['ctr']
        if ctr is not None and ctr < high:
            if ctr < low:
                ctr_quality = 0
            else:
                ctr_quality = 1

        report.update(
            margin=margin,
            margin_quality=margin_quality,
            margin_direction=margin_direction,
            pacing_quality=pacing_quality,
            pacing_direction=pacing_direction,
            video_view_rate_quality=video_view_rate_quality,
            ctr_quality=ctr_quality,
            is_completed=report["end"] < self.today if report["end"] else None,
            is_upcoming=report["start"] > self.today if report["start"] else None

        )

    # ## OPPORTUNITIES ## #
    def get_opportunities(self, get):
        queryset = self.get_opportunities_queryset(get)

        # get raw opportunity data
        opportunities = queryset.values(
            "id", "name", "start", "end", "cannot_roll_over",
            "category", "notes",

            "ad_ops_manager__id", "ad_ops_manager__name",
            "ad_ops_manager__email",
            "account_manager__id", "account_manager__name",
            "sales_manager__id", "sales_manager__name",
            "apex_deal",
            "bill_of_third_party_numbers"
        ).annotate(region=F("region_id"))

        # collect ids
        ad_ops_emails = set()
        opportunity_ids = []
        for o in opportunities:
            opportunity_ids.append(o['id'])
            if o['ad_ops_manager__email']:
                ad_ops_emails.add(o['ad_ops_manager__email'])

        # thumbnail
        if ad_ops_emails:
            user_rows = get_user_model().objects.filter(
                email__in=ad_ops_emails,
                profile_image_url__isnull=False,
            ).exclude(profile_image_url="").values("email",
                                                   "profile_image_url")
            thumbnails = {r['email']: r['profile_image_url'] for r in
                          user_rows}
        else:
            thumbnails = {}

        flight_opp_key = "placement__opportunity_id"
        placement_opp_key = "opportunity_id"
        flights_data = self.get_flights_data(
            placement__opportunity_id__in=opportunity_ids)
        placements_data = self.get_placements_data(
            opportunity_id__in=opportunity_ids)
        all_flights = defaultdict(list)
        all_placements = defaultdict(list)

        for f in flights_data:
            all_flights[f[flight_opp_key]].append(f)

        for p in placements_data:
            all_placements[p[placement_opp_key]].append(p)

        # prepare response
        for o in opportunities:
            today = self.today
            if o["start"] > today:
                status = "upcoming"
            elif o["end"] < today:
                status = "completed"
            else:
                status = "active"
            o["status"] = status
            goal_type_ids = Opportunity.objects.get(pk=o["id"]).goal_type_ids
            o['goal_type_ids'] = goal_type_ids

            flights = all_flights[o["id"]]
            placements = all_placements[o["id"]]

            delivery_stats = self.get_delivery_stats_from_flights(flights)
            o.update(delivery_stats)

            plan_stats = self.get_plan_stats_from_flights(flights)
            o.update(plan_stats)

            o["pacing"] = self.get_pacing_from_flights(flights)
            o["margin"] = self.get_margin_from_flights(flights, o["cost"],
                                                       o["plan_cost"])

            chart_data = {}
            cpv_flights = [f for f in flights if
                           f["placement__goal_type_id"] == 1]
            if cpv_flights:
                chart_data["cpv"] = self.get_chart_data(flights=cpv_flights)

            cpm_flights = [f for f in flights if
                           f["placement__goal_type_id"] == 0]
            if cpm_flights:
                chart_data["cpm"] = self.get_chart_data(flights=cpm_flights)

            budget_flights = [
                f for f in flights
                if
                f["placement__goal_type_id"] == SalesForceGoalType.HARD_COST and
                f["placement__dynamic_placement"] in (
                    DynamicPlacementType.BUDGET,
                    DynamicPlacementType.SERVICE_FEE,
                    DynamicPlacementType.RATE_AND_TECH_FEE)
            ]
            if budget_flights:
                chart_data["budget"] = self.get_chart_data(
                    flights=budget_flights)

            o.update(chart_data=chart_data)

            o['thumbnail'] = thumbnails.get(o['ad_ops_manager__email'])

            o['ad_ops'] = dict(id=o['ad_ops_manager__id'],
                               name=o['ad_ops_manager__name'])
            del o['ad_ops_manager__id'], o['ad_ops_manager__name'], o[
                'ad_ops_manager__email']

            o['am'] = dict(id=o['account_manager__id'],
                           name=o['account_manager__name'])
            del o['account_manager__id'], o['account_manager__name']

            o['sales'] = dict(id=o['sales_manager__id'],
                              name=o['sales_manager__name'])
            del o['sales_manager__id'], o['sales_manager__name']

            region_id = o['region']
            o['region'] = dict(id=region_id, name=SalesForceRegions[region_id]) \
                if region_id is not None else None
            category_id = o['category']
            o['category'] = dict(id=category_id, name=category_id) \
                if category_id is not None else None

            self.add_calculated_fields(o)

            dynamic_placements_types = set(p["dynamic_placement"]
                                           for p in placements)
            o["has_dynamic_placements"] = any([dp in DYNAMIC_PLACEMENT_TYPES
                                               for dp in dynamic_placements_types])
            o["dynamic_placements_types"] = dynamic_placements_types

        return opportunities

    def get_opportunities_queryset(self, get):
        queryset = Opportunity.objects.filter(probability=100)

        start, end = self.get_period_dates(get.get("period"), get.get("start"),
                                           get.get("end"))
        if start and end:
            queryset = queryset.filter(start__lte=end, end__gte=start)

        search = get.get('search')
        if search:
            queryset = queryset.filter(name__icontains=search.strip())

        multi_del = ","
        ad_ops = get.get("ad_ops")
        if ad_ops:
            queryset = queryset.filter(
                ad_ops_manager_id__in=ad_ops.split(multi_del))
        am = get.get("am")
        if am:
            queryset = queryset.filter(
                account_manager_id__in=am.split(multi_del))
        sales = get.get("sales")
        if sales:
            queryset = queryset.filter(
                sales_manager_id__in=sales.split(multi_del))
        category = get.get("category")
        if category:
            queryset = queryset.filter(
                category_id__in=category.split(multi_del))
        goal_type = get.get("goal_type")
        if goal_type:
            queryset = queryset.filter(
                goal_type_id__in=goal_type.split(multi_del))
        region = get.get("region")
        if region:
            queryset = queryset.filter(region_id__in=region.split(multi_del))

        status = get.get("status")
        if status:
            if status == "active":
                queryset = queryset.filter(start__lte=self.today,
                                           end__gte=self.today)
            elif status == "completed":
                queryset = queryset.filter(end__lt=self.today)
            elif status == "upcoming":
                queryset = queryset.filter(start__gt=self.today)
        apex_deal = get.get("apex_deal")
        if apex_deal is not None and apex_deal.isdigit():
            queryset = queryset.filter(apex_deal=bool(int(apex_deal)))
        instance_settings = InstanceSettings()
        if instance_settings.get('global_account_visibility'):
            visible_ids = instance_settings.get('visible_accounts')
            queryset = queryset.filter(
                placements__adwords_campaigns__account_id__in=visible_ids
            )
        return queryset.order_by("name", "id").distinct()

    def get_period_dates(self, period, custom_start, custom_end):
        if period is None:
            return None, None

        class PeriodError(ValueError):
            def __init__(self, p):
                super().__init__("Unknown period: {}".format(p))

        if period == "custom":
            assert custom_start and custom_end, "Start and end dates of a custom period must be provided"
            return custom_start, custom_end

        if period.endswith("month"):
            this_start = self.today.replace(day=1)
            next_start = (this_start + timedelta(days=31)).replace(day=1)

            if period.startswith("this"):
                return this_start, next_start - timedelta(days=1)
            elif period.startswith("next"):
                following_start = (next_start + timedelta(days=31)).replace(
                    day=1)
                return next_start, following_start - timedelta(days=1)
            else:
                raise PeriodError(period)

        if period.endswith("year"):
            this_start = self.today.replace(month=1, day=1)
            next_start = (this_start + timedelta(days=366)).replace(day=1)

            if period.startswith("this"):
                return this_start, next_start - timedelta(days=1)
            elif period.startswith("next"):
                following_start = (next_start + timedelta(days=366)).replace(
                    day=1)
                return next_start, following_start - timedelta(days=1)
            else:
                raise PeriodError(period)

        if period.endswith("quarter"):
            #  1    2    3     4
            # 123  456  789  101112
            this_quarter_num = int(ceil(self.today.month / 3))
            year_of_next_quarter = self.today.year
            if this_quarter_num < 4:
                next_quarter_num = this_quarter_num + 1
            else:
                next_quarter_num = 1
                year_of_next_quarter += 1

            this_start = self.today.replace(
                month=(this_quarter_num - 1) * 3 + 1, day=1)
            next_start = self.today.replace(
                month=(next_quarter_num - 1) * 3 + 1,
                day=1, year=year_of_next_quarter)
            if period.startswith("this"):
                return this_start, next_start - timedelta(days=1)
            elif period.startswith("next"):
                year_of_following_quarter = year_of_next_quarter
                if next_quarter_num < 4:
                    following_quarter_num = next_quarter_num + 1
                else:
                    following_quarter_num = 1
                    year_of_following_quarter += 1
                following_start = self.today.replace(
                    month=(following_quarter_num - 1) * 3 + 1,
                    day=1, year=year_of_following_quarter)
                return next_start, following_start - timedelta(days=1)
            else:
                raise PeriodError(period)
        raise PeriodError(period)

    # ## OPPORTUNITIES ## #

    # ## PLACEMENTS ## #
    def prepare_hard_cost_placement_data(self, placement_dict_data):
        """
        :param placement: dict
        """
        placement_dict_data.update(
            cpm=None, cpv=None, ctr=None, ctr_quality=None, impressions=None,
            pacing=None, pacing_direction=None, pacing_quality=None,
            plan_cmp=None, plan_cpv=None, plan_impressions=None,
            plan_video_views=None, video_view_rate=None,
            video_view_rate_quality=None, video_views=None)
        hardcost_placement_flights = Flight.objects.filter(
            placement_id=placement_dict_data["id"])
        flights_cost_data = hardcost_placement_flights.aggregate(
            total_client_cost=Sum("total_cost"),
            our_cost=Sum("cost"))
        our_cost = flights_cost_data["our_cost"]
        total_client_cost = flights_cost_data["total_client_cost"]
        placement_dict_data.update(cost=our_cost, plan_cost=total_client_cost)
        border = self.borders["margin"]
        margin = get_margin(plan_cost=total_client_cost, cost=our_cost,
                            client_cost=total_client_cost)
        if margin >= border[0]:
            margin_quality = 2
            margin_direction = 0
        elif margin >= border[1]:
            margin_quality = 1
            margin_direction = 1
        else:
            margin_quality = 0
            margin_direction = 1
        placement_dict_data.update(
            margin=margin, margin_quality=margin_quality,
            margin_direction=margin_direction)

    def get_placements(self, opportunity):
        queryset = opportunity.placements.all().order_by("name", "start")

        # get raw  data
        placements = queryset.values(
            "id", "name", "start", "end", "goal_type_id", "ordered_units",
            "ordered_rate", "dynamic_placement",
        ).annotate(plan_cost=F("total_cost"))

        # plan stats
        pl_key = "placement_id"
        flights_data = self.get_flights_data(
            placement__opportunity=opportunity)
        all_flights = defaultdict(list)
        for f in flights_data:
            all_flights[f[pl_key]].append(f)

        for p in placements:
            goal_type_id = p["goal_type_id"]
            p.update(
                goal_type=goal_type_str(goal_type_id),
                is_completed=p["end"] < self.today if p["end"] else None,
                is_upcoming=p["start"] > self.today if p["start"] else None
            )
            if goal_type_id == SalesForceGoalType.HARD_COST:
                self.prepare_hard_cost_placement_data(p)
                continue

            flights = all_flights[p["id"]]

            delivery_stats = self.get_delivery_stats_from_flights(flights)
            p.update(delivery_stats)

            plan_stats = self.get_plan_stats_from_flights(flights)
            p.update(plan_stats)

            p["pacing"] = self.get_pacing_from_flights(flights)
            p["margin"] = self.get_margin_from_flights(flights, p["cost"],
                                                       p["plan_cost"])

            # the  client rate is coming from the placement level
            ordered_rate = p["ordered_rate"]
            del p["ordered_rate"]
            if p["goal_type_id"] == 1:
                p["plan_cpv"] = ordered_rate
                p["plan_cpm"] = None
            elif p["goal_type_id"] == 0:
                p["plan_cpm"] = ordered_rate
                p["plan_cpv"] = None

            self.add_calculated_fields(p)
            del p["ordered_units"]

            chart_data = self.get_chart_data(flights=flights)
            p.update(chart_data)

        return placements

    # ## PLACEMENTS ## #

    # ## FLIGHTS ## #
    def get_flights(self, placement):
        flights_data = self.get_flights_data(placement=placement)

        id_field = "campaign__salesforce_placement__flights__id"

        campaign_stats_qs = CampaignStatistic.objects.filter(
            campaign__salesforce_placement=placement)
        all_aw_before_yesterday_stats = campaign_stats_qs.values(
            id_field).order_by(id_field).annotate(
            **self.get_flight_delivery_annotate(
                ("sum_video_views", "sum_impressions", "sum_cost"),
                date=self.yesterday - timedelta(days=1),
            )
        )
        all_aw_before_yesterday_stats = {i[id_field]: i for i in
                                         all_aw_before_yesterday_stats}
        is_budget_dynamic_placement = placement.dynamic_placement \
                                      == DynamicPlacementType.BUDGET

        flights = []
        for f in flights_data:
            flight = dict(
                id=f["id"], name=f["name"], start=f["start"], end=f["end"],
                plan_cost=f["total_cost"], margin=None, pacing=None
            )
            f_data = [f]

            plan_stats = self.get_plan_stats_from_flights(f_data)
            flight.update(plan_stats)

            delivery_stats = self.get_delivery_stats_from_flights(f_data)
            flight.update(delivery_stats)

            if not is_budget_dynamic_placement:
                flight["pacing"] = self.get_pacing_from_flights(f_data)
                flight["margin"] = self.get_margin_from_flights(
                    f_data, flight["cost"], flight["plan_cost"])

            # chart data
            before_yesterday_stats = all_aw_before_yesterday_stats.get(f['id'],
                                                                       {})

            chart_data = self.get_chart_data(flights=[f],
                                             before_yesterday_stats=before_yesterday_stats)
            flight.update(chart_data)

            self.add_calculated_fields(flight)
            flights.append(flight)

        return flights

    # ## FLIGHTS ## #

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
                for c in campaigns:
                    c['goal_allocation'] = def_allocation

        # flights for plan (we shall use them for all the plan stats calculations)
        # we take them all so the over-delivery is calculated
        all_placement_flights = self.get_flights_data(
            placement=flight.placement)
        flights_data = [f for f in all_placement_flights if
                        f["id"] == flight.id]

        for c in campaigns:
            allocation_ko = c['goal_allocation'] / 100
            kwargs = dict(allocation_ko=allocation_ko, campaign_id=c["id"])

            plan_stats = self.get_plan_stats_from_flights(flights_data,
                                                          **kwargs)
            c.update(plan_stats)

            delivery_stats = self.get_delivery_stats_from_flights(flights_data,
                                                                  campaign_id=
                                                                  c["id"])
            c.update(delivery_stats)

            c["pacing"] = self.get_pacing_from_flights(flights_data, **kwargs)
            c["margin"] = self.get_margin_from_flights(flights_data, c["cost"],
                                                       c["plan_cost"],
                                                       **kwargs)

            chart_data = self.get_chart_data(flights=flights_data, **kwargs)
            c.update(chart_data)

            self.add_calculated_fields(c)

        return campaigns
    # ## CAMPAIGNS ## #


def get_stats_from_flight(flight, start=None, end=None, campaign_id=None):
    daily_delivery = flight["daily_delivery"]
    if campaign_id:
        daily_delivery = (e for e in daily_delivery if
                          e["campaign_id"] == campaign_id)
    if start:
        daily_delivery = (e for e in daily_delivery if e["date"] >= start)
    if end:
        daily_delivery = (e for e in daily_delivery if e["date"] <= end)

    delivery = dict(cost=0, impressions=0, video_views=0)
    for s in daily_delivery:
        delivery["cost"] += s["cost"] or 0
        delivery["impressions"] += s["impressions"] or 0
        delivery["video_views"] += s["video_views"] or 0
    delivery["cpm"] = get_average_cpm(delivery["cost"],
                                      delivery["impressions"])
    delivery["cpv"] = get_average_cpv(delivery["cost"],
                                      delivery["video_views"])
    return delivery
