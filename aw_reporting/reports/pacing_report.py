# pylint: disable=too-many-lines
from collections import Counter
from collections import defaultdict
from datetime import timedelta
from distutils.util import strtobool
from math import ceil
import statistics

from django.contrib.auth import get_user_model
from django.db.models import Case
from django.db.models import Count
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Max
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When
from django.http import QueryDict
from django.utils import timezone

from aw_reporting.calculations.margin import get_margin_from_flights
from aw_reporting.calculations.margin import get_minutes_run_and_total_minutes
from aw_reporting.models import Account
from aw_reporting.models import Alert
from aw_reporting.models import Campaign
from aw_reporting.models import CampaignStatistic
from aw_reporting.models import Flight
from aw_reporting.models import OpPlacement
from aw_reporting.models import Opportunity
from aw_reporting.models import dict_add_calculated_stats
from aw_reporting.models import get_average_cpm
from aw_reporting.models import get_average_cpv
from aw_reporting.models import get_ctr
from aw_reporting.models import get_ctr_v
from aw_reporting.models import get_video_view_rate
from aw_reporting.models import FlightPacingAllocation
from aw_reporting.models.salesforce_constants import ALL_DYNAMIC_PLACEMENTS
from aw_reporting.models.salesforce_constants import DYNAMIC_PLACEMENT_TYPES
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from aw_reporting.models.salesforce_constants import SalesForceGoalTypes
from aw_reporting.models.salesforce_constants import goal_type_str
from aw_reporting.models.salesforce_constants import FlightAlert
from aw_reporting.models.salesforce_constants import PlacementAlert
from aw_reporting.update.recalculate_de_norm_fields import FLIGHTS_DELIVERY_ANNOTATE
from aw_reporting.utils import get_dates_range
from utils.datetime import now_in_default_tz


class PacingReportChartId:
    IDEAL_PACING = "ideal_pacing"
    DAILY_DEVIATION = "daily_deviation"
    PLANNED_DELIVERY = "planned_delivery"
    HISTORICAL_GOAL = "historical_goal"


class DefaultRate:
    CPM = 6.25
    CPV = .04


FLIGHT_FIELDS = (
    "budget",
    "cost",
    "end",
    "id",
    "name",
    "ordered_units",
    "placement__dynamic_placement",
    "placement__goal_type_id",
    "placement__opportunity__cpm_buffer",
    "placement__opportunity__cpv_buffer",
    "placement__opportunity__budget",
    "placement__opportunity__cannot_roll_over",
    "placement__opportunity_id",
    "placement__ordered_rate",
    "placement__ordered_rate",
    "placement__placement_type",
    "placement__tech_fee",
    "placement__tech_fee_type",
    "placement__total_cost",
    "placement_id",
    "placement__name",
    "placement__opportunity__name",
    "start",
    "timezone",
    "total_cost",
    "update_time",
)

DELIVERY_FIELDS = ("yesterday_delivery", "video_views", "sum_cost",
                   "video_impressions", "impressions", "yesterday_cost",
                   "video_clicks", "clicks", "delivery", "video_cost",)

MANAGED_SERVICE_FIELDS = ("video_views_100_quartile",)

ZERO_STATS = {f: 0 for f in DELIVERY_FIELDS}


class PacingReport:
    # todo: remove these two properties

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

    def get_flights_delivery_annotate(self):
        flights_delivery_annotate = dict(
            delivery=F("statistic__delivery"),
            impressions=F("statistic__impressions"),
            video_impressions=F("statistic__video_impressions"),
            video_clicks=F("statistic__video_clicks"),
            video_cost=F("statistic__video_cost"),
            video_views=F("statistic__video_views"),
            clicks=F("statistic__clicks"),
            sum_cost=F("statistic__sum_cost"),
        )
        return flights_delivery_annotate

    def get_placements_data(self, **filters):
        queryset = OpPlacement.objects.filter(**filters)
        placement_fields = ("id", "dynamic_placement", "opportunity_id",
                            "goal_type_id")
        raw_data = queryset.values(*placement_fields)
        return raw_data

    # pylint: disable=too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks
    def get_flights_data(self, with_campaigns=False, managed_service_data=False, **filters):
        queryset = Flight.objects.filter(
            start__isnull=False,
            end__isnull=False,
            **filters
        )
        campaign_id_key = "placement__adwords_campaigns__id"
        group_by = ("id",)

        annotate = self.get_flights_delivery_annotate()

        if with_campaigns or managed_service_data:
            queryset = queryset.filter(
                placement__adwords_campaigns__statistics__date__gte=F("start"),
                placement__adwords_campaigns__statistics__date__lte=F("end"),
            )
            annotate = FLIGHTS_DELIVERY_ANNOTATE.copy()
            group_by = ("id", campaign_id_key)
        if managed_service_data:
            annotate["video_views_100_quartile"] = \
                Sum("placement__adwords_campaigns__statistics__video_views_100_quartile")

        raw_data = queryset.values(
            *group_by  # segment by campaigns
        ).order_by(*group_by).annotate(**annotate)
        relevant_flights = Flight.objects.filter(
            start__isnull=False,
            end__isnull=False,
            **filters
        ).annotate(
            update_time=Case(
                When(placement__placement_type=OpPlacement.OUTGOING_FEE_TYPE,
                     then=Value(now_in_default_tz())),
                default=Max("placement__adwords_campaigns__account__update_time")
            ),
            timezone=Max("placement__adwords_campaigns__account__timezone"),
        ).values(
            *FLIGHT_FIELDS)

        data = dict((f["id"], {**f, **ZERO_STATS, **{"campaigns": {}}})
                    for f in relevant_flights)

        delivery_fields = list(DELIVERY_FIELDS)
        if managed_service_data:
            delivery_fields.extend(MANAGED_SERVICE_FIELDS)
        for row in raw_data:
            fl_data = data[row["id"]]
            if with_campaigns:
                fl_data["campaigns"] = fl_data.get("campaigns") or {}

                fl_data["campaigns"][row[campaign_id_key]] = {
                    k: row.get(k) or 0
                    for k in delivery_fields
                }

            for f in delivery_fields:
                fl_data[f] = fl_data.get(f, 0) + (row.get(f) or 0)

        data = sorted(data.values(), key=lambda el: (el["start"], el["name"]))

        for fl in data:
            start, end = fl["start"], fl["end"]
            fl["days"] = (end - start).days + 1 if end and start else 0

            goal_type_id = fl["placement__goal_type_id"]
            cpm_buffer = fl["placement__opportunity__cpm_buffer"]
            cpv_buffer = fl["placement__opportunity__cpv_buffer"]
            if fl["placement__opportunity__budget"] > self.big_budget_border:
                if SalesForceGoalType.CPV == goal_type_id:
                    goal_factor = self.big_goal_factor if cpv_buffer is None else (1 + cpv_buffer / 100)
                elif SalesForceGoalType.CPM == goal_type_id:
                    goal_factor = self.big_goal_factor if cpm_buffer is None else (1 + cpm_buffer / 100)
                else:
                    goal_factor = self.big_goal_factor
            else:
                if SalesForceGoalType.CPV == goal_type_id:
                    goal_factor = self.goal_factor if cpv_buffer is None else (1 + cpv_buffer / 100)
                elif SalesForceGoalType.CPM == goal_type_id:
                    goal_factor = self.goal_factor if cpm_buffer is None else (1 + cpm_buffer / 100)
                else:
                    goal_factor = self.goal_factor
            fl["plan_units"] = 0
            fl["sf_ordered_units"] = 0
            if fl["placement__dynamic_placement"] in (DynamicPlacementType.BUDGET,
                                                      DynamicPlacementType.RATE_AND_TECH_FEE,
                                                      DynamicPlacementType.SERVICE_FEE):
                fl["plan_units"] = fl["total_cost"] or 0
                fl["sf_ordered_units"] = fl["plan_units"]
            elif fl["placement__goal_type_id"] == SalesForceGoalType.HARD_COST:
                fl["plan_units"] = 0
                fl["sf_ordered_units"] = 0
            else:
                fl["plan_units"] = fl["ordered_units"] * goal_factor \
                    if fl["ordered_units"] else 0
                fl["sf_ordered_units"] = fl["ordered_units"] or 0

            fl["recalculated_plan_units"] = fl["plan_units"]

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
                or goal_type_id not in (0, 1):  # Use re-allocation for CPV and CPM placements only for now
                continue

            # first get the over delivery
            over_delivery = 0
            for f in placement_flights:
                f["recalculated_plan_units"] = f["plan_units"]
                diff = f["delivery"] - f["recalculated_plan_units"]
                if diff > 0:  # over-delivery
                    over_delivery += diff
                elif diff < 0 and f["end"] <= self.yesterday:  # under delivery for an ended flight
                    # if we have an ended under-delivered flight,
                    # it can consume his under-delivery from the total over-delivery amount
                    abs_diff = abs(diff)
                    reallocate_to_flight = min(abs_diff, over_delivery)
                    over_delivery -= reallocate_to_flight
                    f["recalculated_plan_units"] -= reallocate_to_flight

            # then reassign between flights that haven"t finished
            if over_delivery:
                not_finished_flights = [f for f in placement_flights if
                                        f["end"] > self.yesterday]
                if not_finished_flights:

                    # recalculate reassignment
                    for fl in not_finished_flights:
                        flight_can_consume = fl["recalculated_plan_units"] - fl["delivery"]
                        if flight_can_consume > 0:  # if it hasn"t reached the plan yet
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
                            fl["recalculated_plan_units"] -= assigned_over_delivery
                            over_delivery -= assigned_over_delivery
        return data

    # pylint: enable=too-many-locals,too-many-branches,too-many-statements,too-many-nested-blocks

    @staticmethod
    def get_delivery_stats_from_flights(flights, campaign_id=None, managed_service_data=False):
        impressions = video_views = cost = clicks = video_views_100_quartile = 0
        video_impressions = video_clicks = video_cost = 0
        aw_update_time = None
        goal_type_ids = set()
        for f in flights:
            goal_type_ids.add(f["placement__goal_type_id"])

            stats = f["campaigns"].get(campaign_id, ZERO_STATS) \
                if campaign_id else f

            impressions += stats["impressions"] or 0
            video_impressions += stats["video_impressions"] or 0
            video_clicks += stats["video_clicks"] or 0
            video_views += stats["video_views"] or 0
            video_views_100_quartile += stats.get('video_views_100_quartile', 0)
            video_cost += stats["video_cost"] or 0
            clicks += stats["clicks"] or 0
            cost += stats["sum_cost"] or 0
            aw_update_time = max(aw_update_time, f["update_time"]) \
                if all([aw_update_time, f["update_time"]]) \
                else (aw_update_time or f["update_time"])

        if SalesForceGoalType.CPV in goal_type_ids:
            ctr = get_ctr_v(video_clicks, video_views)
        else:
            ctr = get_ctr(clicks, impressions)

        goal_type_id = SalesForceGoalType.CPV
        if SalesForceGoalType.CPM in goal_type_ids:
            if SalesForceGoalType.CPV in goal_type_ids:
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
            aw_update_time=aw_update_time,
        )
        if managed_service_data:
            # convert from views (calculated) back to rate (api value)
            video_quartile_100_rate = video_views_100_quartile / impressions if impressions > 0 else 0
            stats['video_quartile_100_rate'] = video_quartile_100_rate
        return stats

    def get_plan_stats_from_flights(self, flights, allocation_ko=1,
                                    campaign_id=None):
        plan_impressions = plan_video_views = None
        impressions = video_views = cpm_cost = cpv_cost = 0
        sum_total_cost = sum_delivery = sum_spent_cost = 0
        current_cost_limit = 0

        for f in flights:
            goal_type_id = f["placement__goal_type_id"]
            dynamic_placement = f["placement__dynamic_placement"]
            placement_type = f["placement__placement_type"]
            plan_units = f["plan_units"] * allocation_ko
            ordered_units = (f["ordered_units"] or 0) * allocation_ko
            total_cost = (f["total_cost"] or 0) * allocation_ko

            if campaign_id:
                stats = f["campaigns"].get(campaign_id, ZERO_STATS)
            else:
                stats = f

            sum_delivery += stats.get("delivery") or 0
            aw_cost = stats.get("sum_cost") or 0

            if dynamic_placement in ALL_DYNAMIC_PLACEMENTS:
                sum_spent_cost += aw_cost
            elif placement_type == OpPlacement.OUTGOING_FEE_TYPE:
                minutes_run, total_minutes = get_minutes_run_and_total_minutes(f)
                if minutes_run and total_minutes and f["cost"]:
                    cost = f["cost"] or 0
                    sum_spent_cost += cost * minutes_run / total_minutes

            elif goal_type_id == SalesForceGoalType.CPV:
                plan_video_views = (plan_video_views or 0) + plan_units
                video_views += ordered_units
                cpv_cost += total_cost

                sum_spent_cost += aw_cost

            elif goal_type_id == SalesForceGoalType.CPM:
                plan_impressions = (plan_impressions or 0) + plan_units
                impressions += ordered_units
                cpm_cost += total_cost

                sum_spent_cost += aw_cost

            elif goal_type_id == SalesForceGoalType.HARD_COST:
                sum_spent_cost += f["cost"] or 0
            sum_total_cost += total_cost
            if f["start"] <= self.today:
                current_cost_limit += total_cost
        result = dict(
            plan_impressions=plan_impressions,
            plan_video_views=plan_video_views,
            plan_cpv=cpv_cost / video_views if video_views else None,
            plan_cpm=cpm_cost / impressions * 1000 if impressions else None,
            cost=sum_spent_cost,
            plan_cost=sum_total_cost,
            current_cost_limit=current_cost_limit
        )
        return result

    def get_margin_from_flights(self, flights, cost, plan_cost,
                                allocation_ko=1, campaign_id=None):
        return get_margin_from_flights(flights, cost, plan_cost, allocation_ko,
                                       campaign_id)

    def add_calculated_fields(self, report):
        # border signals
        border = self.borders["margin"]
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

        low, high = self.borders["pacing"]
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
        low, high = self.borders["video_view_rate"]
        video_view_rate = report["video_view_rate"]
        if video_view_rate is not None and video_view_rate < high:
            if video_view_rate < low:
                video_view_rate_quality = 0
            else:
                video_view_rate_quality = 1

        ctr_quality = 2
        low, high = self.borders["ctr"]
        ctr = report["ctr"]
        if ctr is not None and ctr < high:
            if ctr < low:
                ctr_quality = 0
            else:
                ctr_quality = 1

        report.update(
            margin_quality=margin_quality,
            margin_direction=margin_direction,
            pacing_quality=pacing_quality,
            pacing_direction=pacing_direction,
            video_view_rate_quality=video_view_rate_quality,
            ctr_quality=ctr_quality,
            is_completed=report["end"] < self.today if report["end"] else None,
            is_upcoming=report["start"] > self.today if report[
                "start"] else None

        )

    # pylint: disable=too-many-statements
    def get_opportunities(self, get, user=None, aw_cid=None, managed_service_data=False):
        queryset = self.get_opportunities_queryset(get, user, aw_cid)

        # get raw opportunity data
        opportunities = queryset.values(
            "id", "name", "start", "end", "cannot_roll_over",
            "category", "notes", "aw_cid",

            "ad_ops_manager__id", "ad_ops_manager__name",
            "ad_ops_manager__email",
            "account_manager__id", "account_manager__name",
            "sales_manager__id", "sales_manager__name",
            "apex_deal",
            "billing_server",
            "territory", "margin_cap_required",
            "cpm_buffer", "cpv_buffer",
            "budget"
        )

        # collect ids
        ad_ops_emails = set()
        opportunity_ids = []
        for o in opportunities:
            opportunity_ids.append(o["id"])
            if o["ad_ops_manager__email"]:
                ad_ops_emails.add(o["ad_ops_manager__email"])

        # thumbnail
        if ad_ops_emails:
            user_rows = get_user_model().objects.filter(
                email__in=ad_ops_emails,
                profile_image_url__isnull=False,
            ).exclude(profile_image_url="").values("email",
                                                   "profile_image_url")
            thumbnails = {r["email"]: r["profile_image_url"] for r in
                          user_rows}
        else:
            thumbnails = {}

        flight_opp_key = "placement__opportunity_id"
        placement_opp_key = "opportunity_id"
        flights_data = self.get_flights_data(
            managed_service_data=managed_service_data,
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
            if o["start"] is None or o["end"] is None:
                status = "undefined"
            elif o["start"] > today:
                status = "upcoming"
            elif o["end"] < today:
                status = "completed"
            else:
                status = "active"
            placements = all_placements[o["id"]]
            o["flights"] = flights = all_flights[o["id"]]

            o["status"] = status
            goal_type_ids = sorted(filter(
                lambda g: g is not None,
                {p["goal_type_id"] for p in placements}
            ))
            o["goal_type_ids"] = goal_type_ids

            delivery_stats = self.get_delivery_stats_from_flights(flights, managed_service_data=managed_service_data)
            o.update(delivery_stats)

            plan_stats = self.get_plan_stats_from_flights(flights)
            o.update(plan_stats)

            o["pacing"] = get_pacing_from_flights(flights)
            o["margin"] = self.get_margin_from_flights(flights, o["cost"],
                                                       o["current_cost_limit"])

            o["thumbnail"] = thumbnails.get(o["ad_ops_manager__email"])

            o["ad_ops"] = dict(id=o["ad_ops_manager__id"],
                               name=o["ad_ops_manager__name"])
            del o["ad_ops_manager__id"], o["ad_ops_manager__name"], o[
                "ad_ops_manager__email"]

            o["am"] = dict(id=o["account_manager__id"],
                           name=o["account_manager__name"])
            del o["account_manager__id"], o["account_manager__name"]

            o["sales"] = dict(id=o["sales_manager__id"],
                              name=o["sales_manager__name"])
            del o["sales_manager__id"], o["sales_manager__name"]

            territory = o["territory"]
            o["region"] = dict(id=territory, name=territory) \
                if territory is not None else None
            category_id = o["category"]
            o["category"] = dict(id=category_id, name=category_id) \
                if category_id is not None else None

            self.add_calculated_fields(o)
            o.update(_get_dynamic_placements_summary(placements))

            if o["budget"] > self.big_budget_border:
                o["cpm_buffer"] = (self.big_goal_factor - 1) * 100 if o["cpm_buffer"] is None else o["cpm_buffer"]
                o["cpv_buffer"] = (self.big_goal_factor - 1) * 100 if o["cpv_buffer"] is None else o["cpv_buffer"]
            else:
                o["cpm_buffer"] = (self.goal_factor - 1) * 100 if o["cpm_buffer"] is None else o["cpm_buffer"]
                o["cpv_buffer"] = (self.goal_factor - 1) * 100 if o["cpv_buffer"] is None else o["cpv_buffer"]

            try:
                o["timezone"] = Account.objects.filter(id__in=o["aw_cid"].split(",")).first().managers.first().timezone
            except AttributeError:
                o["timezone"] = None

            alerts = []
            margin = o["margin"]
            try:
                if margin and today <= o["end"] - timedelta(days=7) and margin < 0.1:
                    alerts.append(
                        create_alert("Campaign Under Margin", f"{o['name']} is under margin at {margin}."
                                                              f" Please adjust IMMEDIATELY.")
                    )
                if is_opp_under_margin(margin, today, o["end"]):
                    alerts.append(
                        create_alert("Campaign Under Margin", f"{o['name']} is under margin at {margin}."
                                                              f" Please adjust IMMEDIATELY.")
                    )
            except TypeError:
                pass
            o["alerts"] = alerts
            # Get account performance with Opportunity.aw_cid
            aw_ids = o["aw_cid"].split(",") if o["aw_cid"] else []
            accounts = Account.objects.filter(id__in=aw_ids)
            try:
                o["active_view_viewability"] = statistics.mean(a.active_view_viewability for a in accounts)
            except statistics.StatisticsError:
                o["active_view_viewability"] = None
            account_completions = [a.completion_rate for a in accounts]
            try:
                completion_avg = {
                    key: sum(comp[key] for comp in account_completions) / len(account_completions)
                    for key in account_completions[0].keys()
                }
                o["completion_rates"] = completion_avg
            except IndexError:
                o["completion_rates"] = {}

        return opportunities

    # pylint: enable=too-many-statements

    def get_opportunities_queryset(self, get, user, aw_cid):
        if not isinstance(get, QueryDict):
            query_dict_get = QueryDict("", mutable=True)
            query_dict_get.update(get)
            get = query_dict_get

        queryset = Opportunity.objects.get_queryset_for_user(user) \
            .filter(probability=100)

        if aw_cid is not None:
            queryset = queryset.filter(aw_cid__in=aw_cid)

        start, end = self.get_period_dates(get.get("period"), get.get("start"),
                                           get.get("end"))
        if start and end:
            queryset = queryset.filter(start__lte=end, end__gte=start)

        ids = get.get("ids")
        if ids:
            queryset = queryset.filter(id__in=ids)

        watch = get.get("watch", False)
        if strtobool(str(watch)):
            queryset = queryset.filter(id__in=user.watch.values_list("opportunity_id", flat=True))

        search = get.get("search")
        if search:
            queryset = queryset.filter(name__icontains=search.strip())

        ad_ops = get.getlist("ad_ops")
        if ad_ops:
            queryset = queryset.filter(
                ad_ops_manager_id__in=ad_ops)
        am = get.getlist("am")
        if am:
            queryset = queryset.filter(
                account_manager_id__in=am)
        sales = get.getlist("sales")
        if sales:
            queryset = queryset.filter(
                sales_manager_id__in=sales)
        category = get.getlist("category")
        if category:
            queryset = queryset.filter(
                category_id__in=category)
        # fixme: remove Opportunity.goal_type_id
        goal_type = get.getlist("goal_type")
        if goal_type:
            queryset = queryset.filter(
                goal_type_id__in=goal_type)
        region = get.getlist("region")
        if region:
            queryset = queryset.filter(territory__in=region)

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

        queryset = queryset \
            .annotate(campaigns=Count("placements__adwords_campaigns")) \
            .exclude(campaigns__lte=0)

        return queryset.order_by("name", "id").distinct()

    # pylint: disable=too-many-statements,too-many-branches,too-many-return-statements
    def get_period_dates(self, period, custom_start, custom_end):
        if period is None or period == "custom":
            return custom_start, custom_end

        class PeriodError(ValueError):
            def __init__(self, p):
                super().__init__("Unknown period: {}".format(p))

        if period.endswith("month"):
            this_start = self.today.replace(day=1)
            next_start = (this_start + timedelta(days=31)).replace(day=1)

            if period.startswith("this"):
                return this_start, next_start - timedelta(days=1)
            if period.startswith("next"):
                following_start = (next_start + timedelta(days=31)).replace(
                    day=1)
                return next_start, following_start - timedelta(days=1)
            raise PeriodError(period)

        if period.endswith("year"):
            this_start = self.today.replace(month=1, day=1)
            next_start = (this_start + timedelta(days=366)).replace(day=1)

            if period.startswith("this"):
                return this_start, next_start - timedelta(days=1)
            if period.startswith("next"):
                following_start = (next_start + timedelta(days=366)).replace(
                    day=1)
                return next_start, following_start - timedelta(days=1)
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
            if period.startswith("next"):
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
            raise PeriodError(period)
        raise PeriodError(period)

    # pylint: enable=too-many-statements,too-many-branches,too-many-return-statements

    # todo: remove this method. Calculate these vales on general logic. Ensure that FE can handle them.
    def _set_none_hard_cost_properties(self, placement_dict_data):
        placement_dict_data.update(
            cpm=None, cpv=None, ctr=None, ctr_quality=None, impressions=None,
            pacing=None, pacing_direction=None, pacing_quality=None,
            plan_cmp=None, plan_cpv=None, plan_impressions=None,
            plan_video_views=None, video_view_rate=None,
            video_view_rate_quality=None, video_views=None)

    def get_placements(self, opportunity):
        today = timezone.now().date()
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

            # placement ordered units changed
            alerts = []
            sf_alerts = {
                alert.code: alert.message for alert in Alert.objects.filter(record_id=p["id"])
            }
            try:
                if p["end"] >= today and PlacementAlert.ORDERED_UNITS_CHANGED.value in sf_alerts:
                    short = "Ordered Units Changed"
                    detail = f"{p['name']} - Ordered units were changed from changed " \
                             f"from {sf_alerts[PlacementAlert.ORDERED_UNITS_CHANGED.value]}"
                    alerts.append(create_alert(short, detail))
            except TypeError:
                pass
            p["alerts"] = alerts

        return placements

    # ## PLACEMENTS ## #

    # ## FLIGHTS ## #
    def get_flights(self, placement):
        today = timezone.now().date()
        flights_data = self.get_flights_data(placement=placement)
        populate_daily_delivery_data(flights_data)

        id_field = "campaign__salesforce_placement__flights__id"
        campaign_stats_qs = CampaignStatistic.objects.filter(
            date=self.yesterday - timedelta(days=1),
            date__gte=F("campaign__salesforce_placement__flights__start"),
            date__lte=F("campaign__salesforce_placement__flights__end"),
            campaign__salesforce_placement=placement)
        all_aw_before_yesterday_stats = campaign_stats_qs \
            .values(id_field) \
            .order_by(id_field) \
            .annotate(**get_flight_delivery_annotate(("sum_video_views", "sum_impressions", "sum_cost"), ))
        all_aw_before_yesterday_stats = {i[id_field]: i for i in
                                         all_aw_before_yesterday_stats}

        flights = []
        for f in flights_data:
            tech_fee = float(f["placement__tech_fee"]) \
                if f["placement__tech_fee"] else None
            dynamic_placement = f["placement__dynamic_placement"]
            flight = dict(
                id=f["id"], name=f["name"], start=f["start"], end=f["end"],
                plan_cost=f["total_cost"], margin=None, pacing=None, delivery=f["delivery"],
                dynamic_placement=dynamic_placement,
                tech_fee=tech_fee, goal_type_id=f["placement__goal_type_id"],
                plan_units=f["plan_units"]
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
            before_yesterday_stats = all_aw_before_yesterday_stats.get(f["id"],
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

            try:
                # Get flight projected budget
                if f["placement__goal_type_id"] is SalesForceGoalType.CPM:
                    flight["projected_budget"] = flight["goal"] / 1000 * flight["cpm"]
                else:
                    flight["projected_budget"] = flight["goal"] * flight["cpv"]
                flight["pacing_allocations"] = get_flight_pacing_allocation_ranges(flight["id"])[1]
            except TypeError:
                flight["projected_budget"] = 0
                flight["pacing_allocations"] = []

            flight["budget"] = f["budget"]

            f["projected_budget"] = flight["projected_budget"]
            pacing_goal_charts = get_flight_historical_pacing_chart(f)
            flight.update(pacing_goal_charts)

            alerts = []
            delivery_alert = None
            try:
                delivery_percentage = f["delivery"] / f["plan_units"]
            except (TypeError, ZeroDivisionError):
                delivery_percentage = None
            if delivery_percentage is not None:
                if delivery_percentage >= 1.0 and f["end"] >= today:
                    delivery_alert = "100%"
                elif delivery_percentage >= 0.8 and f["end"] >= today:
                    delivery_alert = "80%"
                if delivery_alert:
                    short = f"Unit Progress at {delivery_alert}"
                    detail = f"{f['name']} in {f['placement__opportunity__name']} - {f['placement__name']} " \
                             f"has delivered {delivery_alert} of its ordered units"
                    alerts.append(create_alert(short, detail))

            pacing_alert = None
            flight_pacing = flight["pacing"]
            if flight_pacing is not None:
                if flight["pacing"] > 1.1:
                    pacing_alert = "over pacing by 10%"
                elif flight["pacing"] < 0.9:
                    pacing_alert = "under pacing by 10%"
                if pacing_alert:
                    short = "Campaign Under / Overpacing"
                    detail = f"The flight {f['name']} is {pacing_alert} and " \
                             f"ends on {f['end']}. Please check and adjust IMMEDIATELY."
                    alerts.append(create_alert(short, detail))

            sf_alerts = {
                alert.code: alert.message for alert in Alert.objects.filter(record_id=flight["id"])
            }
            if flight["end"] and flight["end"] >= today and FlightAlert.DATES_CHANGED.value in sf_alerts:
                short = "Flight Dates Changed"
                detail = f"{f['placement__name']} - {f['name']} - Flight dates have been " \
                         f"changed from {sf_alerts[FlightAlert.DATES_CHANGED.value]}"
                alerts.append(create_alert(short, detail))

            flight["alerts"] = alerts
            flights.append(flight)

        return flights

    # ## FLIGHTS ## #

    # ## CAMPAIGNS ## #
    def get_campaigns(self, flight, status=None):
        queryset = Campaign.objects.filter(
            salesforce_placement__flights=flight)

        # status = "serving" | "paused" | "ended"
        if status:
            if isinstance(status, str):
                queryset = queryset.filter(status=status)
            else:
                queryset = queryset.filter(status__in=status)

        # flights for plan (we shall use them for all the plan stats calculations)
        # we take them all so the over-delivery is calculated
        all_placement_flights = self.get_flights_data(
            placement=flight.placement, with_campaigns=True)
        flights_data = [f for f in all_placement_flights if
                        f["id"] == flight.id]
        populate_daily_delivery_data(flights_data)

        campaigns = set_campaign_allocations(queryset)
        try:
            flight_daily_budget = get_flight_daily_budget(flight)
        except (KeyError, TypeError):
            flight_daily_budget = 0

        for c in campaigns:
            allocation_ko = c["goal_allocation"] / 100
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
            c["flight_budget"] = flight.budget
            c["flight_daily_budget"] = flight_daily_budget

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


def get_today_goal(goal_items, delivered_items, end, today):
    goal = 0
    days_left = (end - today).days + 1 if end else 0
    if days_left > 0:
        goal = (goal_items - delivered_items) / days_left
        if goal < 0:
            goal = 0
    return goal


def get_yesterday_delivery(flights, today):
    yesterday = today - timedelta(days=1)
    flight_ids = [flight["id"] for flight in flights]
    flights_yesterday_delivery = Flight.objects.filter(
        pk__in=flight_ids,
        placement__adwords_campaigns__statistics__date__gte=F("start"),
        placement__adwords_campaigns__statistics__date__lte=F("end"),
    ).annotate(
        yesterday_cost=Sum(
            Case(
                When(
                    placement__adwords_campaigns__statistics__date=yesterday,
                    then=F("placement__adwords_campaigns__statistics__cost"),
                ),
                default=0,
            ),
        ),
        yesterday_delivery=Sum(
            Case(
                When(
                    placement__adwords_campaigns__statistics__date=yesterday,
                    then=Case(
                        When(
                            placement__goal_type_id=Value(SalesForceGoalType.CPM),
                            then=F("placement__adwords_campaigns__statistics__impressions"),
                        ),
                        When(
                            placement__goal_type_id=Value(SalesForceGoalType.CPV),
                            then=F("placement__adwords_campaigns__statistics__video_views"),
                        ),
                        When(
                            placement__dynamic_placement__in=[
                                DynamicPlacementType.BUDGET,
                                DynamicPlacementType.RATE_AND_TECH_FEE],
                            then=F("placement__adwords_campaigns__statistics__cost"),
                        ),
                        output_field=FloatField(),
                    ),
                ),
                default=0,
            ),
        ),
    ).values("id", "yesterday_cost", "yesterday_delivery")
    return {flight["id"]: flight for flight in flights_yesterday_delivery}


# pylint: disable=too-many-locals
def get_chart_data(*_, flights, today, before_yesterday_stats=None,
                   allocation_ko=1, campaign_id=None, cpm_buffer=0, cpv_buffer=0):
    flights = [f for f in flights if None not in (f["start"], f["end"])]
    flights_yesterday_delivery_map = get_yesterday_delivery(flights, today)
    sum_today_budget = yesterday_cost = 0
    targeting = dict(impressions=0, video_views=0, clicks=0,
                     video_impressions=0)

    yesterday_views = yesterday_impressions = 0
    today_goal_views = today_goal_impressions = 0
    goal = 0
    for f in flights:
        goal_type_id = f["placement__goal_type_id"]
        goal += (f["sf_ordered_units"] or 0) * allocation_ko
        stats = f["campaigns"].get(campaign_id, ZERO_STATS) \
            if campaign_id else f
        yesterday_stats = flights_yesterday_delivery_map.get(f["id"], defaultdict(int))

        if f["start"] <= today <= f["end"]:
            today_units, today_budget = get_pacing_goal_for_today(
                f, today, allocation_ko=allocation_ko,
                campaign_id=campaign_id)

            if goal_type_id == SalesForceGoalType.CPV:
                today_goal_views += today_units
            elif goal_type_id == SalesForceGoalType.CPM:
                today_goal_impressions += today_units

            sum_today_budget += today_budget

        if goal_type_id == SalesForceGoalType.CPV:
            yesterday_views += yesterday_stats["yesterday_delivery"]
        elif goal_type_id == SalesForceGoalType.CPM:
            yesterday_impressions += yesterday_stats["yesterday_delivery"]

        yesterday_cost += yesterday_stats["yesterday_cost"]

        for k, v in targeting.items():
            targeting[k] = v + stats[k]

    yesterday_units = yesterday_views + yesterday_impressions
    sum_today_units = today_goal_views + today_goal_impressions

    dict_add_calculated_stats(targeting)
    del targeting["average_cpv"], targeting["average_cpm"]

    goal_types = set(f["placement__goal_type_id"] for f in flights)
    hard_cost_only = goal_types == {SalesForceGoalType.HARD_COST}
    charts = None
    if hard_cost_only:
        sum_today_budget = None
        sum_today_units = None
        yesterday_units = None
    else:
        charts = get_flight_charts(flights, today, allocation_ko,
                                   campaign_id=campaign_id)
    data = dict(
        today_goal=sum_today_units,
        today_goal_views=today_goal_views,
        today_goal_impressions=today_goal_impressions,
        today_budget=sum_today_budget,
        yesterday_budget=yesterday_cost,
        yesterday_delivered=yesterday_units,
        yesterday_delivered_views=yesterday_views,
        yesterday_delivered_impressions=yesterday_impressions,
        goal=goal,
        charts=charts,
        targeting=targeting,
    )

    if before_yesterday_stats is not None:
        before_yesterday_views = before_yesterday_stats.get("sum_video_views")
        before_yesterday_impressions = before_yesterday_stats.get(
            "sum_impressions")
        data.update(
            before_yesterday_budget=before_yesterday_stats.get("sum_cost"),
            before_yesterday_delivered_views=before_yesterday_views,
            before_yesterday_delivered_impressions=before_yesterday_impressions,
        )
    return data


# pylint: enable=too-many-locals

def get_pacing_goal_for_today(flight, today, allocation_ko=1, campaign_id=None):
    # fixme: requirements inconsistency
    dynamic_placement = flight["placement__dynamic_placement"]
    if dynamic_placement == DynamicPlacementType.RATE_AND_TECH_FEE:
        return get_rate_and_tech_fee_today_goal(flight, today, allocation_ko,
                                                campaign_id)
    return get_pacing_goal_for_date(flight, today, today, allocation_ko,
                                    campaign_id)


def get_rate_and_tech_fee_today_goal(flight, today, allocation_ko=1,
                                     campaign_id=None):
    stats_total = get_stats_from_flight(flight, campaign_id=campaign_id,
                                        end=today - timedelta(days=1))
    goal_type_id = flight["placement__goal_type_id"]
    today_budget = today_units = 0
    total_cost = flight["total_cost"] or 0

    stats_3days = get_stats_from_flight(
        flight, campaign_id=campaign_id,
        start=today - timedelta(days=3),
        end=today - timedelta(days=1),
    )

    tech_fee = float(flight["placement__tech_fee"] or 0)
    if goal_type_id == SalesForceGoalType.CPV:
        video_views = stats_total["video_views"] or 0
        total_cpv = DefaultRate.CPV \
            if stats_total["cpv"] is None \
            else stats_total["cpv"]
        three_days_cpv = DefaultRate.CPV \
            if stats_3days["cpv"] is None \
            else stats_3days["cpv"]
        client_cost_spent = video_views * (total_cpv + tech_fee)
        spend_kf = three_days_cpv / (three_days_cpv + tech_fee)

    elif goal_type_id == SalesForceGoalType.CPM:
        impressions = stats_total["impressions"] or 0
        total_cpm = DefaultRate.CPM \
            if stats_total["cpm"] is None \
            else stats_total["cpm"]
        three_days_cpm = DefaultRate.CPM \
            if stats_3days["cpm"] is None \
            else stats_3days["cpm"]
        client_cost_spent = impressions / 1000 * (total_cpm + tech_fee)
        spend_kf = three_days_cpm / (three_days_cpm + tech_fee)
    else:
        client_cost_spent = spend_kf = 0

    client_cost_remaining = total_cost * allocation_ko \
                            - client_cost_spent

    days_remain = (flight["end"] - today).days + 1
    if days_remain > 0:
        today_budget = spend_kf * client_cost_remaining / days_remain
    return today_units, today_budget


def get_pacing_goal_for_date(flight, date, today, allocation_ko=1,
                             campaign_id=None):
    stats_total = get_stats_from_flight(flight, campaign_id=campaign_id,
                                        end=date - timedelta(days=1))
    last_day = max(min(date, today), flight["start"])
    goal_type_id = flight["placement__goal_type_id"]
    dynamic_placement = flight["placement__dynamic_placement"]
    today_budget = today_units = 0
    total_cost = flight["total_cost"] or 0

    if dynamic_placement in (DynamicPlacementType.BUDGET,
                             DynamicPlacementType.RATE_AND_TECH_FEE,
                             DynamicPlacementType.SERVICE_FEE):
        today_budget = get_today_goal(
            total_cost * allocation_ko,
            stats_total["cost"], flight["end"], last_day)
    elif goal_type_id in (SalesForceGoalType.CPV, SalesForceGoalType.CPM):
        delivery_field = "video_views" \
            if goal_type_id == SalesForceGoalType.CPV else "impressions"
        today_units = get_today_goal(
            flight["plan_units"] * allocation_ko,
            stats_total[delivery_field],
            flight["end"],
            last_day)

        yesterday = last_day - timedelta(days=1)
        yesterdays_stats = get_stats_from_flight(flight,
                                                 campaign_id=campaign_id,
                                                 start=yesterday,
                                                 end=yesterday)
        if goal_type_id == SalesForceGoalType.CPV:
            cpv = DefaultRate.CPV \
                if yesterdays_stats["cpv"] is None \
                else yesterdays_stats["cpv"]
            today_budget = cpv * today_units
        else:
            cpm = DefaultRate.CPM \
                if yesterdays_stats["cpm"] is None \
                else yesterdays_stats["cpm"]
            today_budget = cpm * today_units / 1000
    return today_units, today_budget


# pylint: disable=too-many-branches,too-many-statements,too-many-nested-blocks
def get_flight_charts(flights, today, allocation_ko=1, campaign_id=None):
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
            DynamicPlacementType.SERVICE_FEE,
            DynamicPlacementType.RATE_AND_TECH_FEE)

        delivery_field_name = get_delivery_field_name(flight)
        daily_delivery = defaultdict(int)
        flight["_delivery_field_name"] = delivery_field_name
        if delivery_field_name:
            for row in flight["daily_delivery"]:
                date = row["date"]
                if campaign_id is None or row["campaign_id"] == campaign_id:
                    daily_delivery[date] += row[delivery_field_name] or 0

        for date in get_dates_range(flight["start"], flight["end"]):
            today_units, today_budget = get_pacing_goal_for_date(
                flight, date, today, allocation_ko=allocation_ko,
                campaign_id=campaign_id)
            daily_goal = today_budget if budget_is_goal else today_units
            flight["daily_goal"][date] = daily_goal

    delivered_chart = []
    pacing_chart = []
    delivery_plan_chart = []
    historical_goal_chart = []
    total_pacing = 0
    total_delivered = 0
    total_goal = sum(f["plan_units"] for f in flights)
    recalculated_total_goal = sum(f["recalculated_plan_units"] for f in flights)
    for date in get_dates_range(min_start, max_end):
        # plan cumulative chart
        current_flights = [f for f in flights if
                           f["start"] <= date <= f["end"]]

        if current_flights:
            goal_for_today = sum(
                f["daily_goal"].get(date, 0) for f in current_flights)
        else:
            goal_for_today = 0

        total_pacing = total_delivered + goal_for_today \
            if date <= today else total_pacing + goal_for_today

        pacing_chart.append(
            dict(
                label=date,
                value=min(total_pacing, recalculated_total_goal),
            )
        )

        delivery_plan_chart.append(
            dict(
                label=date,
                value=get_ideal_delivery_for_date(flights, date) * allocation_ko,
            )
        )

        if date <= today:
            historical_goal_chart.append(
                dict(
                    label=date,
                    value=get_historical_goal(flights, date, total_goal, total_delivered) * allocation_ko,
                )
            )

        # delivered cumulative chart
        delivered = 0
        for f in current_flights:
            delivery_field_name = f["_delivery_field_name"]
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
                id=PacingReportChartId.IDEAL_PACING,
                data=pacing_chart,
            )
        )
    if delivered_chart:
        charts.append(
            dict(
                title="Daily Deviation",
                id=PacingReportChartId.DAILY_DEVIATION,
                data=delivered_chart,
            )
        )
    if delivery_plan_chart:
        charts.append(
            dict(
                title="Planned delivery",
                id=PacingReportChartId.PLANNED_DELIVERY,
                data=delivery_plan_chart,
            )
        )
    if historical_goal_chart:
        charts.append(
            dict(
                title="Historical Goal",
                id=PacingReportChartId.HISTORICAL_GOAL,
                data=historical_goal_chart,
            )
        )
    return charts
# pylint: enable=too-many-branches,too-many-statements,too-many-nested-blocks

def get_pacing_from_flights(flights, allocation_ko=1,
                            campaign_id=None):
    goal_type_ids = list(set(flight["placement__goal_type_id"] for flight in flights))
    dynamic_placements = list(
        set(flight["placement__dynamic_placement"] for flight in flights))
    if len(goal_type_ids) == 1 \
        and goal_type_ids[0] == SalesForceGoalType.HARD_COST \
        and len(dynamic_placements) == 1 and \
        dynamic_placements[0] == DynamicPlacementType.SERVICE_FEE:
        pacing = 1
    else:
        total_planned_units = sum_delivery = 0
        for f in flights:
            if f["placement__placement_type"] == OpPlacement.OUTGOING_FEE_TYPE:
                continue
            stats = f["campaigns"].get(campaign_id, ZERO_STATS) \
                if campaign_id else f
            sum_delivery += stats["delivery"] or 0
            minutes_run, total_minutes = get_minutes_run_and_total_minutes(f)
            if minutes_run and total_minutes:
                plan_units = f["plan_units"] * allocation_ko
                total_planned_units += plan_units * minutes_run / total_minutes
        pacing = sum_delivery / total_planned_units \
            if total_planned_units else None
    return pacing


def get_delivery_field_name(flight_dict):
    dynamic_placement_types = (
        DynamicPlacementType.BUDGET,
        DynamicPlacementType.SERVICE_FEE,
        DynamicPlacementType.RATE_AND_TECH_FEE)
    if flight_dict["placement__dynamic_placement"] in dynamic_placement_types:
        return "cost"
    if flight_dict["placement__goal_type_id"] == SalesForceGoalType.CPM:
        return "impressions"
    if flight_dict["placement__goal_type_id"] == SalesForceGoalType.CPV:
        return "video_views"
    return None


def populate_daily_delivery_data(flights):
    placement_ids = set(f["placement_id"] for f in flights)
    campaign_stats_qs = CampaignStatistic.objects.filter(
        date__gte=F("campaign__salesforce_placement__flights__start"),
        date__lte=F("campaign__salesforce_placement__flights__end"),
        campaign__salesforce_placement_id__in=placement_ids)
    fl_id_field = "campaign__salesforce_placement__flights__id"
    raw_aw_daily_stats = campaign_stats_qs \
        .values(fl_id_field, "campaign_id", "date") \
        .order_by(fl_id_field, "campaign_id", "date") \
        .annotate(**{k: v for k, v in get_flight_delivery_annotate().items()
                     if k in ("sum_video_views", "sum_impressions", "sum_cost")}
                  )
    all_aw_daily_stats = defaultdict(list)
    for row in raw_aw_daily_stats:
        all_aw_daily_stats[row[fl_id_field]].append(row)

    for fl in flights:
        fl["daily_delivery"] = [
            dict(
                date=e["date"], campaign_id=e["campaign_id"],
                cost=e["sum_cost"], impressions=e["sum_impressions"],
                video_views=e["sum_video_views"],
            ) for e in all_aw_daily_stats[fl["id"]]
        ]


def get_flight_delivery_annotate(fields=None):
    annotate = dict(
        sum_video_views=Sum("video_views"),
        sum_cost=Sum("cost"),
        sum_impressions=Sum("impressions"),
    )
    if fields:
        annotate = {k: v for k, v in annotate.items() if k in fields}
    return annotate


def _get_dynamic_placements_summary(placements):
    dynamic_placements_types = set(p["dynamic_placement"]
                                   for p in placements
                                   if p["dynamic_placement"] is not None)
    has_dynamic_placements = any([dp in DYNAMIC_PLACEMENT_TYPES
                                  for dp in dynamic_placements_types])

    return dict(
        has_dynamic_placements=has_dynamic_placements,
        dynamic_placements_types=list(dynamic_placements_types)
    )


def get_ideal_delivery_for_date(flights, selected_date):
    ideal_delivery = 0
    started_flights = [flight for flight in flights if flight["start"] <= selected_date]
    for flight in started_flights:
        end_date = min(selected_date, flight["end"])
        total_duration_days = (flight["end"] - flight["start"]).days + 1
        if total_duration_days != 0:
            passed_duration_days = (end_date - flight["start"]).days + 1
            ideal_delivery += flight["plan_units"] / total_duration_days * passed_duration_days

    return ideal_delivery


def get_historical_goal(flights, selected_date, total_goal, delivered):
    started_flights = [f for f in flights if f["start"] <= selected_date]
    not_started_flights = [f for f in flights if f["start"] > selected_date]
    current_max_goal = sum(f["plan_units"] for f in started_flights)
    can_consume = sum(f["plan_units"] for f in not_started_flights)
    over_delivered = max(delivered - current_max_goal, 0)
    return total_goal - min(over_delivered, can_consume)


def get_flight_historical_pacing_chart(flight_data):
    """
    Calculate historical unit and spend actual / goal charts
    To calculate historical recommendations, first get how much was actually delivered for each day, the pacing
    allocations for each date, and how many days are in an allocation range
    Then for each date, get the allocation amount by multiplying today's allocation by the total amount and divide by
    how many days are in the allocation range

    For example for a flight of 1000 units from 01-01-2000 to 01-10-2000, 01-01-2000 - 01-03-2000 may have an allocation
    of 30% and 01-04-2000 - 01-10-2000 may have an allocation of 70%.
    The allocation range 01-01-2000 - 01-03-2000 consists of 3 days with 30% allocated to it (300 units). The goal for
    each day within this range would be 300 units / 3 days, so a daily goal of 100 units
    01-04-2000 - 01-10-2000 consists of 7 days with 70% allocated to this range (700 units), so the daily goal for this
    range would bee 700 units / 7 days = 100 units / day
    :param flight_data:
    :return:
    """
    today = timezone.now().date()
    historical_units_chart = dict(
        id="historical_units",
        title="Historical Units",
        data=[],
    )
    historical_spend_chart = dict(
        id="historical_spend",
        title="Historical Spend",
        data=[],
    )
    today_goal_units = None
    today_goal_spend = None
    goal_mapping = FlightPacingAllocation.get_allocations(flight_data["id"])
    allocation_count = Counter([goal.allocation for goal in goal_mapping.values()])
    delivery_mapping = {}
    for delivery in flight_data["daily_delivery"]:
        # Sum the daily delivery of all campaigns
        try:
            data = delivery_mapping[delivery["date"]]
            data["impressions"] += delivery["impressions"] or 0
            data["cost"] += delivery["cost"] or 0
            data["video_views"] += delivery["video_views"] or 0
        except KeyError:
            delivery_mapping[delivery["date"]] = delivery

    end = min(flight_data["end"], today)
    plan_units = flight_data["plan_units"] if flight_data["plan_units"] else 0
    projected_budget = flight_data["projected_budget"]
    for date in get_dates_range(flight_data["start"], end):
        goal_obj = goal_mapping[date]
        # Get the count of allocation amounts to divide total plan units by
        allocation_date_range_count = allocation_count[goal_obj.allocation]
        goal_units = round(plan_units * goal_obj.allocation / allocation_date_range_count / 100)
        goal_spend = round(projected_budget * goal_obj.allocation / allocation_date_range_count / 100)
        units_key = "impressions" if flight_data["placement__goal_type_id"] is SalesForceGoalType.CPM else "video_views"
        try:
            actual_units = delivery_mapping[date][units_key]
            actual_spend = delivery_mapping[date]["cost"]
            goal_type = flight_data["placement__goal_type_id"]
            margin = get_daily_margin(flight_data["placement__ordered_rate"], actual_units, actual_spend, goal_type)
        except KeyError:
            # If KeyError, Flight did not delivery for the current date being processed
            actual_units = actual_spend = margin = 0
        if date == today:
            # Do not add today's recommendations to chart, provide separate keys for today's values
            today_goal_units = goal_units
            today_goal_spend = goal_spend
            break
        historical_units_chart["data"].append(dict(
            label=date,
            goal=goal_units,
            actual=actual_units,
        ))
        historical_spend_chart["data"].append(dict(
            label=date,
            goal=goal_spend,
            actual=actual_spend,
            margin=margin,
        ))
    try:
        today_goal_units_percent = plan_units / today_goal_units * 100
    except (TypeError, ZeroDivisionError):
        today_goal_units_percent = None
    try:
        today_goal_spend_percent = projected_budget / today_goal_spend * 100
    except (TypeError, ZeroDivisionError):
        today_goal_spend_percent = None
    data = dict(
        historical_units_chart=historical_units_chart,
        historical_spend_chart=historical_spend_chart,
        today_goal_units=today_goal_units,
        today_goal_units_percent=today_goal_units_percent,
        today_goal_spend=today_goal_spend,
        today_goal_spend_percent=today_goal_spend_percent,
    )
    return data


def set_campaign_allocations(campaign_queryset):
    campaigns = campaign_queryset.values(
        "id", "name", "goal_allocation",
    ).order_by("name").annotate(start=F("start_date"), end=F("end_date"))

    total_allocation = sum(campaign["goal_allocation"] for campaign in campaigns)
    # Distribute remaining goal_allocations to campaigns with 0 goal allocation
    if campaigns and total_allocation < 100:
        goal_allocation_remaining = 100
        campaigns_without_allocation = []
        for campaign in campaigns:
            if campaign["goal_allocation"] == 0:
                campaigns_without_allocation.append(campaign)
            else:
                goal_allocation_remaining -= campaign["goal_allocation"]
        # Do not allocate if there are no campaigns without allocations
        try:
            split_allocation = goal_allocation_remaining / len(campaigns_without_allocation)
            for campaign in campaigns_without_allocation:
                campaign["goal_allocation"] = split_allocation
        except ZeroDivisionError:
            pass
    return campaigns


def get_flight_pacing_allocation_ranges(flight_id):
    """
    Formats individual flight pacing allocation dates into a list of date ranges group by allocation values
    :param flight_id:
    :return:
    """
    pacing_allocation_mapping = FlightPacingAllocation.get_allocations(flight_id)
    pacing_allocations = list(pacing_allocation_mapping.values())
    pacing_allocations.sort(key=lambda x: x.date)

    # Get start and end date ranges grouped by allocation value
    pacing_allocation_ranges = []
    curr = 0
    start = pacing_allocations[curr]
    # Find the start and end of each date range allocation
    while curr < len(pacing_allocations):
        step = pacing_allocations[curr]
        if step.is_end or curr == len(pacing_allocations) - 1:
            pacing_allocation_ranges.append({
                "start": start.date,
                "end": step.date,
                "allocation": step.allocation,
            })
            try:
                # Reached end of allocations
                start = pacing_allocations[curr + 1]
            except IndexError:
                break
        curr += 1
    return pacing_allocation_mapping, pacing_allocation_ranges


def get_flight_daily_budget(flight):
    """
    Calculates the daily Flight budget to reach overall goal
    In order to calculate projected daily budget, we need the today's FlightPacingAllocation allocation percentage,
    the number of days that are the same allocation, and the flight's projected budget.
    Once the flight projected budget is calculated, multiply it by today's allocation to determine the percentage
    of the flight budget that should be used to the date range and divide it by how many days are in the current
    date range.
    :param flight:
    :return:
    """
    pacing_allocations, allocation_ranges = get_flight_pacing_allocation_ranges(flight.id)
    today_allocation = pacing_allocations[timezone.now().date()]
    allocation_range_mapping = {
        item["allocation"]: (item["end"] - item["start"]).days for item in allocation_ranges
    }
    days_count = allocation_range_mapping[today_allocation.allocation]

    if flight.placement.goal_type_id is SalesForceGoalType.CPM:
        flight_projected_budget = flight.ordered_units / 1000 * get_average_cpm(flight.cost, flight.delivered_units)
    else:
        flight_projected_budget = flight.ordered_units * get_average_cpv(flight.cost, flight.delivered_units)

    flight_daily_budget = flight_projected_budget * today_allocation.allocation / 100 / days_count
    return flight_daily_budget


def create_alert(short, detail):
    alert = {
        "short": short,
        "detail": detail,
    }
    return alert


def get_daily_margin(client_rate, daily_delivered, daily_cost, goal_type):
    """
    Calculate daily margin
    :param client_rate: Placement CPM or CPV rate
    :param daily_delivered: Units delivered for the day
    :param daily_cost: Total cost of delivery for the day
    :param goal_type: CPM or CPV goal type ID
    :return:
    """
    try:
        if goal_type == SalesForceGoalType.CPM:
            client_cost = client_rate * daily_delivered / 1000
        else:
            # CPV
            client_cost = client_rate * daily_delivered
        margin = (client_cost - daily_cost) / client_cost * 100
    except (TypeError, ZeroDivisionError):
        margin = 0
    return margin


def is_opp_under_margin(margin, today, end):
    """
    Determine if Opportunity is under margin
    :param margin: float
    :param today: date
    :param end: date
    :return: bool
    """
    is_under_margin = False
    try:
        if margin and today <= end - timedelta(days=7) and margin < 0.1:
            is_under_margin = True
    except TypeError:
        pass
    return is_under_margin
