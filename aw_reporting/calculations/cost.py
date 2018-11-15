from django.db.models import Case
from django.db.models import DateField
from django.db.models import F
from django.db.models import FloatField
from django.db.models import Sum
from django.db.models import When
from django.db.models.functions import Cast
from django.db.models.functions import ExtractDay
from django.db.models.functions import Least

from aw_reporting.models.salesforce import OpPlacement
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from utils.datetime import now_in_default_tz


def get_client_cost(goal_type_id, dynamic_placement, placement_type,
                    ordered_rate, total_cost, tech_fee, start, end,
                    impressions, video_views, aw_cost):
    client_cost = 0
    if placement_type == OpPlacement.OUTGOING_FEE_TYPE:
        client_cost = _get_client_cost_outgoing_fee()
    elif goal_type_id == SalesForceGoalType.HARD_COST:
        client_cost = _get_client_cost_hard_cost(
            start=start, end=end,
            total_cost=total_cost,
        )
    elif dynamic_placement == DynamicPlacementType.RATE_AND_TECH_FEE:
        client_cost = _get_client_cost_rate_and_tech_fee(
            goal_type_id=goal_type_id,
            video_views=video_views,
            impressions=impressions,
            aw_cost=aw_cost,
            tech_fee=tech_fee
        )
    elif dynamic_placement == DynamicPlacementType.BUDGET:
        client_cost = _get_client_cost_budget(aw_cost)
    else:
        client_cost = _get_client_cost_regular_cpm_cpv(
            goal_type_id=goal_type_id,
            impressions=impressions,
            video_views=video_views,
            ordered_rate=ordered_rate
        )
    actualized_client_cost = min(client_cost, total_cost)
    return actualized_client_cost


def get_client_cost_aggregation(campaign_ref="ad_group__campaign"):
    today = now_in_default_tz().date()

    placement_ref = campaign_ref + "__salesforce_placement"
    start_ref = campaign_ref + "__start_date"
    end_ref = campaign_ref + "__end_date"
    start_lte_ref = start_ref + "__lte"
    end_missed_ref = campaign_ref + "__end_date__isnull"

    placement_type_ref = placement_ref + "__placement_type"
    dynamic_placement_ref = placement_ref + "__dynamic_placement"
    goal_type_ref = placement_ref + "__goal_type_id"
    total_cost_ref = placement_ref + "__total_cost"
    ordered_rate_ref = placement_ref + "__ordered_rate"
    ordered_rate_isnull = ordered_rate_ref + "__isnull"
    tech_fee_ref = placement_ref + "__tech_fee"

    impressions_ref = "impressions"
    video_views_ref = "video_views"
    aw_cost_ref = "cost"

    then = "then"
    aggregation = Sum(Case(
        # outgoing fee
        When(**{
            placement_type_ref: OpPlacement.OUTGOING_FEE_TYPE,
            then: 0
        }),
        # hard cost
        When(**{
            goal_type_ref: SalesForceGoalType.HARD_COST,
            start_lte_ref: today,
            end_missed_ref: True,
            then: F(total_cost_ref)
        }),
        When(**{
            goal_type_ref: SalesForceGoalType.HARD_COST,
            start_lte_ref: today,
            then: F(total_cost_ref)
                  / (ExtractDay(F(end_ref) - F(start_ref)) + 1)
                  * (ExtractDay(Least(Cast(today, DateField()),
                                      F(end_ref))
                                - F(start_ref)) + 1)
        }),
        When(**{
            goal_type_ref: SalesForceGoalType.HARD_COST,
            then: 0
        }),
        # rate and tech fee
        When(**{
            dynamic_placement_ref: DynamicPlacementType.RATE_AND_TECH_FEE,
            goal_type_ref: SalesForceGoalType.CPV,
            then: F(aw_cost_ref) + F(video_views_ref) * F(tech_fee_ref)
        }),
        When(**{
            dynamic_placement_ref: DynamicPlacementType.RATE_AND_TECH_FEE,
            goal_type_ref: SalesForceGoalType.CPM,
            then: F(aw_cost_ref) + F(impressions_ref) * F(tech_fee_ref)
        }),
        # budget
        When(**{
            dynamic_placement_ref: DynamicPlacementType.BUDGET,
            then: F(aw_cost_ref)
        }),
        # regular CPM/CPV
        When(**{
            goal_type_ref: SalesForceGoalType.CPM,
            ordered_rate_isnull: False,
            then: F(impressions_ref) / 1000. * F(ordered_rate_ref),
        }),
        When(**{
            goal_type_ref: SalesForceGoalType.CPV,
            ordered_rate_isnull: False,
            then: F(video_views_ref) * F(ordered_rate_ref),
        }),
        output_field=FloatField()
    ))

    return aggregation


def _get_client_cost_outgoing_fee():
    return 0


def _get_client_cost_hard_cost(start, end, total_cost):
    total_cost_or_zero = total_cost or 0
    today = now_in_default_tz().date()
    if start is not None and start > today:
        return 0
    if start is None or end is None:
        return total_cost_or_zero
    total_days = (end - start).days + 1
    days_pass = (min(today, end) - start).days + 1
    return total_cost_or_zero / total_days * days_pass


def _get_client_cost_rate_and_tech_fee(goal_type_id, video_views, impressions, aw_cost, tech_fee):
    video_views = video_views or 0
    impressions = impressions or 0
    units = video_views if goal_type_id == SalesForceGoalType.CPV else impressions
    aw_rate = aw_cost / units if units else 0
    return units * (aw_rate + float(tech_fee))


def _get_client_cost_budget(aw_cost):
    return aw_cost or 0


def _get_client_cost_regular_cpm_cpv(goal_type_id, video_views, impressions, ordered_rate):
    units = _get_payable_units(
        goal_type_id=goal_type_id,
        impressions=impressions,
        video_views=video_views,
    )
    ordered_rate = ordered_rate or 0
    norm_rate = ordered_rate / 1000. if goal_type_id == SalesForceGoalType.CPM \
        else ordered_rate
    return norm_rate * units


def _get_payable_units(goal_type_id, impressions, video_views):
    video_views = video_views or 0
    impressions = impressions or 0
    return video_views if goal_type_id == SalesForceGoalType.CPV else impressions
