import logging
from collections import defaultdict
from functools import reduce

from django.conf import settings
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Avg
from django.db.models import Case
from django.db.models import Count
from django.db.models import F
from django.db.models import FloatField
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models import Sum
from django.db.models import Value
from django.db.models import When
from django.db.models.functions import Coalesce

from aw_reporting.models import ALL_AGE_RANGES
from aw_reporting.models import ALL_DEVICES
from aw_reporting.models import ALL_GENDERS
from aw_reporting.models import ALL_PARENTS
from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import Flight
from aw_reporting.models import FlightStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models.ad_words.statistic import ModelDenormalizedFields
from aw_reporting.models.salesforce_constants import DynamicPlacementType
from aw_reporting.models.salesforce_constants import SalesForceGoalType
from utils.lang import flatten
from utils.lang import pick_dict

logger = logging.getLogger(__name__)

MODELS_WITH_ACCOUNT_ID_REF = (
    (Campaign, "account_id"),
    (AdGroup, "campaign__account_id"),
)


def recalculate_de_norm_fields_for_account(account_id: Account.id, with_counts=True):
    """
    Update account statistics that must be calculated across many tables
    :param account_id: aw_reporting.models.Account id
    :param with_counts: If _recalculate_de_norm_fields_for_account_statistics should be invoked
    :return:
    """
    _recalculate_de_norm_fields_for_account_campaigns_and_groups(account_id)
    _recalculate_de_norm_fields_for_account_flights(account_id)
    # This function costly as it aggregates on the massive
    # YTChannel and YTVideo statistics tables. These stats do not need to be updated often and will be updated
    # by the update_without_campaigns task which processes accounts on a longer interval
    if with_counts:
        _recalculate_de_norm_fields_for_account_statistics(account_id)


def _recalculate_de_norm_fields_for_account_campaigns_and_groups(account_id):
    for model, account_ref in MODELS_WITH_ACCOUNT_ID_REF:
        filter_dict = {
            "de_norm_fields_are_recalculated": False,
            account_ref: account_id,
        }
        queryset = model.objects.filter(**filter_dict)
        items_ids = list(queryset.values_list("id", flat=True))

        if not settings.IS_TEST:
            logger.debug(
                "Calculating de-norm fields. Model=%s, account_id=%s",
                model.__name__,
                account_id
            )

        ag_link = "ad_groups__" if model is Campaign else ""
        items = model.objects.filter(id__in=items_ids).values("id").order_by("id")

        data = items.annotate(
            min_date=Min("statistics__date"),
            max_date=Max("statistics__date"),
        )
        sum_statistic_map = _get_sum_statistic_map(items)
        avg_statistic_map = _get_avg_statistic_map(items)
        device_data = items.annotate(**_device_annotation())
        gender_data = items.annotate(**_gender_annotation(ag_link))
        age_data = items.annotate(**_age_annotation(ag_link))
        parent_data = items.annotate(**_parent_annotation(ag_link))

        def update_key(aggregator, item, key):
            current_value = aggregator[key]
            current_value.update(item)
            return aggregator

        stats_by_id = reduce(
            lambda res, i: update_key(res, i, i["id"]),
            flatten([device_data, gender_data, age_data, parent_data]),
            defaultdict(dict)
        )

        if model is Campaign:
            ad_group_ids_map = AdGroup.objects.filter(campaign_id__in=items_ids) \
                .values("campaign_id").order_by("campaign_id").annotate(ids=ArrayAgg("id"))
            ad_group_ids_map = {item.get("campaign_id"): item.get("ids") for item in ad_group_ids_map}
        else:
            ad_group_ids_map = {_id: [_id] for _id in items_ids}

        aggregated_data = {}

        for key, ids in ad_group_ids_map.items():
            _item_filter = {"ad_group_id__in": ids}
            aggregated_data[key] = {}

            aggregated_data[key]["has_interests"] = AudienceStatistic.objects.filter(**_item_filter).exists()
            aggregated_data[key]["has_keywords"] = KeywordStatistic.objects.filter(**_item_filter).exists()
            aggregated_data[key]["has_channels"] = YTChannelStatistic.objects.filter(**_item_filter).exists()
            aggregated_data[key]["has_videos"] = YTVideoStatistic.objects.filter(**_item_filter).exists()
            aggregated_data[key]["has_remarketing"] = RemarkStatistic.objects.filter(**_item_filter).exists()
            aggregated_data[key]["has_topics"] = TopicStatistic.objects.filter(**_item_filter).exists()

        update = {}
        for i in data:
            uid = i["id"]

            update[uid] = dict(
                de_norm_fields_are_recalculated=True,

                min_stat_date=i["min_date"],
                max_stat_date=i["max_date"],

                **sum_statistic_map.get(uid, {}),
                **avg_statistic_map.get(uid, {}),
                **stats_by_id[uid],
                **aggregated_data.get(uid, {}),
            )

        for uid, updates in update.items():
            model.objects.filter(id=uid).update(**updates)


def _recalculate_de_norm_fields_for_account_statistics(account_id):
    ad_group_ids = list(AdGroup.objects.filter(campaign__account_id=account_id).values_list("id", flat=True))
    formulas = dict(
        ad_count=Count("ads", distinct=True),
        channel_count=Count("channel_statistics__yt_id", distinct=True),
        video_count=Count("managed_video_statistics__yt_id", distinct=True),
        interest_count=Count("audiences__audience_id", distinct=True),
        topic_count=Count("topics__topic_id", distinct=True),
        keyword_count=Count("keywords__keyword", distinct=True),
    )
    get_queryset = AdGroup.objects.filter(id__in=ad_group_ids)
    set_queryset = Account.objects.filter(pk=account_id)
    data = dict()
    for k, v in formulas.items():
        data_item = get_queryset.aggregate(**{k: v})
        data.update(data_item)
    set_queryset.update(**data)


def _build_boolean_case(ref, value):
    when = When(**{ref: value},
                then=1)
    return Max(Case(when,
                    default=0,
                    output_field=IntegerField()))


def _build_group_aggregation_map(ref, all_values, fields_map):
    return {
        fields_map[value]: _build_boolean_case(ref, value)
        for value in all_values
    }


def _age_annotation(ad_group_link):
    age_ref = "{}age_statistics__age_range_id".format(ad_group_link)

    return _build_group_aggregation_map(age_ref, ALL_AGE_RANGES, ModelDenormalizedFields.AGES)


def _gender_annotation(ad_group_link):
    gender_ref = "{}gender_statistics__gender_id".format(ad_group_link)

    return _build_group_aggregation_map(gender_ref, ALL_GENDERS, ModelDenormalizedFields.GENDERS)


def _parent_annotation(ad_group_link):
    parent_ref = "{}parent_statistics__parent_status_id".format(ad_group_link)

    return _build_group_aggregation_map(parent_ref, ALL_PARENTS, ModelDenormalizedFields.PARENTS)


def _device_annotation():
    device_ref = "statistics__device_id"

    return _build_group_aggregation_map(device_ref, ALL_DEVICES, ModelDenormalizedFields.DEVICES)


def _get_sum_fields(model):
    fields = (
        "clicks",
        "clicks_app_store",
        "clicks_call_to_action_overlay",
        "clicks_cards",
        "clicks_end_cap",
        "clicks_website",
        "cost",
        "impressions",
        "video_views",
        "video_views_25_quartile",
        "video_views_50_quartile",
        "video_views_75_quartile",
        "video_views_100_quartile",
    )
    if model is AdGroup:
        fields = fields + ("engagements", "active_view_impressions")
    return fields


def _get_sum_statistic_map(queryset):
    sum_fields = _get_sum_fields(queryset.model)
    sum_statistic = queryset.annotate(
        **{
            field: Coalesce(Sum("statistics__" + field), 0)
            for field in sum_fields
        }
    )

    sum_statistic_map = {
        stats["id"]: pick_dict(stats, sum_fields)
        for stats in sum_statistic
    }
    return sum_statistic_map


def _get_avg_fields():
    fields = (
        "active_view_viewability",
    )
    return fields


def _get_avg_statistic_map(queryset):
    avg_fields = _get_avg_fields()
    annotations = {}
    for field in avg_fields:
        filters = {}
        if field == "active_view_viewability":
            filters = Q(**{f"statistics__{field}__gt": 0})
        annotations[field] = Coalesce(Avg("statistics__" + field, filter=filters), 0)
    avg_statistics = queryset.annotate(**annotations)
    avg_stats_map = {
        stats["id"]: pick_dict(stats, avg_fields)
        for stats in avg_statistics
    }
    return avg_stats_map


FLIGHTS_DELIVERY_ANNOTATE = dict(
    delivery=Sum(
        Case(
            When(
                placement__dynamic_placement__in=[
                    DynamicPlacementType.BUDGET,
                    DynamicPlacementType.RATE_AND_TECH_FEE],
                then=F("placement__adwords_campaigns__statistics__cost"),
            ),
            When(
                placement__goal_type_id=Value(SalesForceGoalType.CPM),
                then=F("placement__adwords_campaigns__statistics__impressions"),
            ),
            When(
                placement__goal_type_id=Value(SalesForceGoalType.CPV),
                then=F("placement__adwords_campaigns__statistics__video_views"),
            ),
            output_field=FloatField(),
        ),
    ),
    impressions=Sum("placement__adwords_campaigns__statistics__impressions"),
    video_impressions=Sum(
        Case(
            When(
                placement__adwords_campaigns__video_views__gt=Value(0),
                then=F("placement__adwords_campaigns__statistics__impressions"),
            ),
        ),
    ),
    video_clicks=Sum(
        Case(
            When(
                placement__adwords_campaigns__video_views__gt=Value(0),
                then=F("placement__adwords_campaigns__statistics__clicks"),
            ),
        ),
    ),
    video_cost=Sum(
        Case(
            When(
                placement__adwords_campaigns__video_views__gt=Value(0),
                then=F("placement__adwords_campaigns__statistics__cost"),
            ),
        ),
    ),
    video_views=Sum("placement__adwords_campaigns__statistics__video_views"),
    clicks=Sum("placement__adwords_campaigns__statistics__clicks"),
    sum_cost=Sum(
        Case(
            When(
                ~Q(placement__dynamic_placement=DynamicPlacementType.SERVICE_FEE),
                then=F("placement__adwords_campaigns__statistics__cost"),
            ),
        ),
    ),
)


def _recalculate_de_norm_fields_for_account_flights(account_id):
    flights = Flight.objects.filter(
        placement__adwords_campaigns__account_id=account_id,
        placement__adwords_campaigns__statistics__date__gte=F("start"),
        placement__adwords_campaigns__statistics__date__lte=F("end"),
    )
    flights_annotated = flights.annotate(**FLIGHTS_DELIVERY_ANNOTATE)
    for flight in flights_annotated.values():
        defaults = {
            key: flight.get(key) or 0
            for key in FLIGHTS_DELIVERY_ANNOTATE
        }
        FlightStatistic.objects.update_or_create(
            flight_id=flight["id"],
            defaults=defaults
        )
