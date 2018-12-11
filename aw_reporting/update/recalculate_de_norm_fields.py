import logging
from collections import defaultdict
from functools import reduce

from django.conf import settings
from django.db.models import Case
from django.db.models import Count
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.db.models import When
from django.db.models.functions import Coalesce

from aw_reporting.models import ALL_AGE_RANGES
from aw_reporting.models import ALL_DEVICES
from aw_reporting.models import ALL_GENDERS
from aw_reporting.models import ALL_PARENTS
from aw_reporting.models import AdGroup
from aw_reporting.models import Campaign
from aw_reporting.models.ad_words.statistic import ModelDenormalizedFields
from utils.lang import flatten
from utils.lang import pick_dict

logger = logging.getLogger(__name__)

MODELS_WITH_ACCOUNT_ID_REF = (
    (Campaign, "account_id"),
    (AdGroup, "campaign__account_id"),
)


def recalculate_de_norm_fields_for_account(account_id):
    for model, account_ref in MODELS_WITH_ACCOUNT_ID_REF:
        filter_dict = {
            "de_norm_fields_are_recalculated": False,
            account_ref: account_id,
        }
        queryset = model.objects \
            .filter(**filter_dict) \
            .order_by("id")
        if not settings.IS_TEST:
            logger.debug(
                "Calculating de-norm fields. Model={}, account_id={}".format(
                    model.__name__,
                    account_id
                ))

        ag_link = "ad_groups__" if model is Campaign else ""
        items = queryset.values("id")

        data = items.annotate(
            min_date=Min("statistics__date"),
            max_date=Max("statistics__date"),
        )
        sum_statistic_map = _get_sum_statistic_map(items)
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

        # Targeting data
        audience_data = items.annotate(
            count=Count("{}audiences__audience_id".format(ag_link)),
        )
        audience_data = {e["id"]: e["count"] for e in audience_data}

        keyword_data = items.annotate(
            count=Count("{}keywords__keyword".format(ag_link)),
        )
        keyword_data = {e["id"]: e["count"] for e in keyword_data}

        channel_data = items.annotate(
            count=Count("{}channel_statistics__id".format(ag_link)),
        )
        channel_data = {e["id"]: e["count"] for e in channel_data}

        video_data = items.annotate(
            count=Count("{}managed_video_statistics__id".format(ag_link)),
        )
        video_data = {e["id"]: e["count"] for e in video_data}

        rem_data = items.annotate(
            count=Count("{}remark_statistic__remark_id".format(ag_link)),
        )
        rem_data = {e["id"]: e["count"] for e in rem_data}

        topic_data = items.annotate(
            count=Count("{}topics__topic_id".format(ag_link)),
        )
        topic_data = {e["id"]: e["count"] for e in topic_data}

        update = {}
        for i in data:
            uid = i["id"]
            sum_stats = sum_statistic_map.get(uid, {})
            stats = stats_by_id[uid]
            update[uid] = dict(
                de_norm_fields_are_recalculated=True,

                min_stat_date=i["min_date"],
                max_stat_date=i["max_date"],

                **sum_stats,
                **stats,

                has_interests=bool(audience_data.get(uid)),
                has_keywords=bool(keyword_data.get(uid)),
                has_channels=bool(channel_data.get(uid)),
                has_videos=bool(video_data.get(uid)),
                has_remarketing=bool(rem_data.get(uid)),
                has_topics=bool(topic_data.get(uid)),
            )

        for uid, updates in update.items():
            model.objects.filter(id=uid).update(**updates)


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
        "clicks_end_cap"
        "clicks_website",
        "cost",
        "impressions",
        "video_views",
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
