from collections import defaultdict
from functools import reduce

from django.db.models import Count, BooleanField, \
    QuerySet
from django.db.models import Q, Min, When, Case, Max, \
    IntegerField
from django.db.models.expressions import CombinedExpression, Value, Combinable

from aw_reporting.models import AdGroup, ParentStatuses, AudienceStatistic, \
    TopicStatistic, Topic, Audience, AgeRanges, \
    Genders, Devices
from aw_reporting.tools.forecast_tool.constants import GENDER_FIELDS, \
    AGE_FIELDS, PARENT_FIELDS, DEVICE_FIELDS, VIDEO_LENGTHS, TARGETING_TYPES
from utils.datetime import now_in_default_tz, quarter_days
from utils.query import build_query_bool, split_request, Operator

CONDITIONS = [
    dict(id="or", name="Or"),
    dict(id="and", name="And"),
]

INTERESTS_MAP = {
    "audience_id": "id",
    "audience__name": "name",
    "audience__type": "type",
}


class ForecastToolFiltering:
    default_condition = "or"

    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.interest_child_cache = dict()
        self.topic_child_cache = dict()
        self.filter_item_ids = None

    @classmethod
    def get_filters(cls):
        start, end = cls._get_default_dates()
        product_types = AdGroup.objects.all().values_list("type", flat=True).order_by("type").distinct()
        product_types = [dict(id=t, name=t) for t in sorted(product_types) if t not in ["", " --", "Standard"]]
        filters = dict(
            quarters=[dict(id=c, name=c) for c in list(sorted(quarter_days.keys()))],
            quarters_condition=CONDITIONS,
            start=start,
            end=end,
            compare_yoy=False,
            product_types=product_types,
            product_types_condition=CONDITIONS,
            targeting_types=[dict(id=i, name=i.capitalize()) for i in TARGETING_TYPES],
            targeting_types_condition=CONDITIONS,
            geo_locations="Use /geo_target_list?search= endpoint to get locations and sent their ids back",
            geo_locations_condition=CONDITIONS,
            ages=list_to_filter(AgeRanges),
            genders=list_to_filter(Genders),
            parents=list_to_filter(ParentStatuses),
            demographic_condition=CONDITIONS,
            topics=[dict(id=i['topic_id'], name=i['topic__name'])
                    for i in TopicStatistic.objects.filter(topic__parent__isnull=True).values(
                        'topic_id', 'topic__name').order_by('topic__name', 'topic_id').distinct()],
            topics_condition=CONDITIONS,
            devices=list_to_filter(Devices),
            devices_condition=CONDITIONS,
            creative_lengths=[
                dict(
                    id=uid,
                    name="{}{} sec{}".format(f, "" if t is None else "-{}".format(t), "+" if t is None else ""))
                for uid, (f, t) in enumerate(VIDEO_LENGTHS)],
            creative_lengths_condition=CONDITIONS,
            **_get_interests_filters()
        )
        return filters

    @staticmethod
    def _get_default_dates():
        today = now_in_default_tz().date()
        return today.replace(day=1, month=1), today

    @staticmethod
    def _merge_item_ids(base_ids, new_ids):
        if new_ids is not None:
            if base_ids is None:
                base_ids = new_ids
            else:
                base_ids &= new_ids
        return base_ids

    def apply(self, queryset):
        return self._filter(queryset)

    def _filter(self, queryset):
        filters = (
            self._filter_by_periods,
            self._filter_by_product_types,
            self._filter_by_targeting_types,
            *self._filter_by_demographic(),
            self._filter_by_geo_targeting,
            self._filter_by_topics,
            self._filter_by_interests,
            self._filter_by_creative_length,
            self._filter_by_devices,
        )
        queryset = split_request(queryset, filters, 1)
        if self.filter_item_ids is not None:
            queryset = queryset.filter(id__in=self.filter_item_ids)
        return queryset.distinct()

    def _filter_by_periods(self, queryset):
        periods = self.kwargs["periods"]
        if len(periods) == 0:
            return queryset, False
        queryset = queryset.annotate(
            min_start=Min("placements__adwords_campaigns__start_date"),
            max_end=Max("placements__adwords_campaigns__end_date"),
        )
        filters = [Q(min_start__lte=end, max_end__gte=start) for start, end in periods]
        query = reduce(
            lambda res, f: res | f,
            filters,
            Q(min_start__isnull=True) | Q(max_end__isnull=True)
        )
        return queryset.filter(query), True

    def _filter_by_product_types(self, queryset):
        product_types = self.kwargs.get("product_types", [])
        if len(product_types) == 0:
            return queryset, False
        product_types_condition = self.kwargs.get("product_types_condition", self.default_condition)
        if product_types_condition == "or":
            queryset = queryset.filter(placements__adwords_campaigns__ad_groups__type__in=product_types)
        elif product_types_condition == "and":
            queryset = reduce(
                lambda qs, pt: qs.filter(
                    placements__adwords_campaigns__ad_groups__type=pt),
                product_types,
                queryset
            )
        return queryset, True

    def _filter_by_targeting_types(self, queryset: QuerySet):
        targeting_types = self.kwargs.get("targeting_types")
        if targeting_types is None:
            return queryset, False
        condition = self.kwargs.get("targeting_types_condition", self.default_condition)
        true_value = Value(1)
        annotation = {
            "has_" + t: Max(Case(
                When(**{"placements__adwords_campaigns__has_" + t: Value(True), "then": true_value}),
                output_field=BooleanField(),
                default=Value(0)))
            for t in TARGETING_TYPES}
        queryset = queryset.annotate(**annotation)
        fields = ["has_{}".format(t) for t in targeting_types]
        return queryset.filter(build_query_bool(fields, condition, true_value)), True

    def _filter_by_demographic(self):
        return [
            self._filter_by_gender,
            self._filter_by_age,
            self._filter_parent_status
        ]

    def _filter_by_gender(self, queryset):
        genders = self.kwargs.get("genders")
        if genders is None:
            return queryset, False
        condition = self.kwargs.get("demographic_condition", self.default_condition)
        campaign_fields = [GENDER_FIELDS[g] for g in genders]
        fields = ["placements__adwords_campaigns__{}".format(f) for f in campaign_fields]
        return queryset.filter(build_query_bool(fields, condition)), True

    def _filter_by_age(self, queryset):
        ages = self.kwargs.get("ages")
        if ages is None:
            return queryset, False
        condition = self.kwargs.get("demographic_condition", self.default_condition)
        campaign_fields = [AGE_FIELDS[g] for g in ages]
        fields = ["placements__adwords_campaigns__{}".format(f) for f in campaign_fields]
        return queryset.filter(build_query_bool(fields, condition)), True

    def _filter_parent_status(self, queryset):
        parents = self.kwargs.get("parents")
        if parents is None:
            return queryset, False
        condition = self.kwargs.get("demographic_condition", self.default_condition)
        campaign_fields = [PARENT_FIELDS[g] for g in parents]
        fields = ["placements__adwords_campaigns__{}".format(f) for f in campaign_fields]
        return queryset.filter(build_query_bool(fields, condition)), True

    def _filter_by_geo_targeting(self, queryset):
        geo_targets = self.kwargs.get("geo_locations", [])
        if len(geo_targets) == 0:
            return queryset, False
        condition = self.kwargs.get("geo_locations_condition", self.default_condition).lower()
        geo_field = "placements__adwords_campaigns__geo_performance__geo_target"
        if condition == "or":
            queryset = queryset.filter(**{geo_field + "__id__in": geo_targets})
        elif condition == "and":
            queryset = reduce(
                lambda qs, v: qs.filter(**{geo_field + "__id": v}),
                geo_targets,
                queryset)
        return queryset, True

    def _filter_by_topics(self, queryset):
        topics = self.kwargs.get("topics", [])
        if len(topics) == 0:
            return queryset, False
        return self._filter_topics(queryset, topics, "placements__adwords_campaigns__ad_groups__"), True

    def _filter_by_interests(self, queryset):
        interests = self.kwargs.get("interests", [])
        if len(interests) == 0:
            return queryset, False
        return self._filter_interests(queryset, interests, "placements__adwords_campaigns__ad_groups__"), True

    def _filter_by_creative_length(self, queryset):
        lengths = self.kwargs.get("creative_lengths", [])
        if len(lengths) == 0:
            return queryset, False
        return self._filter_creative_lengths(queryset, lengths, "placements__adwords_campaigns__ad_groups__"), True

    def _filter_by_devices(self, queryset):
        devices = self.kwargs.get("devices", [])
        if len(devices) == 0:
            return queryset, False
        devices_condition = self.kwargs.get("devices_condition", self.default_condition)
        prefix = "placements__adwords_campaigns__"
        if devices_condition == "or":
            query_exclude = dict(
                (prefix + field, False) for i, field
                in enumerate(DEVICE_FIELDS)
                if i in devices)
            queryset = queryset.exclude(**query_exclude)
        elif devices_condition == "and":
            query_filter = dict(
                (prefix + field, True) for i, field
                in enumerate(DEVICE_FIELDS)
                if i in devices)
            queryset = queryset.filter(**query_filter)
        return queryset.distinct(), True

    def _filter_creative_lengths(self, queryset, creative_lengths, ad_group_link):
        video_lengths_filter = VIDEO_LENGTHS
        def get_creative_length_annotation(l_id):
            min_val, max_val = video_lengths_filter[l_id]
            criteria = {
                "{}videos_stats__creative__duration__gte".format(
                    ad_group_link): min_val * 1000,
                "{}videos_stats__impressions__gt".format(ad_group_link): 0,
            }
            if max_val is not None:
                criteria["{}videos_stats__creative__duration__lte".format(
                    ad_group_link)] = max_val * 1000
            ann = Count(
                Case(
                    When(
                        then='{}videos_stats__id'.format(ad_group_link),
                        **criteria
                    ),
                    output_field=IntegerField(),
                ),
            )
            return ann
        creative_lengths_condition = self.kwargs.get("creative_lengths_condition")
        if creative_lengths_condition == "or":
            operator = "|"
        else:
            operator = "&"
        creative_length_annotate = get_creative_length_annotation(
            creative_lengths[0])
        for length_id in creative_lengths[1:]:
            creative_length_annotate = CombinedExpression(
                creative_length_annotate, operator,
                get_creative_length_annotation(length_id),
                output_field=IntegerField()
            )
        queryset = queryset.annotate(
            creative_length_annotate=creative_length_annotate)
        queryset = queryset.filter(creative_length_annotate__gt=0)
        return queryset

    @staticmethod
    def _get_filter_true(queryset, filtered_fields, is_or_condition):
        condition = Q(**{filtered_fields[0]: True})
        for f in filtered_fields[1:]:
            if is_or_condition:
                condition |= Q(**{f: True})
            else:
                condition &= Q(**{f: True})
        queryset = queryset.filter(condition)
        return queryset

    def _filter_topics(self, queryset, topics, ad_group_link):
        # get topic ids with all child items
        topic_groups = self._get_topic_child_groups(topics)

        # annotate and filter
        def get_topics_annotation(t_ids):
            ann = Max(
                Case(
                    When(
                        **{
                            "{}topics__topic_id__in".format(
                                ad_group_link): t_ids,
                        },
                        then=Value(1),
                    ),
                    output_field=IntegerField(),
                    default=Value(0)
                ),
            )
            return ann
        topics_condition = self.kwargs.get("topics_condition",
                                           self.default_condition).upper()
        operator = {Operator.OR: Combinable.BITOR,
                    Operator.AND: Combinable.BITAND}.get(topics_condition) or Combinable.BITAND
        top_topics_annotate = get_topics_annotation(topic_groups[0])
        for topic_ids in topic_groups[1:]:
            top_topics_annotate = CombinedExpression(
                top_topics_annotate, operator,
                get_topics_annotation(topic_ids),
                output_field=IntegerField()
            )
        qs = queryset.annotate(top_topics_annotate=top_topics_annotate)
        qs = qs.filter(top_topics_annotate__gt=0)
        item_ids = set(qs.values_list("id", flat=True))
        self.filter_item_ids = self._merge_item_ids(self.filter_item_ids, item_ids)
        return queryset

    def _get_topic_child_groups(self, topics):
        """
        Convert item ids to groups of ids of item itself and all its child items
        """
        key = tuple(sorted(topics))
        if key not in self.topic_child_cache:
            topic_groups = defaultdict(list)
            for topic_id in topics:
                parent_ids = [topic_id]
                while parent_ids:
                    topic_groups[topic_id].extend(parent_ids)
                    parent_ids = list(
                        Topic.objects.filter(
                            parent__in=parent_ids
                        ).values_list('id', flat=True)
                    )
            self.topic_child_cache[key] = list(topic_groups.values())
        return self.topic_child_cache[key]

    def _filter_interests(self, queryset, interests, ad_group_link):
        item_groups = self._get_interest_child_groups(interests)

        # annotate and filter
        def get_interests_annotation(i_ids):
            ann = Max(
                Case(
                    When(
                        **{
                            "{}audiences__audience_id__in".format(
                                ad_group_link): i_ids,
                        },
                        then=Value(1),
                    ),
                    output_field=IntegerField(),
                    default=Value(0)
                ),
            )
            return ann
        interests_condition = self.kwargs.get("interests_condition", self.default_condition).upper()
        operator = {Operator.OR: Combinable.BITOR,
                    Operator.AND: Combinable.BITAND}.get(interests_condition) or Combinable.BITAND
        top_interests_annotate = get_interests_annotation(item_groups[0])
        for item_ids in item_groups[1:]:
            top_interests_annotate = CombinedExpression(
                top_interests_annotate, operator,
                get_interests_annotation(item_ids),
                output_field=IntegerField()
            )
        qs = queryset.annotate(top_interests_annotate=top_interests_annotate)
        qs = qs.filter(top_interests_annotate__gt=0)
        item_ids = set(qs.values_list("id", flat=True))
        self.filter_item_ids = self._merge_item_ids(self.filter_item_ids, item_ids)
        return queryset

    def _get_interest_child_groups(self, interests):
        """
        Convert item ids to groups of ids of item itself and all its child items
        """
        key = tuple(sorted(interests))
        if key not in self.interest_child_cache:
            item_groups = defaultdict(list)
            for item_id in interests:
                parent_ids = [item_id]
                while parent_ids:
                    item_groups[item_id].extend(parent_ids)
                    parent_ids = list(
                        Audience.objects.filter(
                            parent__in=parent_ids
                        ).values_list('id', flat=True)
                    )
            self.interest_child_cache[key] = list(item_groups.values())
        return self.interest_child_cache[key]


def _get_interests_filters():
    interests = AudienceStatistic.objects.filter(audience__parent__isnull=True).values(
        "audience_id", "audience__name", "audience__type").order_by("audience__name", "audience_id").distinct()
    return dict(interests=map_items(interests, INTERESTS_MAP), interests_condition=CONDITIONS)


def opportunity_statistic_date_filter(periods):
    return [dict(
        placements__adwords_campaigns__statistics__date__gte=start,
        placements__adwords_campaigns__statistics__date__lte=end)
               for start, end in periods
           ] or [dict()]


def list_to_filter(items):
    return [dict(id=i, name=n) for i, n in enumerate(items)]


def map_items(items, key_map):
    return [map_item(item, key_map) for item in items]


def map_item(item, key_map):
    return {key_map[key]: value
            for key, value in item.items()
            if key in key_map}
