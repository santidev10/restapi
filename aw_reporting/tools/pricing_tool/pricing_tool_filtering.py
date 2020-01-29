from collections import defaultdict
from functools import reduce

from django.db.models import BooleanField
from django.db.models import Case
from django.db.models import Count
from django.db.models import F
from django.db.models import FloatField
from django.db.models import IntegerField
from django.db.models import Max
from django.db.models import Min
from django.db.models import Q
from django.db.models import QuerySet
from django.db.models import Sum
from django.db.models import When
from django.db.models.expressions import Combinable
from django.db.models.expressions import CombinedExpression
from django.db.models.expressions import Value
from django.contrib.postgres.aggregates import ArrayAgg

from aw_reporting.models import AdGroup
from aw_reporting.models import AgeRanges
from aw_reporting.models import Audience
from aw_reporting.models import Devices
from aw_reporting.models import Campaign
from aw_reporting.models import Genders
from aw_reporting.models import Opportunity
from aw_reporting.models import ParentStatuses
from aw_reporting.models import SalesForceGoalType
from aw_reporting.models import Topic
from aw_reporting.tools.pricing_tool.constants import AGE_FIELDS
from aw_reporting.tools.pricing_tool.constants import DEVICE_FIELDS
from aw_reporting.tools.pricing_tool.constants import GENDER_FIELDS
from aw_reporting.tools.pricing_tool.constants import PARENT_FIELDS
from aw_reporting.tools.pricing_tool.constants import TARGETING_TYPES
from aw_reporting.tools.pricing_tool.constants import VIDEO_LENGTHS
from aw_reporting.tools.pricing_tool.constants import ModelFiltersOptions
from utils.datetime import now_in_default_tz
from utils.datetime import quarter_days
from utils.query import Operator
from utils.query import build_query_bool
from utils.query import merge_when
from utils.query import split_request

CONDITIONS = [
    dict(id="or", name="Or"),
    dict(id="and", name="And"),
]


class PricingToolFiltering:
    default_condition = "or"

    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.interest_child_cache = dict()
        self.topic_child_cache = dict()
        self.filter_item_ids = None

    @classmethod
    def get_filters(cls, user, opportunities_ids=None):

        start, end = cls._get_default_dates()

        if opportunities_ids is None:
            opportunities_ids = Opportunity.objects.have_campaigns(user=user).values_list("id", flat=True)

        opportunities = Opportunity.objects.filter(id__in=opportunities_ids)

        brands = opportunities \
            .filter(brand__isnull=False, ) \
            .values_list("brand", flat=True) \
            .order_by("brand").distinct()

        categories = opportunities \
            .filter(category__isnull=False, ) \
            .values_list("category_id", flat=True) \
            .order_by("category_id").distinct()

        product_types = AdGroup.objects.get_queryset_for_user(user=user) \
            .values_list("type", flat=True) \
            .order_by("type") \
            .distinct()
        product_types = [dict(id=t, name=t) for t in sorted(product_types)
                         if t not in ["", " --", "Standard"]]

        filters = dict(
            # timing
            quarters=[dict(id=c, name=c)
                      for c in list(sorted(quarter_days.keys()))],
            quarters_condition=CONDITIONS,
            start=str(start), end=str(end),
            compare_yoy=False,

            product_types=product_types,
            product_types_condition=CONDITIONS,

            targeting_types=[
                dict(id=i, name=i.capitalize()) for i in TARGETING_TYPES
            ],
            targeting_types_condition=CONDITIONS,

            brands=[dict(id=b, name=b) for b in brands],
            categories=[dict(id=c, name=c) for c in categories],

            geo_locations="Use /geo_target_list?search= endpoint to get locations and sent their ids back",
            geo_locations_condition=CONDITIONS,

            ages=list_to_filter(AgeRanges),
            genders=list_to_filter(Genders),
            parents=list_to_filter(ParentStatuses),
            demographic_condition=CONDITIONS,
            topics=list(Topic.objects.filter(parent=None).exclude(topicstatistic=None)
                        .order_by("name", "id").values("id", "name")),
            topics_condition=CONDITIONS,
            devices=list_to_filter(Devices),
            devices_condition=CONDITIONS,

            creative_lengths=[
                dict(
                    id=uid,
                    name="{}{} sec{}".format(f,
                                             "" if t is None else "-{}".format(
                                                 t), "+" if t is None else ""),
                )
                for uid, (f, t) in enumerate(VIDEO_LENGTHS)
            ],
            creative_lengths_condition=CONDITIONS,

            exclude_campaigns="list of campaign ids is expected",
            exclude_opportunities="list of opportunity ids is expected",
            ctr=dict(min=0, max=None),
            ctr_v=dict(min=0, max=None),
            video_view_rate=dict(min=0, max=100),
            video100rate=dict(min=0, max=100),
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

    def apply(self, user):
        return self._filter(user)

    def _filter(self, user):

        def apply_filters(queryset, filters, model_options):
            for filter in filters:
                queryset, _ = filter(queryset, model_options)
            return queryset

        campaigns_filters = (
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

        filters = (
            self._filter_by_brand,
            self._filter_by_categories,
            self._filter_by_apex,
            *self._filter_by_kpi(),
        )

        campaigns_queryset = Campaign.objects.get_queryset_for_user(user=user)\
            .values("salesforce_placement__opportunity")\
            .annotate(ids=ArrayAgg("id"))\
            .values("salesforce_placement__opportunity", "ids")

        campaigns_queryset = apply_filters(campaigns_queryset, campaigns_filters,
                                           model_options=ModelFiltersOptions.Campaign).all()

        if self.filter_item_ids is not None:
            campaigns_queryset = campaigns_queryset.filter(id__in=self.filter_item_ids)

        campaigns_ids_map = {campaigns["salesforce_placement__opportunity"]: campaigns["ids"]
                             for campaigns in campaigns_queryset}

        opportunity_queryset = Opportunity.objects.get_queryset_for_user(user=user)\
            .annotate(aw_budget=Sum("placements__adwords_campaigns__cost")) \
            .order_by("-aw_budget")

        opportunity_queryset = apply_filters(opportunity_queryset, filters, model_options=ModelFiltersOptions.Opportunity)

        opportunity_queryset = opportunity_queryset.filter(id__in=campaigns_ids_map.keys()).values("id")

        return opportunity_queryset, campaigns_ids_map

    def _filter_by_periods(self, queryset, model_options):
        periods = self.kwargs["periods"]
        if len(periods) == 0:
            return queryset, False
        queryset = queryset.annotate(
            min_start=Min(f"{model_options.prefix}start_date"),
            max_end=Max(f"{model_options.prefix}end_date")
        )
        filters = [Q(min_start__lte=end, max_end__gte=start)
                   for start, end in periods]
        query = reduce(lambda res, f: res | f,
                       filters,
                       Q(min_start__isnull=True) | Q(max_end__isnull=True))

        return queryset.filter(query), True


    def _filter_by_product_types(self, queryset, model_options):
        product_types = self.kwargs.get("product_types", [])
        if len(product_types) == 0:
            return queryset, False
        product_types_condition = self.kwargs.get("product_types_condition",
                                                  self.default_condition)

        if product_types_condition == "or" and model_options.filtering_or:
            queryset = queryset.filter(**{f"{model_options.prefix}ad_groups__type__in":product_types})

        elif product_types_condition == "and" and model_options.filtering_and:
            queryset = reduce(
                lambda qs, pt: qs.filter(**{f"{model_options.prefix}ad_groups__type": pt}),
                product_types,
                queryset)

        return queryset, True

    def _filter_by_targeting_types(self, queryset: QuerySet, model_options):
        targeting_types = self.kwargs.get("targeting_types")
        if targeting_types is None:
            return queryset, False
        condition = self.kwargs.get("targeting_types_condition",
                                    self.default_condition)
        true_value = Value(1)
        annotation = {"has_" + t: Max(Case(When(
            **{
                f"{model_options.prefix}has_" + t: Value(True),
                "then": true_value
            }),
            output_field=BooleanField(),
            default=Value(0)))
            for t in TARGETING_TYPES}
        queryset = queryset.annotate(**annotation)

        fields = ["has_{}".format(t) for t in targeting_types]

        return queryset.filter(
            build_query_bool(fields, condition, true_value)), True

    def _filter_by_demographic(self):
        return [
            self._filter_by_gender,
            self._filter_by_age,
            self._filter_parent_status
        ]

    def _filter_by_gender(self, queryset, model_options):
        genders = self.kwargs.get("genders")
        if genders is None:
            return queryset, False
        condition = self.kwargs.get("demographic_condition",
                                    self.default_condition)
        campaign_fields = [GENDER_FIELDS[g] for g in genders]
        fields = [f"{model_options.prefix}{f}" for f in campaign_fields]
        return queryset.filter(build_query_bool(fields, condition)), True

    def _filter_by_age(self, queryset, model_options):
        ages = self.kwargs.get("ages")
        if ages is None:
            return queryset, False
        condition = self.kwargs.get("demographic_condition",
                                    self.default_condition)
        campaign_fields = [AGE_FIELDS[g] for g in ages]
        fields = [f"{model_options.prefix}{f}" for f in campaign_fields]
        return queryset.filter(build_query_bool(fields, condition)), True

    def _filter_parent_status(self, queryset, model_options):
        parents = self.kwargs.get("parents")
        if parents is None:
            return queryset, False
        condition = self.kwargs.get("demographic_condition",
                                    self.default_condition)
        campaign_fields = [PARENT_FIELDS[g] for g in parents]
        fields = [f"{model_options.prefix}{f}" for f in campaign_fields]
        return queryset.filter(build_query_bool(fields, condition)), True

    def _filter_by_geo_targeting(self, queryset, model_options):
        geo_targets = self.kwargs.get("geo_locations", [])
        if len(geo_targets) == 0:
            return queryset, False
        condition = self.kwargs.get("geo_locations_condition",
                                    self.default_condition).lower()
        geo_field = f"{model_options.prefix}geo_performance__geo_target"
        if condition == "or" and model_options.filtering_or:
            queryset = queryset.filter(**{geo_field + "__id__in": geo_targets})
        elif condition == "and" and model_options.filtering_and:
            queryset = reduce(
                lambda qs, v: qs.filter(**{geo_field + "__id": v}),
                geo_targets,
                queryset)
        return queryset, True

    def _filter_by_brand(self, queryset, *args):
        brands = self.kwargs.get("brands", [])
        if len(brands) == 0:
            return queryset, False
        return queryset.filter(brand__in=brands), True

    def _filter_by_categories(self, queryset, *args):
        categories = self.kwargs.get("categories", [])
        if len(categories) == 0:
            return queryset, False
        return queryset.filter(category_id__in=categories), True

    def _filter_by_topics(self, queryset, model_options):
        topics = self.kwargs.get("topics", [])
        if len(topics) == 0:
            return queryset, False
        return self._filter_topics(queryset, topics, f"{model_options.prefix}ad_groups__"), True

    def _filter_by_interests(self, queryset, model_options):
        interests = self.kwargs.get("interests", [])
        if len(interests) == 0:
            return queryset, False
        return self._filter_interests(queryset, interests, f"{model_options.prefix}ad_groups__"), True

    def _filter_by_creative_length(self, queryset, model_options):
        lengths = self.kwargs.get("creative_lengths", [])
        if len(lengths) == 0:
            return queryset, False
        return self._filter_creative_lengths(queryset, lengths, f"{model_options.prefix}ad_groups__"), True

    def _filter_by_devices(self, queryset, model_options):
        devices = self.kwargs.get("devices", [])
        if len(devices) == 0:
            return queryset, False
        devices_condition = self.kwargs.get("devices_condition", self.default_condition)
        if devices_condition == "or" and model_options.filtering_or:
            query_exclude = dict((model_options.prefix + field, False) for i, field
                                 in enumerate(DEVICE_FIELDS)
                                 if i in devices)
            queryset = queryset.exclude(**query_exclude)

        elif devices_condition == "and" and model_options.filtering_and:
            query_filter = dict((model_options.prefix + field, True) for i, field
                                in enumerate(DEVICE_FIELDS)
                                if i in devices)
            queryset = queryset.filter(**query_filter)
        return queryset.distinct(), True

    def _filter_by_apex(self, queryset, *args):
        apex_deal = self.kwargs.get("apex_deal")
        if apex_deal is not None and apex_deal.isdigit():
            queryset = queryset.filter(apex_deal=bool(int(apex_deal)))
        else:
            return queryset, False
        return queryset, True

    def _filter_by_kpi(self):
        return [
            self._filter_by_ctr,
            self._filter_by_ctr_v,
            self._filter_by_view_rate,
            self._filter_by_video100rate,
        ]

    def _filter_by_ctr(self, queryset, *args):
        max_ctr_filter = self.kwargs.get("max_ctr", None)
        min_ctr_filter = self.kwargs.get("min_ctr", None)

        if max_ctr_filter is None and min_ctr_filter is None:
            return queryset, False

        date_filter = opportunity_statistic_date_filter(
            self.kwargs.get("periods", []))

        queryset = queryset.annotate(
            aw_clicks=Sum(Case(
                *merge_when(
                    date_filter,
                    then=f"placements__adwords_campaigns__statistics__clicks"),
                output_field=IntegerField(),
                default=0
            )),
            aw_impressions=Sum(Case(
                *merge_when(
                    date_filter,
                    then=f"placements__adwords_campaigns__statistics__impressions"),
                output_field=IntegerField(),
                default=0
            ))
        ).annotate(ctr=Case(
            When(aw_impressions=0, then=0),
            When(aw_impressions__gt=0,
                 then=F("aw_clicks") * 1. / F("aw_impressions") * 100),
            output_field=FloatField()
        ))

        if max_ctr_filter is not None:
            queryset = queryset.filter(ctr__lte=max_ctr_filter)

        if min_ctr_filter is not None:
            queryset = queryset.filter(ctr__gte=min_ctr_filter)

        return queryset, True

    def _filter_by_ctr_v(self, queryset, *args):
        max_ctr_v_filter = self.kwargs.get("max_ctr_v", None)
        min_ctr_v_filter = self.kwargs.get("min_ctr_v", None)

        if max_ctr_v_filter is None and min_ctr_v_filter is None:
            return queryset, False

        date_filter = opportunity_statistic_date_filter(
            self.kwargs.get("periods", []))

        queryset = queryset.annotate(
            aw_clicks=Sum(Case(
                *merge_when(
                    date_filter,
                    then="placements__adwords_campaigns__statistics__clicks"),
                output_field=IntegerField(),
                default=0
            )),
            aw_video_views=Sum(Case(
                *merge_when(
                    date_filter,
                    then="placements__adwords_campaigns__statistics__video_views"),
                output_field=IntegerField(),
                default=0
            ))
        ).annotate(ctr_v=Case(
            When(aw_video_views=0, then=0),
            When(aw_video_views__gt=0,
                 then=F("aw_clicks") * 1. / F("aw_video_views") * 100),
            output_field=FloatField()
        ))

        if max_ctr_v_filter is not None:
            queryset = queryset.filter(ctr_v__lte=max_ctr_v_filter)

        if min_ctr_v_filter is not None:
            queryset = queryset.filter(ctr_v__gte=min_ctr_v_filter)
        return queryset, True

    def _filter_by_view_rate(self, queryset, *args):
        max_rate = self.kwargs.get("max_video_view_rate", None)
        min_rate = self.kwargs.get("min_video_view_rate", None)

        if max_rate is None and min_rate is None:
            return queryset, False
        date_filter = opportunity_statistic_date_filter(
            self.kwargs.get("periods", []))

        queryset = queryset.annotate(
            aw_cpv_impressions=Sum(
                Case(
                    *merge_when(
                        date_filter,
                        placements__adwords_campaigns__salesforce_placement__goal_type_id__in=[
                            SalesForceGoalType.CPV,
                            SalesForceGoalType.CPM_AND_CPV],
                        then="placements__adwords_campaigns__statistics__impressions"),
                    output_field=IntegerField(),
                    default=0
                )
            ),
            aw_video_views=Sum(Case(
                *merge_when(date_filter,
                            placements__goal_type_id=SalesForceGoalType.CPV,
                            then="placements__adwords_campaigns__statistics__video_views"),
                output_field=IntegerField(),
                default=0
            )),
        ).annotate(view_rate=Case(
            When(aw_cpv_impressions=0, then=0),
            When(aw_cpv_impressions__gt=0,
                 then=F("aw_video_views") * 1. / F("aw_cpv_impressions") * 100),
            output_field=FloatField()
        ))

        if max_rate is not None:
            queryset = queryset.filter(view_rate__lte=max_rate)

        if min_rate is not None:
            queryset = queryset.filter(view_rate__gte=min_rate)
        return queryset, True

    def _filter_by_video100rate(self, queryset, *args):
        max_rate = self.kwargs.get("max_video100rate", None)
        min_rate = self.kwargs.get("min_video100rate", None)

        if max_rate is None and min_rate is None:
            return queryset, False

        date_filter = opportunity_statistic_date_filter(
            self.kwargs.get("periods", []))

        statistic_ref = "placements__adwords_campaigns__statistics__"
        queryset = queryset.annotate(
            aw_impressions=Sum(Case(
                *merge_when(date_filter,
                            then=statistic_ref + "impressions"))),
            video_views_100_sum=Sum(Case(
                *merge_when(date_filter,
                            then=statistic_ref + "video_views_100_quartile"))),
        ).annotate(
            video100rate_percent=Case(
                When(Q(aw_impressions=0)
                     | Q(aw_impressions__isnull=True)
                     | Q(video_views_100_sum__isnull=True),
                     then=0),
                When(aw_impressions__gt=0,
                     then=F("video_views_100_sum") * 1. / F(
                         "aw_impressions") * 100),
                output_field=FloatField()
            ))

        if max_rate is not None:
            queryset = queryset.filter(video100rate_percent__lte=max_rate)

        if min_rate is not None:
            queryset = queryset.filter(video100rate_percent__gte=min_rate)
        return queryset, True

    def _filter_creative_lengths(self, queryset, creative_lengths,
                                 ad_group_link):
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

        creative_lengths_condition = self.kwargs.get("creative_lengths_condition") or "or"
        operator = "|" if creative_lengths_condition == "or" else "&"
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
        operator = {
                       Operator.OR: Combinable.BITOR,
                       Operator.AND: Combinable.BITAND
                   }.get(topics_condition) or Combinable.BITAND

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
        self.filter_item_ids = self._merge_item_ids(self.filter_item_ids,
                                                    item_ids)

        return queryset

    def _get_topic_child_groups(self, topics):
        """
        Convert item ids to groups of ids of item itself and all its child items
        :param topics:
        :return:
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

        interests_condition = self.kwargs.get("interests_condition",
                                              self.default_condition).upper()
        operator = {
                       Operator.OR: Combinable.BITOR,
                       Operator.AND: Combinable.BITAND
                   }.get(interests_condition) or Combinable.BITAND

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

        self.filter_item_ids = self._merge_item_ids(self.filter_item_ids,
                                                    item_ids)

        return queryset

    def _get_interest_child_groups(self, interests):
        """
        Convert item ids to groups of ids of item itself and all its child items
        :param interests:
        :return:
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
    interests = Audience.objects.filter(parent=None) \
        .exclude(audiencestatistic=None) \
        .order_by("name", "id") \
        .values("id", "name", "type")

    return dict(interests=list(interests),
                interests_condition=CONDITIONS)


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


