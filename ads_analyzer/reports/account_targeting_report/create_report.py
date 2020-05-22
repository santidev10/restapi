"""
Module used to gather aggregated AdGroup targeting statistics for Account
Each statistics table has a serializer used to aggregate all statistics for an AdGroup / targeting criteria pair

Before any operation can be used, the prepare_report must be invoked to provide the report with the required parameters
    to filter and aggregate statistics

Serializers first group individual statistics segmented by date with the 'statistics_filters' parameter
Serializers then aggregate filtered statistics with the 'aggregation_columns' parameter, which are constant values
    that map to aggregation expressions defined in ads_analyzer.reports.annotations

Aggregations can then be filtered using the 'aggregation_filters' parameter
Once prepare_report is invoked, the methods get_targeting_report, get_kpi_filters, and get_overall_summary methods
    may be called

kpi_filters are characterized as important AdGroup targeting metrics such as sum_cost, sum_revenue, max_cpv, etc. for
    the account, which are calculated with 'aggregation_columns' and 'aggregation_summary_funcs'
overall_summary are values from the AdGroupStatistic table segmented by date
"""

from collections import defaultdict
from collections import namedtuple
from itertools import chain

import hashlib
import json
from django.core.serializers.json import DjangoJSONEncoder
from utils.api.cache import cache_method

from django.db.models import Q
from django.db.models import Avg
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum

from ads_analyzer.reports.account_targeting_report.serializers import AdGroupSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AgeTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AudienceTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import DeviceTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import GenderTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import KeywordTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementChannelTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementVideoTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import RemarketTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import TopicTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import ParentTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import VideoCreativeTargetingSerializer
from ads_analyzer.reports.account_targeting_report.constants import KPI_FILTER_NAME_MAP
from ads_analyzer.reports.account_targeting_report.constants import TOTAL_SUMMARY_COLUMN_AGG_MAPPING
from aw_reporting.models import CriteriaTypeEnum


CriterionConfig = namedtuple("CriterionConfig", "model serializer")
CostDelivery = namedtuple("CostDelivery", "cost impressions views")


class AccountTargetingReport:
    """
    Description:
    Retrieves and aggregates targeting statistics for provided account. TARGETING config consists of namedtuple's
    containing corresponding statistics model and serializer.

    Usage:
        1. Init class with account to retrieve report for and criterion_types values to retrieve statistics
        2. Invoke prepare_report with parameters
        3. Request report info
            a. get_targeting_report
            b. get_kpi_filters
            c. get_overall_summary
    """
    CACHE_KEY_PREFIX = "ads_analyzer.reports.account_targeting_report.create_report"
    TARGETING = {
        "AdGroup": AdGroupSerializer,
        CriteriaTypeEnum.DEVICE.name: DeviceTargetingSerializer,
        CriteriaTypeEnum.VIDEO_CREATIVE.name: VideoCreativeTargetingSerializer,
        CriteriaTypeEnum.KEYWORD.name: KeywordTargetingSerializer,
        f"{CriteriaTypeEnum.PLACEMENT.name}_CHANNEL": PlacementChannelTargetingSerializer,
        f"{CriteriaTypeEnum.PLACEMENT.name}_VIDEO": PlacementVideoTargetingSerializer,
        CriteriaTypeEnum.VERTICAL.name: TopicTargetingSerializer,
        CriteriaTypeEnum.USER_LIST.name: RemarketTargetingSerializer,
        CriteriaTypeEnum.USER_INTEREST.name: AudienceTargetingSerializer,
        CriteriaTypeEnum.AGE_RANGE.name: AgeTargetingSerializer,
        CriteriaTypeEnum.GENDER.name: GenderTargetingSerializer,
        CriteriaTypeEnum.PARENT.name: ParentTargetingSerializer,
    }

    def __init__(self, account,  criterion_types=None):
        """
        reporting_type determines what data to return from the report
        :param account:
        :param criterion_types: list [str, str, ...] -> List of aw_reporting.models.Criterion
            types to retrieve statistics for
        """
        self.account = account
        if criterion_types is None:
            self.criterion_types = self.TARGETING.keys()
        elif type(criterion_types) is str:
            self.criterion_types = [criterion_types]
        else:
            self.criterion_types = criterion_types
        self.targeting_configs = [self.TARGETING[criterion] for criterion in self.criterion_types
                                  if criterion in self.TARGETING]

        # Container to hold un-calculated aggregated querysets
        self._aggregated_serializers = []
        # Container to hold calculated aggregations of aggregated querysets
        self._aggregations = []

        # Values set by prepare_report method
        self.summary_aggregation_columns = None
        self.aggregation_columns = None
        self.aggregation_summary_funcs = None
        self.statistics_filters = None
        self.aggregation_filters = None

        # Objects to be mutated by _update methods
        self._all_aggregated_data = []
        # {"average_cpv": {"min": 0.0, "max": 0.5}, ... }
        self._base_kpi_filters = defaultdict(lambda: dict(min=0, max=0))
        # {"impressions": 100, "video_views": 200, ... }
        self._base_overall_summary = defaultdict(int)

    @staticmethod
    def get_aggregated_serializer(serializer_class, filters, aggregation_keys, aggregation_filters=None):
        """
        Instantiate serializer to apply aggregations
        :param serializer_class:
        :param filters: filters to apply to non-aggregated statistics
        :param aggregation_filters: filters to apply to aggregated statistics
        :return: Serializer
        """
        statistic_model = serializer_class.Meta.model
        queryset = statistic_model.objects.filter(filters)
        serializer = serializer_class(queryset, many=True, context=dict(
            report_name=serializer_class.report_name,
            aggregation_keys=aggregation_keys, kpi_filters=aggregation_filters))
        return serializer

    def _build_statistics_filters(self, statistics_filters=None):
        """
        Get filters for individual statistics before grouped aggregations
        :param statistics_filters: dict
        :return: Q expression
        """
        base_filter = Q(**{"ad_group__campaign__account_id": self.account.id})
        if statistics_filters:
            base_filter &= Q(**statistics_filters)
        return base_filter

    def _set_kpi_filters(self):
        """
        Mutates self._base_kpi_filters defaultdict with aggregated serialized data using self._all_aggregations
        Supports iterating over many serialized aggregations if report was configured with multiple targeting
            CriteriaTypeEnum values
        :return: None
        """
        def safe_compare(func, val1, val2):
            """
            Safe compare for val1, val2 None values
            val1 param should always be valid min or max value from base filters
            """
            result = val1
            try:
                result = func(val1, val2)
            except TypeError:
                pass
            return result
        for aggregation in self._all_aggregations:
            for kpi_name in self.aggregation_columns:
                # Get formatted kpi name as some aggs might be prepended with "sum"
                # sum_impressions must be named this way to not conflict with individual statistics "impressions" column
                self._base_kpi_filters[kpi_name]["title"] = KPI_FILTER_NAME_MAP[kpi_name]
                self._base_kpi_filters[kpi_name]["avg"] = aggregation.get(f"{kpi_name}__avg")
                curr_min = safe_compare(min, self._base_kpi_filters[kpi_name]["min"], aggregation.get(f"{kpi_name}__min"))
                curr_max = safe_compare(max, self._base_kpi_filters[kpi_name]["max"], aggregation.get(f"{kpi_name}__max"))
                if curr_min is not None:
                    self._base_kpi_filters[kpi_name]["min"] = curr_min
                if curr_max is not None:
                    self._base_kpi_filters[kpi_name]["max"] = curr_max
        return self._base_kpi_filters

    def _set_overall_summary(self):
        """
        Calculate overall summary using AdGroup config with prepare_report method parameters
        Uses TOTAL_SUMMARY_COLUMN_AGG_MAPPING to map Django generated aggregation names
            e.g. ...aggregate(Avg(impressions)) = impressions__avg=x
        :return:
        """
        serializer_class = self.TARGETING["AdGroup"]
        aggregated_serializer = self.get_aggregated_serializer(
            serializer_class, self.statistics_filters, self.aggregation_columns, aggregation_filters=self.aggregation_filters
        )
        aggregations = self._get_aggregation(aggregated_serializer.aggregated_queryset)
        for agg_key, col_name in TOTAL_SUMMARY_COLUMN_AGG_MAPPING.items():
            if agg_key not in aggregations:
                continue
            col_value = aggregations[agg_key]
            try:
                # Sum all aggregation values for all serializers before calculating averages
                self._base_overall_summary[col_name] += col_value or 0
            except TypeError:
                pass

    def _set_aggregations(self):
        """
        Calculate aggregated values for each serializer
            queryset with applied grouping and annotations with prepare_report method parameters
            to prepare_report method
        :return:
        """
        all_aggregations = []
        for serializer in self._aggregated_serializers:
            queryset = serializer.aggregated_queryset
            aggregations = self._get_aggregation(queryset)
            all_aggregations.append(aggregations)
        self._all_aggregations = all_aggregations
        return all_aggregations

    def _get_aggregation(self, queryset):
        """
        Calculate aggregations for queryset
        targeting_agg_funcs prepares Django aggregation functions to apply to queryset
        :param queryset:
        :return:
        """
        # Prepare aggregation functions for each self.aggregation_summary_funcs db function and for each
        # aggregation column
        targeting_agg_funcs = chain(
            [func(col) for func in self.aggregation_summary_funcs for col in self.aggregation_columns]
        )
        aggregations = queryset.aggregate(*targeting_agg_funcs)
        return aggregations

    def get_targeting_report(self, sort_key="campaign_id"):
        """
        Serialize all aggregated queryset's and sort
        :param sort_key: key to sort aggregated statistics
        :return:
        """
        if not self._aggregated_serializers:
            raise ValueError("You must first call prepare_report with valid parameters.")
        for aggregated_serializer in self._aggregated_serializers:
            # Other aggregated serializers may be used by other parts of the report that should not be serialized
            # into targeting data
            if aggregated_serializer.context["report_name"] not in self.criterion_types:
                continue
            data = self._serialize(aggregated_serializer)
            self._all_aggregated_data.extend(data)
        if sort_key:
            reverse = sort_key[0] == "-"
            sort_value = sort_key.strip("-")
            # Ensure that all None values sort to the end of the list depending on reverse
            if reverse is True:
                key_expression = lambda x: (x[sort_value] is not None, x[sort_value])
            else:
                key_expression = lambda x: (x[sort_value] is None, x[sort_value])
            self._all_aggregated_data.sort(key=key_expression, reverse=reverse)
        return self._all_aggregated_data

    @cache_method(timeout=3600)
    def _serialize(self, aggregated_serializer):
        """
        Cached serialization method
        :param aggregated_serializer:
        :return:
        """
        data = aggregated_serializer.data
        return data

    def get_kpi_filters(self):
        if not self._aggregated_serializers:
            raise ValueError("You must first call prepare_report with valid parameters.")
        if not self._aggregations:
            self._set_aggregations()
        self._set_kpi_filters()
        return self._base_kpi_filters

    def get_overall_summary(self):
        if not self._aggregated_serializers:
            raise ValueError("You must first call prepare_report with valid parameters.")
        if not self._aggregations:
            self._set_aggregations()
        self._set_overall_summary()
        return self._base_overall_summary

    def prepare_report(self, statistics_filters=None, aggregation_filters=None, aggregation_columns=None,
                       aggregation_summary_funcs=None):
        """
        Retrieve statistics for provided criterion_types values
            statistics_filters are filters used for specific statistics, such as retrieving
                all KeywordStatistic rows with impressions > 1
            aggregation_filters are filters used for grouped statistics, such as retrieving
                TopicStatistic's grouped by AdGroup with sum video_views > 1
            aggregation_columns are aggregations calculated using aggregation_column_funcs

        :param statistics_filters: dict -> Filters to apply to individual statistics before grouping
        :param aggregation_filters: dict[filters: str, sorts: str] -> Dictionary with kpi filters to apply
            in aggregated serializer queryset's
        :param aggregation_summary_funcs: Django db funcs
        :return: tuple(list, dict, dict)
        """
        self.aggregation_columns = aggregation_columns or []
        self.aggregation_filters = aggregation_filters or []
        self.aggregation_summary_funcs = aggregation_summary_funcs or [Avg, Min, Max, Sum]

        # Filter to retrieve non-aggregated statistics
        self.statistics_filters = self._build_statistics_filters(statistics_filters or {})

        # AdGroup serializer is not usually used in targeting data, but should be part of kpi_filters in the case of
        # results of the overall summary is out of bounds of the targeting data
        if "targeting_status" not in self.aggregation_filters:
            self.targeting_configs += [self.TARGETING["AdGroup"]]
        for serializer_class in self.targeting_configs:
            # Get grouped statistics for statistic table
            aggregated_serializer = self.get_aggregated_serializer(
                serializer_class, self.statistics_filters, self.aggregation_columns, aggregation_filters=self.aggregation_filters
            )
            self._aggregated_serializers.append(aggregated_serializer)

    def get_cache_key(self, part, options):
        query = str(options[0][0].aggregated_queryset.query)
        key_json = json.dumps(query, sort_keys=True, cls=DjangoJSONEncoder)
        key_hash = hashlib.md5(key_json.encode()).hexdigest()
        key = f"{self.CACHE_KEY_PREFIX}.{part}.{key_hash}"
        return key, key_json
