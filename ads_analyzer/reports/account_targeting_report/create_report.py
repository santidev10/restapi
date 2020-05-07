from collections import defaultdict
from collections import namedtuple
from itertools import chain

from django.db.models import Q
from django.db.models import Avg
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.utils import timezone

from ads_analyzer.reports.account_targeting_report.serializers import AdGroupSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AgeTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import GenderTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import KeywordTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementChannelTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementVideoTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import TopicTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AudienceTargetingSerializer
from ads_analyzer.reports.account_targeting_report.constants import KPI_FILTER_NAME_MAP
from ads_analyzer.reports.account_targeting_report.constants import TOTAL_SUMMARY_COLUMN_AGG_MAPPING
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import CriterionType
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic

CriterionConfig = namedtuple("CriterionConfig", "model serializer")
CostDelivery = namedtuple("CostDelivery", "cost impressions views")


class AccountTargetingReport:
    """
    Description:
    Retrieves and aggregates targeting statistics for provided account. TARGETING config consists of namedtuple's
    containing corresponding statistics model and serializer.

    get_report method drives reporting logic and invokes main update methods which are prefixed with "_update".
    Each update method checks self.reporting_type to determine if update logic for that method should run. This is
    designed to only retrieve necessary data / optimize the report as update methods for large accounts may be
    expensive. If self.reporting_type contains report_type for the update_method, then method will mutate its
    corresponding instance variable (e.g. self._all_data)

    """
    TARGETING = {
        "AdGroup": CriterionConfig(AdGroupStatistic, AdGroupSerializer),
        CriterionType.AGE: CriterionConfig(AgeRangeStatistic, AgeTargetingSerializer),
        CriterionType.GENDER: CriterionConfig(GenderStatistic, GenderTargetingSerializer),
        CriterionType.KEYWORD: CriterionConfig(KeywordStatistic, KeywordTargetingSerializer),
        f"{CriterionType.PLACEMENT}_CHANNEL": CriterionConfig(YTChannelStatistic, PlacementChannelTargetingSerializer),
        f"{CriterionType.PLACEMENT}_VIDEO": CriterionConfig(YTVideoStatistic, PlacementVideoTargetingSerializer),
        CriterionType.USER_INTEREST_LIST: CriterionConfig(AudienceStatistic, AudienceTargetingSerializer),
        CriterionType.VERTICAL: CriterionConfig(TopicStatistic, TopicTargetingSerializer),
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
            criterion_types = self.TARGETING.keys()
        elif type(criterion_types) is str:
            criterion_types = [criterion_types]
        self.targeting_configs = [self.TARGETING[criterion] for criterion in criterion_types
                                  if criterion in self.TARGETING]

        # Container to hold un-calculated aggregated querysets
        self._aggregated_serializers = []
        # Container to hold calculated aggregations of aggregated querysets
        self._aggregations = []

        # Values set by prepare_report method
        self.summary_aggregation_columns = None
        self.aggregation_columns = None
        self.aggregation_summary_funcs = None

        # Objects to be mutated by _update methods
        self._all_aggregated_data = []
        # {"average_cpv": {"min": 0.0, "max": 0.5}, ... }
        self._base_kpi_filters = defaultdict(lambda: dict(min=0, max=0))
        # {"impressions": 100, "video_views": 200, ... }
        self._base_overall_summary = defaultdict(int)

    @staticmethod
    def get_aggregated_serializer(config, filters, aggregation_keys, aggregation_filters=None):
        """
        Instantiate serializer to apply aggregations
        :param config: CriterionConfig
        :param filters: filters to apply to non-aggregated statistics
        :param aggregation_filters: filters to apply to aggregated statistics
        :return: Serializer
        """
        now = timezone.now()
        model, serializer_class = config
        queryset = model.objects.filter(filters)
        serializer = serializer_class(queryset, many=True, context=dict(
            now=now, aggregation_keys=aggregation_keys, kpi_filters=aggregation_filters))
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
        Builds and mutates self._base_kpi_filters defaultdict with serialized data for all kpi_filter_keys values
        :param kpi_filter_keys: list[str, ...] -> List of kpi keys to evaluate from aggregations dict
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
        Mutate self._base_overall_summary values with aggregations
        :return:
        """
        for aggregation in self._all_aggregations:
            for agg_key, col_name in TOTAL_SUMMARY_COLUMN_AGG_MAPPING.items():
                if agg_key not in aggregation:
                    continue
                col_value = aggregation[agg_key]
                try:
                    # Sum all aggregation values for all serializers before calculating averages
                    self._base_overall_summary[col_name] += col_value or 0
                except TypeError:
                    pass

    def _set_aggregations(self):
        """
        Calculate aggregated values for queryset with applied grouping and annotations with parameters provided
            to prepare_report method
        :return:
        """
        all_aggregations = []
        for serializer in self._aggregated_serializers:
            queryset = serializer.aggregated_queryset
            targeting_aggs = chain(
                [func(col) for func in self.aggregation_summary_funcs for col in self.aggregation_columns]
            )
            aggregations = queryset.aggregate(
                *targeting_aggs,
            )
            all_aggregations.append(aggregations)
        self._all_aggregations = all_aggregations
        return all_aggregations

    def get_targeting_report(self, sort_key="campaign_id"):
        """

        :param sort_key: key to sort aggregated statistics
        :return:
        """
        if not self._aggregated_serializers:
            raise ValueError("You must call prepare report first.")
        for aggregated_serializer in self._aggregated_serializers:
            data = aggregated_serializer.data
            self._all_aggregated_data.extend(data)

        if sort_key:
            reverse = sort_key[0] == "-"
            sort_value = sort_key.strip("-")
            self._all_aggregated_data.sort(key=lambda x: x[sort_value], reverse=reverse)
        return self._all_aggregated_data

    def get_kpi_filters(self):
        if not self._aggregated_serializers:
            raise ValueError("You must call prepare report first.")
        if not self._aggregations:
            self._set_aggregations()
        self._set_kpi_filters()
        return self._base_kpi_filters

    def get_overall_summary(self):
        if not self._aggregated_serializers:
            raise ValueError("You must call prepare report first.")
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
        aggregation_summary_funcs = aggregation_summary_funcs or [Avg, Min, Max, Sum]
        self.aggregation_columns = aggregation_columns
        self.aggregation_summary_funcs = aggregation_summary_funcs

        # Filter to retrieve non-aggregated statistics
        statistics_filters = self._build_statistics_filters(statistics_filters or {})
        for config in self.targeting_configs:
            # Get grouped statistics for statistic table
            aggregated_serializer = self.get_aggregated_serializer(
                config, statistics_filters, self.aggregation_columns, aggregation_filters=aggregation_filters
            )
            self._aggregated_serializers.append(aggregated_serializer)
