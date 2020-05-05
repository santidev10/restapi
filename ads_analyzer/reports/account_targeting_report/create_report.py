from collections import defaultdict
from collections import namedtuple
from itertools import chain

from django.db.models import Q
from django.db.models import Avg
from django.db.models import Max
from django.db.models import Min
from django.utils import timezone

from ads_analyzer.reports.account_targeting_report import constants as names
from ads_analyzer.reports.account_targeting_report.serializers import AdGroupSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AgeTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import GenderTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import KeywordTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementChannelTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementVideoTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import TopicTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AudienceTargetingSerializer
from ads_analyzer.reports.account_targeting_report.constants import KPI_FILTER_NAME_MAP
from ads_analyzer.reports.account_targeting_report.constants import ReportType
from ads_analyzer.reports.account_targeting_report.constants import TOTAL_SUMMARY_COLUMN_AGG_MAPPING
from ads_analyzer.reports.account_targeting_report.annotations import SUMMARY_ANNOTATIONS
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

    def __init__(self, account, aggregation_keys, summary_keys, reporting_type=None, all_targeting=False):
        """
        reporting_type determines what data to return from the report
        :param account:
        :param aggregation_keys: Aggregations to retrieve from grouped statistics
        :param reporting_type: iter: -> ReportType
        """
        if reporting_type is None:
            reporting_type = ReportType.ALL
        elif isinstance(reporting_type, str):
            reporting_type = [reporting_type]

        self.default_get_aggregations = (names.IMPRESSIONS, names.VIDEO_VIEWS, names.CLICKS, names.COST)

        # Flag to determine if statistics should be grouped by ad_group or campaign level
        self.all_targeting = all_targeting
        self.aggregation_keys = aggregation_keys
        self.summary_keys = summary_keys
        self.reporting_type = reporting_type
        self.account = account
        # Objects to be mutated by _update methods
        self._all_aggregated_data = []
        # {"average_cpv": {"min": 0.0, "max": 0.5}, ... }
        self._base_kpi_filters = defaultdict(lambda: dict(min=0, max=0))
        # {"impressions": 100, "video_views": 200, ... }
        self._base_overall_summary = defaultdict(int)

    def get_report(self, criterion_types=None, sort_key="campaign_id",
                   statistics_filters=None, aggregation_filters=None):
        """
        Retrieve statistics for provided criterion_types values

        :param criterion_types: list [str, str, ...] -> List of aw_reporting.models.Criterion
            types to retrieve statistics for
        :param sort_key: key to sort aggregated statistics
        :param statistics_filters: dict -> Filters to apply to statistics before aggregation
        :param aggregation_filters: dict[filters: str, sorts: str] -> Dictionary with kpi filters to apply
            in aggregated serializer queryset's
        :return: tuple(list, dict, dict)
        """
        if criterion_types is None:
            criterion_types = self.TARGETING.keys()
        elif type(criterion_types) is str:
            criterion_types = [criterion_types]
        targeting_configs = [self.TARGETING[criterion] for criterion in criterion_types if criterion in self.TARGETING]
        # Filter to retrieve non-aggregated statistics
        statistics_filters = self._build_statistics_filters(statistics_filters or {})

        for config in targeting_configs:
            aggregated_serializer = self.get_aggregated_serializer(
                config, statistics_filters, self.aggregation_keys,
                all_targeting=self.all_targeting, aggregation_filters=aggregation_filters
            )
            aggregations = self._get_aggregations(aggregated_serializer.aggregated_queryset)
            self._process_report(aggregated_serializer, aggregations, config)

        self._finalize_report(sort_key)
        return self._all_aggregated_data, self._base_kpi_filters, self._base_overall_summary

    @staticmethod
    def get_aggregated_serializer(config, filters, aggregation_keys, aggregation_filters=None, all_targeting=False):
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
            now=now, aggregation_keys=aggregation_keys, kpi_filters=aggregation_filters, all_targeting=all_targeting))
        return serializer

    def _process_report(self, aggregated_serializer, aggregations, config):
        """
        Method to hold update method invocations
        :param aggregated_serializer: serializer instantiation
        :param aggregations: dict
        :return: None
        """
        self._update_aggregated_data(aggregated_serializer)
        self._update_kpi_filters(aggregations)
        self._update_overall_summary(aggregations)

    def _finalize_report(self, sort_key):
        """
        Method to invoke all report finalization logic
        :param sort_key: str
        :return: None
        """
        self._base_overall_summary = self._get_finalized_summary(self._base_overall_summary, len(self._all_aggregated_data))
        self._sort_data(sort_key)

    def _get_finalized_summary(self, summary, count):
        finalized = {}
        for key, val in summary.items():
            # Rename aggregation keys: sum_impressions -> impressions
            updated_key = key.split("_", 1)[1]
            if "average" in key:
                try:
                    val = self._base_overall_summary[key] / count
                except ZeroDivisionError:
                    pass
            finalized[updated_key] = val
        return finalized

    def _build_statistics_filters(self, statistics_filters=None):
        """
        Get filters for individual statistics to aggregate
        :param statistics_filters: dict
        :return: Q expression
        """
        base_filter = Q(**{"ad_group__campaign__account_id": self.account.id})
        base_filter &= Q(**{"ad_group__campaign__id": 9722147987})
        if statistics_filters:
            base_filter &= Q(**statistics_filters)
        return base_filter

    def _sort_data(self, sort_key):
        """
        Mutate self._all_aggregated_data by sorting with sort_key
        Reverse (desc) sorts should be prefixed with "-"
        :param sort_key: str
        :return: None
        """
        reverse = sort_key[0] == "-"
        sort_value = sort_key.strip("-")
        self._all_aggregated_data.sort(key=lambda x: x[sort_value], reverse=reverse)

    def _update_aggregated_data(self, aggregated_serializer):
        """
        Mutate self._all_aggregated_data by extending with serialized data
        :param aggregated_serializer: CriterionConfig serializer instantiation
        :return: None
        """
        if ReportType.STATS in self.reporting_type:
            data = aggregated_serializer.data
            self._all_aggregated_data.extend(data)

    def _update_kpi_filters(self, aggregations):
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

        if ReportType.KPI_FILTERS in self.reporting_type or True:
            for kpi in self.aggregation_keys + self.default_get_aggregations:
                self._base_kpi_filters[kpi]["title"] = KPI_FILTER_NAME_MAP[kpi]
                self._base_kpi_filters[kpi]["avg"] = aggregations.get(f"{kpi}__avg")
                curr_min = safe_compare(min, self._base_kpi_filters[kpi]["min"], aggregations.get(f"{kpi}__min"))
                curr_max = safe_compare(max, self._base_kpi_filters[kpi]["max"], aggregations.get(f"{kpi}__max"))
                if curr_min is not None:
                    self._base_kpi_filters[kpi]["min"] = curr_min
                if curr_max is not None:
                    self._base_kpi_filters[kpi]["max"] = curr_max

    def _update_overall_summary(self, aggregations):
        """
        Mutate self._base_overall_summary values with aggregations
        :param aggregations: dict
        :param finalize: int -> If not 0, then should be length of entire data list to calculate averages
            for Avg calculated aggregations
        :return:
        """
        if ReportType.SUMMARY in self.reporting_type:
            # for key, aggregation_result in aggregations.items():
            for key, val in aggregations.items():
                overall_summary_key = TOTAL_SUMMARY_COLUMN_AGG_MAPPING.get(key)
                if overall_summary_key is not None:
                    try:
                        # Sum all aggregation values for all serializers before calculating averages
                        self._base_overall_summary[overall_summary_key] += val or 0
                    except TypeError:
                        pass

    def _get_stats(self, config, filters, kpi_filters=None):
        """
        Retrieve stats with provided CriterionConfig named tuple config
        Handles config containing multiple model / serializer pairs
        :param config: namedtuple: Criterion
        :param kpi_filters: dict[filters: str, sorts: str] -> Dictionary with kpi filters / sorts to apply in serializers
        :return:
        """
        now = timezone.now()
        model, serializer_class = config
        queryset = model.objects.filter(filters)
        serializer = serializer_class(queryset, many=True, context=dict(now=now, params=kpi_filters))
        data = serializer.data
        return serializer, data

    def _get_aggregations(self, queryset):
        """
        Calculate aggregated values for queryset with applied grouping and annotations
        :param queryset:
        :return:
        """
        db_funcs = [Min, Max, Avg]
        targeting_aggs = chain([func(col) for func in db_funcs for col in self.aggregation_keys + self.default_get_aggregations])
        summary_aggs = [SUMMARY_ANNOTATIONS[col] for col in self.summary_keys]
        aggregations = queryset.aggregate(
            *targeting_aggs,
            *summary_aggs,
        )
        return aggregations
