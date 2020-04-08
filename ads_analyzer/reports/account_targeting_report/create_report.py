from collections import defaultdict
from collections import namedtuple

from django.db.models import Q
from django.db.models import Avg
from django.db.models import Max
from django.db.models import Min
from django.db.models import Sum
from django.utils import timezone

from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import CriterionType
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import VideoCreativeStatistic
from aw_reporting.models import AdGroup


from ads_analyzer.reports.account_targeting_report.serializers import AgeTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import GenderTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import KeywordTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementChannelTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementVideoTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import TopicTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AudienceTargetingSerializer
from ads_analyzer.reports.account_targeting_report.constants import KPI_FILTERS

CriterionConfig = namedtuple("CriterionConfig", "model serializer")
CostDelivery = namedtuple("CostDelivery", "cost impressions views")

TOTAL_SUMMARY_COLUMNS = {
            "impressions": "impressions__sum",
            "video_views": "video_views__sum",
            "clicks": "clicks__sum",
            "view_rate": "view_rate__avg",
            "contracted_rate": "contracted_rate__avg",
            "average_cpm": "average_cpm__avg",
            "average_cpv": "average_cpv__avg",
            "cost": "cost__avg",
            "revenue": "revenue__avg",
            "profit": "profit__avg",
            "margin": "margin__avg",
            "ctr_i": "ctr_i__avg",
            "ctr_v": "ctr_v__avg",
        }


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
        CriterionType.AGE: CriterionConfig(AgeRangeStatistic, AgeTargetingSerializer),
        CriterionType.GENDER: CriterionConfig(GenderStatistic, GenderTargetingSerializer),
        CriterionType.KEYWORD: CriterionConfig(KeywordStatistic, KeywordTargetingSerializer),
        f"{CriterionType.PLACEMENT}_CHANNEL": CriterionConfig(YTChannelStatistic, PlacementChannelTargetingSerializer),
        f"{CriterionType.PLACEMENT}_VIDEO": CriterionConfig(YTVideoStatistic, PlacementVideoTargetingSerializer),
        CriterionType.USER_INTEREST_LIST: CriterionConfig(AudienceStatistic, AudienceTargetingSerializer),
        CriterionType.VERTICAL: CriterionConfig(TopicStatistic, TopicTargetingSerializer),
    }
    TOTAL_SUMMARY_COLUMNS = ("impressions", "video_views", "view_rate", "average_cpm", "average_cpv", "cost", "revenue", "margin", "clicks", "ctr_i", "ctr_v")

    def __init__(self, account, reporting_type=None):
        self.account = account
        self.reporting_type = reporting_type if reporting_type is not None else {"stats", "kpi_filters", "summary"}

        # Objects to be mutated by _update methods
        self._all_aggregated_data = []
        # {"average_cpv": {"min": 0.0, "max": 0.5}, ... }
        self._base_kpi_filters = defaultdict(lambda: dict(min=0, max=0))
        # {"impressions": 100, "video_views": 200, ... }
        self._base_overall_summary = defaultdict(int)

    def get_report(self, criterion_types=None, sort_key="campaign_id", statistics_filters=None, aggregation_filters=None):
        """
        Retrieve statistics for provided criterion_types values

        :param criterion_types: list [str, str, ...] -> List of aw_reporting.models.Criterion
            types to retrieve statistics for
        :param sort_key: key to sort aggregated statistics
        :param statistics_filters: dict -> Filters to apply to statistics before aggregation
        :param kpi_filters: dict[filters: str, sorts: str] -> Dictionary with kpi filters to apply
            in aggregated serializer querysets
        :return: tuple(list, dict, dict)
        """
        if criterion_types is None:
            criterion_types = self.TARGETING.keys()
        elif type(criterion_types) is str:
            criterion_types = [criterion_types]
        targeting_configs = [self.TARGETING[criterion] for criterion in criterion_types]
        # Filter to retrieve non-aggregated statistics
        statistics_filters = self._build_statistics_filters(statistics_filters or {})

        for config in targeting_configs:
            aggregated_serializer = self.get_aggregated_serializer(config, statistics_filters, aggregation_filters=aggregation_filters)
            aggregations = self._get_aggregations(aggregated_serializer.aggregated_queryset)
            self._process_report(aggregated_serializer, aggregations)

        self._finalize_report(sort_key)
        return self._all_aggregated_data, self._base_kpi_filters, self._base_overall_summary

    @staticmethod
    def get_aggregated_serializer(config, filters, aggregation_filters=None):
        now = timezone.now()
        model, serializer_class = config
        queryset = model.objects.filter(filters)
        serializer = serializer_class(queryset, many=True, context=dict(now=now, params=aggregation_filters))
        return serializer

    def _process_report(self, aggregated_serializer, aggregations):
        """
        Method to hold update invocations
        :param aggregated_serializer:
        :param aggregations: dict
        :return:
        """
        self._update_aggregated_data(aggregated_serializer)
        self._update_kpi_filters(KPI_FILTERS, aggregations)
        self._update_overall_summary(aggregations)

    def _finalize_report(self, sort_key):
        """
        Method to invoke all report finalization logic
        :param sort_key: str
        :return:
        """
        self._update_overall_summary(finalize=len(self._all_aggregated_data))
        self._sort_data(sort_key)

    def _build_statistics_filters(self, statistics_filters=None):
        """
        Get filters for individual statistics to aggregate
        :param statistics_filters: dict
        :return: Q expression
        """
        base_filter = Q(**{"ad_group__campaign__account_id": self.account.id})
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
        if "stats" in self.reporting_type:
            data = aggregated_serializer.data
            self._all_aggregated_data.extend(data)

    def _update_kpi_filters(self, kpi_filter_keys, aggregations):
        """
        Mutates self._base_kpi_filters defaultdict with serialized data for all kpi_filter_keys values
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

        if "kpi_filters" in self.reporting_type:
            for kpi in kpi_filter_keys:
                curr_min = safe_compare(min, self._base_kpi_filters[kpi]["min"], aggregations.get(f"{kpi}__min"))
                curr_max = safe_compare(max, self._base_kpi_filters[kpi]["max"], aggregations.get(f"{kpi}__max"))
                if curr_min is not None:
                    self._base_kpi_filters[kpi]["min"] = curr_min
                if curr_max is not None:
                    self._base_kpi_filters[kpi]["max"] = curr_max

    def _update_overall_summary(self, aggregations=None, finalize=0):
        """
        Mutate self._base_overall_summary values with aggregations
        Adds to aggregation keys when finalize = 0, else averages Avg calculated aggregations
        :param aggregations: dict
        :param finalize: int -> If not 0, then should be length of entire data list to calculate averages
            for Avg calculated aggregations
        :return:
        """
        if "summary" in self.reporting_type:
            for key, aggregations_key in TOTAL_SUMMARY_COLUMNS.items():
                # Calculate average values for Avg aggregations using finalize = len(all statistics from all serializers)
                if finalize != 0:
                    if "avg" in aggregations_key:
                        self._base_overall_summary[key] /= finalize
                else:
                    # Sum all aggregation values for all serializers before calculating averages
                    self._base_overall_summary[key] += aggregations[aggregations_key]

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
        aggregations = queryset.aggregate(
            Min("average_cpv"), Max("average_cpv"),
            Min("average_cpm"), Max("average_cpm"),
            Min("margin"), Max("margin"),
            Min("cost"), Max("cost"),
            Min("video_views_share"), Max("video_views_share"),
            Min("impressions_share"), Max("impressions_share"),
            Min("view_rate"), Max("view_rate"),

            Sum("impressions"),
            Sum("video_views"),
            Sum("clicks"),

            Avg("view_rate"),
            Avg("contracted_rate"),
            Avg("average_cpm"),
            Avg("average_cpv"),
            Avg("cost"),
            Avg("revenue"),
            Avg("profit"),
            Avg("margin"),
            Avg("ctr_i"),
            Avg("ctr_v"),
        )
        return aggregations

