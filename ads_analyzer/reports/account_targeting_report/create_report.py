from collections import defaultdict
from collections import namedtuple

from django.db.models import Q
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


class AccountTargetingReport:
    TARGETING = {
        CriterionType.AGE: CriterionConfig(AgeRangeStatistic, AgeTargetingSerializer),
        CriterionType.GENDER: CriterionConfig(GenderStatistic, GenderTargetingSerializer),
        CriterionType.KEYWORD: CriterionConfig(KeywordStatistic, KeywordTargetingSerializer),
        CriterionType.PLACEMENT: CriterionConfig(
            [YTChannelStatistic, YTVideoStatistic],
            [PlacementChannelTargetingSerializer, PlacementVideoTargetingSerializer],
        ),
        CriterionType.USER_INTEREST_LIST: CriterionConfig(AudienceStatistic, AudienceTargetingSerializer),
        CriterionType.VERTICAL: CriterionConfig(TopicStatistic, TopicTargetingSerializer),
    }

    def __init__(self, account):
        self.account = account
        self.now = timezone.now()

    def get_stats(self, criterion_types=None, sort_key="campaign_id", statistics_filters=None, kpi_filters=None):
        """
        Retrieve statistics for provided criterion_types values
        :param criterion_types: list [str, str, ...] -> List of aw_reporting.models.Criterion types to retrieve statistics for
            Some configs in self.TARGETING may have multiple model / serializer pairs
        :param sort_key: key to sort aggregated statistics
        :param statistics_filters: dict -> Filters to apply to statistics before aggregation
        :param kpi_params: dict[filters: str, sorts: str] -> Dictionary with kpi filters / sorts to apply in serializers
        :return: list
        """
        if criterion_types is None:
            criterion_types = self.TARGETING.keys()
        elif type(criterion_types) is str:
            criterion_types = [criterion_types]
        targeting_configs = [self.TARGETING[criterion] for criterion in criterion_types]
        filters = self._build_filters(statistics_filters or {})
        all_data = []
        for config in targeting_configs:
            data = self._get_stats(config, filters, kpi_filters=kpi_filters)
            all_data.extend(data)
        final = self._sort_data(all_data, sort_key)
        return final

    def _build_filters(self, statistics_filters=None):
        """
        Get filters for individual statistics for account
        :param date_from:
        :param date_to:
        :return:
        """
        base_filter = Q(**{"ad_group__campaign__account_id": self.account.id})
        if statistics_filters:
            base_filter &= Q(**statistics_filters)
        return base_filter

    def _sort_data(self, data, sort):
        """
        Sort serialized data
        :param queryset:
        :param sort:
        :return:
        """
        reverse = sort[0] == "-"
        sort_value = sort.strip("-")
        sorted_data = sorted(data, key=lambda x: x[sort_value], reverse=reverse)
        return sorted_data

    def _get_stats(self, config, filters, kpi_filters=None):
        """
        Retrieve stats with provided CriterionConfig named tuple config
        Handles config containing multiple model / serializer pairs
        :param config: namedtuple: Criterion
        :param kpi_filters: dict[filters: str, sorts: str] -> Dictionary with kpi filters / sorts to apply in serializers
        :return:
        """
        if type(config.model) is list:
            # Join CriterionConfig.model and CriterionConfig.serializers lists
            config = zip(config.model, config.serializer)
        else:
            config = [config]
        all_data = []
        # Init base kpi filters to update with serialized data
        base_kpi_filters = defaultdict(lambda: dict(min=0, max=0))
        for model, serializer_class in config:
            queryset = model.objects.filter(filters)
            # Pass kpi_params kwarg to filter / sort for aggregated targeting statistics
            data = serializer_class(queryset, many=True, context=dict(now=self.now, params=kpi_filters)).data
            self.update_kpi_filters(base_kpi_filters, KPI_FILTERS, data)
            all_data.extend(data)
        return all_data

    @staticmethod
    def update_kpi_filters(base_filters: defaultdict, kpi_filter_keys, serialized):
        """
        Update base_filters defaultdict with serialized data for all kpi_filter_keys values
        :param base_filters: defaultdict -> Should return dict(min=0, max=0) for absent keys
        :param kpi_filter_keys: list[str, ...] -> List of keys to extract from serialized
        :param serialized: list[dict, ...] -> List of serialized aggregated targeting statistics
        :return:
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

        for item in serialized:
            # base_filters = {"average_cpv": {"min": 0, "max": 0}, ...}
            for kpi in kpi_filter_keys:
                curr_min = safe_compare(min, base_filters[kpi]["min"], item.get(kpi))
                curr_max = safe_compare(max, base_filters[kpi]["max"], item.get(kpi))
                if curr_min is not None:
                    base_filters[kpi]["min"] = curr_min
                if curr_max is not None:
                    base_filters[kpi]["max"] = curr_max
