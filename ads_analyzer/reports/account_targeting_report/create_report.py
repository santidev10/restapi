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
from aw_reporting.models.salesforce_constants import SalesForceGoalType

Criterion = namedtuple("Criterion", "model serializer")
CostDelivery = namedtuple("CostDelivery", "cost impressions views")


class AccountTargetingReport:
    TARGETING = {
        CriterionType.AGE: Criterion(AgeRangeStatistic, AgeTargetingSerializer),
        CriterionType.GENDER: Criterion(GenderStatistic, GenderTargetingSerializer),
        CriterionType.KEYWORD: Criterion(KeywordStatistic, KeywordTargetingSerializer),
        CriterionType.PLACEMENT: Criterion(
            [YTChannelStatistic, YTVideoStatistic],
            [PlacementChannelTargetingSerializer, PlacementVideoTargetingSerializer],
        ),
        CriterionType.USER_INTEREST_LIST: Criterion(AudienceStatistic, AudienceTargetingSerializer),
        CriterionType.VERTICAL: Criterion(TopicStatistic, TopicTargetingSerializer),
    }

    def __init__(self, account, sort_key="campaign_id"):
        self.account = account
        self.now = timezone.now()
        self.sort_key = sort_key

    def get_stats(self, criterion_types=None, kpi_params=None, sorts=None):
        """
        Retrieve statistics for provided criterion_types values
        :param criterion_types: list [str, str, ...] -> List of Adgroup criterion types to retrieve
            stats for
            Some configs may have multiple model / serializer pairs
        :return: list
        """
        if criterion_types is None:
            criterion_types = self.TARGETING.keys()
        elif type(criterion_types) is str:
            criterion_types = [criterion_types]
        targeting_configs = [self.TARGETING[criterion] for criterion in criterion_types]
        filters = self._build_filters()
        all_data = []
        for config in targeting_configs:
            data = self._get_stats(config, filters, kpi_params=kpi_params)
            all_data.extend(data)
        all_data.sort(key=lambda row: row[self.sort_key])
        return all_data

    def _build_filters(self, date_from=None, date_to=None):
        base_filter = Q(**{"ad_group__campaign__account_id": self.account.id})
        if date_from:
            base_filter &= Q(date__gte=date_from)
        if date_to:
            base_filter &= Q(date__lte=date_to)
        return base_filter

    def _get_stats(self, config, filters, kpi_params=None):
        """
        Retrieve stats with provided Criterion named tuple config
        Handles config containing multiple model / serializer pairs
        :param config: namedtuple: Criterion
        :param filters:
        :return:
        """
        if type(config.model) is list:
            config = zip(config.model, config.serializer)
        else:
            config = [config]

        all_data = []
        base_kpi_filters = defaultdict(lambda: dict(min=0, max=0))
        # Retrieve all data
        for model, serializer_class in config:
            queryset = model.objects.filter(filters)
            data = serializer_class(queryset, many=True, context=dict(now=self.now, kpi_params=kpi_params)).data
            self.update_kpi_filters(base_kpi_filters, KPI_FILTERS, data)
            all_data.extend(data)
        return all_data

    @staticmethod
    def update_kpi_filters(base_filters, kpi_filter_keys, serialized):
        def safe_compare(func, val1, val2):
            result = val1
            try:
                result = func(val1, val2)
            except TypeError:
                pass
            return result

        for item in serialized:
            # {"average_cpv": {"min": 0, "max": 0}, ...}
            for kpi in kpi_filter_keys:
                curr_min = safe_compare(min, base_filters[kpi]["min"], item.get(kpi))
                curr_max = safe_compare(max, base_filters[kpi]["max"], item.get(kpi))
                if curr_min is not None:
                    base_filters[kpi]["min"] = curr_min
                if curr_max is not None:
                    base_filters[kpi]["max"] = curr_max
