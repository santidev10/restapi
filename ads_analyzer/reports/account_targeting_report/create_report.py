from collections import namedtuple

from django.db.models import Q
from django.utils import timezone

from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import CriterionType
from aw_reporting.models import YTChannelStatistic
from aw_reporting.models import YTVideoStatistic
from aw_reporting.models import VideoCreativeStatistic

from ads_analyzer.reports.opportunity_targeting_report.serializers import DemoGenderTableSerializer
from ads_analyzer.reports.opportunity_targeting_report.serializers import DevicesTableSerializer

from ads_analyzer.reports.account_targeting_report.serializers import VideoCreativeTableSerializer
from ads_analyzer.reports.account_targeting_report.serializers import KeywordTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AgeTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementChannelTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import PlacementVideoTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import TopicTargetingSerializer
from ads_analyzer.reports.account_targeting_report.serializers import AudienceTargetingSerializer


Criterion = namedtuple("Criterion", "model serializer")
CostDelivery = namedtuple("CostDelivery", "cost impressions views")


class AccountTargetingReport:
    TARGETING = {
        CriterionType.AGE: Criterion(AgeRangeStatistic, AgeTargetingSerializer),
        "Creative": Criterion(VideoCreativeStatistic, VideoCreativeTableSerializer),
        "Device": Criterion(AdGroupStatistic, DevicesTableSerializer),
        CriterionType.GENDER: Criterion(GenderStatistic, DemoGenderTableSerializer),
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

    def get_stats(self, criterion_types):
        """
        Retrieve statistics for provided criterion_types values
        :param criterion_types: list [str, str, ...] -> List of Adgroup criterion types to retrieve
            stats for
            Some configs may have multiple model / serializer pairs
        :return: list
        """
        if type(criterion_types) is str:
            criterion_types = [criterion_types]
        targeting_configs = [self.TARGETING[criterion] for criterion in criterion_types]
        filters = self._build_filters()
        all_data = []
        for config in targeting_configs:
            data = self._get_stats(config, filters)
            all_data.extend(data)
        all_data.sort(key=lambda row: row[self.sort_key])
        return all_data

    def _build_filters(self, date_from=None, date_to=None):
        filters = Q(**{"ad_group__campaign__account_id": self.account.id})
        if date_from:
            filters = filters & Q(date__gte=date_from)
        if date_to:
            filters = filters & Q(date__lte=date_to)
        return filters

    def _get_stats(self, config, filters):
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
        # Retrieve all data
        for model, serializer in config:
            queryset = model.objects\
                .filter(filters)
            data = serializer(queryset, many=True, context=dict(now=self.now)).data
            all_data.extend(data)
        return all_data
