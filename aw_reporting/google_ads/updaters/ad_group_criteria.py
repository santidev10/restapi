"""
Module used to gather AdGroup targeting criteria using performance report methods
"""

import logging

from aw_reporting.adwords_reports import age_range_performance_report
from aw_reporting.adwords_reports import audience_performance_report
from aw_reporting.adwords_reports import gender_performance_report
from aw_reporting.adwords_reports import keywords_performance_report
from aw_reporting.adwords_reports import parent_performance_report
from aw_reporting.adwords_reports import placement_performance_report
from aw_reporting.adwords_reports import topics_performance_report
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupTargeting
from aw_reporting.models import AgeRanges
from aw_reporting.models import Genders
from aw_reporting.models import ParentStatuses
from aw_reporting.models import Topic
from aw_reporting.models import CriteriaTypeEnum
from aw_reporting.models import TargetingStatusEnum
from aw_reporting.google_ads.updaters.interests import AudienceAWType
from aw_reporting.google_ads.utils import get_criteria_exists_key
from utils.utils import chunks_generator
from utils.db.functions import safe_bulk_create

logger = logging.getLogger(__name__)


class AdGroupCriteriaUpdater(object):
    BATCH_SIZE = 5000
    REPORT_FIELDS = ["AdGroupId", "Status", "Criteria", "Id"]
    WITH_NEGATIVE = REPORT_FIELDS + ["IsNegative"]
    UPDATE_FIELDS = ["status", "is_negative", "criteria"]
    # Define configs for retrieving criteria from each report function
    REPORTS = None

    def __init__(self, account):
        self.account = account
        self.topics = dict(Topic.objects.values_list("name", "id"))
        self.ad_group_ids = set(AdGroup.objects.filter(campaign__account=self.account).values_list("id", flat=True))
        self.REPORTS = [
            (audience_performance_report, CriteriaTypeEnum.USER_LIST, self.REPORT_FIELDS,
                self._get_audience_predicate(as_user_list=True)),
            (audience_performance_report, CriteriaTypeEnum.USER_INTEREST, self.REPORT_FIELDS,
                self._get_audience_predicate(as_user_list=False)),
            (placement_performance_report, CriteriaTypeEnum.PLACEMENT, self.WITH_NEGATIVE,
                self._get_placement_predicate()),
            # Not all reports to gather criteria require specific predicates
            (keywords_performance_report, CriteriaTypeEnum.KEYWORD, self.WITH_NEGATIVE, None),
            (age_range_performance_report, CriteriaTypeEnum.AGE_RANGE, self.WITH_NEGATIVE, None),
            (gender_performance_report, CriteriaTypeEnum.GENDER, self.WITH_NEGATIVE, None),
            (parent_performance_report, CriteriaTypeEnum.PARENT, self.WITH_NEGATIVE, None),
            (topics_performance_report, CriteriaTypeEnum.VERTICAL, self.WITH_NEGATIVE, None),
        ]

    def update(self, client):
        for report_func, criteria_type_enum, fields, predicates in self.REPORTS:
            report = report_func(client, dates=None, fields=fields, predicates=predicates)
            criteria_type_name = criteria_type_enum.name
            existing_targeting = {
                (item.ad_group_id, item.type_id, item.statistic_criteria): item.id
                for item in AdGroupTargeting.objects.filter(ad_group__campaign__account=self.account,
                                                            type=criteria_type_enum.value)
            }
            for batch in chunks_generator(report, size=self.BATCH_SIZE):
                to_update = []
                to_create = []
                for row_obj in batch:
                    ad_group_id = int(row_obj.AdGroupId)
                    if ad_group_id not in self.ad_group_ids:
                        continue
                    statistic_criteria = self._get_statistic_criteria(row_obj.Criteria, criteria_type_name)
                    criteria_exists_key = get_criteria_exists_key(ad_group_id, criteria_type_enum.value, statistic_criteria)
                    # statistic_criteria are values aw_reporting.google_ads.updaters use to store statistic criteria
                    # and is used to match aggregated statistics with targeting
                    targeting_item = AdGroupTargeting(
                        ad_group_id=row_obj.AdGroupId,
                        type_id=criteria_type_enum.value,
                        is_negative=True if getattr(row_obj, "IsNegative", None) is "TRUE" else False,
                        criteria=row_obj.Criteria,
                        status=TargetingStatusEnum[row_obj.Status.upper()].value,
                        statistic_criteria=statistic_criteria,
                    )
                    # Update if exists and has not been mutated by user, else is pending to be synced with Google Ads
                    if criteria_exists_key in existing_targeting and targeting_item.sync_pending is False:
                        targeting_item.id = existing_targeting[criteria_exists_key]
                        to_update.append(targeting_item)
                    else:
                        to_create.append(targeting_item)
                AdGroupTargeting.objects.bulk_update(to_update, fields=self.UPDATE_FIELDS)
                safe_bulk_create(AdGroupTargeting, to_create, batch_size=self.BATCH_SIZE)

    def _get_statistic_criteria(self, criteria, criteria_type_name):
        """
        Method to get same criteria values that aw_reporting.google_ads.updaters statistics updates use
        These values will be used to easily match internal criteria values such as GenderStatistic.gender_id to
            AdGroupTargeting api values
        :param criteria: Adwords Criteria Report Criteria  value
        :param criteria_type_name: CriteriaTypeEnum.nameCriteriaTypeEnum.name
        :return:
        """
        method_mapping = {
            CriteriaTypeEnum.KEYWORD.name: self._get_keyword_criteria,
            CriteriaTypeEnum.AGE_RANGE.name: self._get_age_range_criteria,
            CriteriaTypeEnum.PARENT.name: self._get_parent_criteria,
            CriteriaTypeEnum.VERTICAL.name: self._get_topic_criteria,
            CriteriaTypeEnum.USER_LIST.name: self._get_audience_criteria,
            CriteriaTypeEnum.USER_INTEREST.name: self._get_audience_criteria,
            CriteriaTypeEnum.GENDER.name: self._get_gender_criteria,
            CriteriaTypeEnum.PLACEMENT.name: self._get_placement_criteria,
            CriteriaTypeEnum.YOUTUBE_CHANNEL.name: self._get_placement_criteria,
            CriteriaTypeEnum.YOUTUBE_VIDEO.name: self._get_placement_criteria,
        }
        method = method_mapping[criteria_type_name]
        statistics_criteria = str(method(criteria))
        return statistics_criteria

    def _get_placement_criteria(self, criteria):
        value = criteria
        # only youtube ids we need in criteria
        if "youtube.com/" in criteria:
            value = criteria.split("/")[-1]
        return value

    def _get_keyword_criteria(self, criteria):
        return criteria

    def _get_audience_criteria(self, criteria):
        au_type, au_id, *_ = criteria.split("::")
        return au_id

    def _get_parent_criteria(self, criteria):
        parent_status = ParentStatuses.index(criteria)
        return parent_status

    def _get_gender_criteria(self, criteria):
        gender = Genders.index(criteria)
        return gender

    def _get_age_range_criteria(self, criteria):
        age_range = AgeRanges.index(criteria)
        return age_range

    def _get_topic_criteria(self, criteria):
        topic = self.topics[criteria]
        return topic

    def _get_audience_predicate(self, as_user_list=True):
        """
        Get audience predicates
        For CriteriaTypeEnum.USER_LIST, operator contains AudienceAWType.REMARK
            CriteriaTypeEnum.USER_INTEREST operator excludes AudienceAWType.REMARK
        """
        predicate = [{
            "field": "Criteria",
            "operator": None,
            "values": AudienceAWType.REMARK,
        }]
        operator = "CONTAINS" if as_user_list is True else "DOES_NOT_CONTAIN"
        predicate[0]["operator"] = operator
        return predicate

    def _get_placement_predicate(self):
        """ Get managed Youtube placements predicate """
        predicate = [
            {
                "field": "AdNetworkType1",
                "operator": "EQUALS",
                "values": ["YOUTUBE_WATCH"]
            },
            {
                "field": "IsNegative",
                "operator": "EQUALS",
                "values": "FALSE",
            },
            {
                "field": "Id",
                "operator": "GREATER_THAN",
                "values": "0",
            }
        ]
        return predicate
