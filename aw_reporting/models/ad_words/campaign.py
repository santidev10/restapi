from django.db import models

from aw_reporting.models.ad_words.account import Account
from aw_reporting.models.ad_words.constants import BudgetType
from aw_reporting.models.ad_words.statistic import BaseClicksTypesStatisticsModel
from aw_reporting.models.ad_words.statistic import ModelPlusDeNormFields
from aw_reporting.models.salesforce import OpPlacement
from userprofile.managers import UserRelatedManagerMixin


class CampaignManager(models.Manager, UserRelatedManagerMixin):
    _account_id_ref = "account_id"


class Campaign(ModelPlusDeNormFields, BaseClicksTypesStatisticsModel):
    objects = CampaignManager()

    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250)
    account = models.ForeignKey(Account, null=True, related_name='campaigns', on_delete=models.CASCADE)

    start_date = models.DateField(null=True, db_index=True)
    end_date = models.DateField(null=True, db_index=True)
    type = models.CharField(max_length=20, null=True, db_index=True)
    budget = models.FloatField(null=True, db_index=True)
    status = models.CharField(max_length=10, null=True, db_index=True)
    update_time = models.DateTimeField(auto_now_add=True)
    sync_time = models.DateTimeField(null=True)
    salesforce_placement = models.ForeignKey(
        OpPlacement,
        null=True,
        related_name='adwords_campaigns',
        on_delete=models.SET_NULL,
        db_index=True,
    )
    goal_allocation = models.FloatField(default=0)

    # setup fields
    targeting_interests = models.BooleanField(default=False)
    targeting_topics = models.BooleanField(default=False)
    targeting_keywords = models.BooleanField(default=False)
    targeting_channels = models.BooleanField(default=False)
    targeting_videos = models.BooleanField(default=False)
    targeting_remarketings = models.BooleanField(default=False)
    targeting_custom_affinity = models.BooleanField(default=False)
    tracking_template_is_set = models.BooleanField(default=False)
    targeting_excluded_channels = models.BooleanField(default=False)
    targeting_excluded_topics = models.BooleanField(default=False)
    targeting_excluded_keywords = models.BooleanField(default=False)

    budget_type = models.CharField(max_length=30, default=BudgetType.DAILY.value, null=False, blank=False)

    _start = models.DateField(null=True, db_index=True)
    _end = models.DateField(null=True, db_index=True)
    placement_code = models.CharField(max_length=10, null=True, default=None)

    SERVING_STATUSES = ("serving", "eligible", "pending", "suspended", "ended", "none")

    @property
    def start(self):
        return self._start or self.start_date

    @property
    def end(self):
        return self._end or self.end_date

    def __str__(self):
        return "%s" % self.name
