from django.db import models

from aw_reporting.models.ad_words.account import Account
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
    account = models.ForeignKey(Account, null=True, related_name='campaigns')

    start_date = models.DateField(null=True, db_index=True)
    end_date = models.DateField(null=True)
    type = models.CharField(max_length=20, null=True)
    budget = models.FloatField(null=True)
    status = models.CharField(max_length=10, null=True)
    update_time = models.DateTimeField(auto_now_add=True)
    salesforce_placement = models.ForeignKey(
        OpPlacement,
        null=True,
        related_name='adwords_campaigns',
        on_delete=models.SET_NULL,
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

    _start = models.DateField(null=True)
    _end = models.DateField(null=True)
    placement_code = models.CharField(max_length=10, null=True, default=None)

    SERVING_STATUSES = ("eligible", "pending", "suspended", "ended", "none")

    @property
    def start(self):
        return self._start or self.start_date

    @property
    def end(self):
        return self._end or self.end_date

    def __str__(self):
        return "%s" % self.name
