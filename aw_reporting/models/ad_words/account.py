from django.db import models
from django.db.models import Avg
from django.db.models import Case
from django.db.models import ExpressionWrapper
from django.db.models import F
from django.db.models import Min
from django.db.models import Q
from django.db.models import When

from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.managers import UserRelatedManagerMixin


class AccountManager(models.Manager, UserRelatedManagerMixin):
    _account_id_ref = "id"


class Account(models.Model):
    objects = AccountManager()
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=255, null=True, db_index=True)
    currency_code = models.CharField(max_length=5, null=True)
    timezone = models.CharField(max_length=100, null=True)
    can_manage_clients = models.BooleanField(default=False, db_index=True)
    is_test_account = models.BooleanField(default=False, db_index=True)
    managers = models.ManyToManyField("self", db_index=True)
    visible = models.BooleanField(default=True, db_index=True)
    update_time = models.DateTimeField(null=True, db_index=True)
    hourly_updated_at = models.DateTimeField(null=True)
    settings_updated_at = models.DateTimeField(null=True)
    is_active = models.BooleanField(null=False, default=True, db_index=True)

    ad_count = models.BigIntegerField(default=0, null=False, db_index=True)
    channel_count = models.BigIntegerField(default=0, null=False, db_index=True)
    video_count = models.BigIntegerField(default=0, null=False, db_index=True)
    interest_count = models.BigIntegerField(default=0, null=False, db_index=True)
    topic_count = models.BigIntegerField(default=0, null=False, db_index=True)
    keyword_count = models.BigIntegerField(default=0, null=False, db_index=True)

    def __init__(self, *args, **kwargs):
        skip_creating_account_creation = kwargs.pop("skip_creating_account_creation", False)
        super(Account, self).__init__(*args, **kwargs)
        self.skip_creating_account_creation = skip_creating_account_creation

    def __str__(self):
        return "Account: {}".format(self.name)

    @classmethod
    def user_mcc_objects(cls, user):
        return Account.objects.filter(
            mcc_permissions__aw_connection__user_relations__user=user
        ).order_by("id").distinct()

    @classmethod
    def user_objects(cls, user):
        return cls.objects.filter(
            Q(managers__id__in=cls.user_mcc_objects(user))
            | Q(id=DEMO_ACCOUNT_ID)
        )

    @property
    def start_date(self):
        return self.campaigns.aggregate(date=Min("start_date"))["date"]

    @property
    def end_date(self):
        dates = self.campaigns.all().values_list("end_date", flat=True)
        if None not in dates and dates:
            return max(dates)
        return None

    def get_video_completion_rate(self, rate: str):
        """
        Calculate average completion rates from campaigns
        :return:
        """
        rate = str(rate)
        rates = ["25", "50", "75", "100"]
        if rate not in rates:
            raise ValueError(f"Valid rates: {','.join(rates)}")
        completion_rate = self.campaigns\
            .filter(**{f"video_views_{rate}_quartile__gt": 0})\
            .annotate(
                completion_rate=Case(
                    When(impressions=0, then=0),
                    default=ExpressionWrapper(F(f"video_views_{rate}_quartile") / F("impressions") * 100,
                                              output_field=models.FloatField())
                )
            )\
            .aggregate(Avg("completion_rate"))["completion_rate__avg"]
        return completion_rate

    @property
    def active_view_viewability(self):
        """
        Calculate active view viewability average froom campaigns
        :return:
        """
        viewability = self.campaigns\
            .filter(active_view_viewability__gt=0)\
            .aggregate(Avg("active_view_viewability"))["active_view_viewability__avg"]
        return viewability
