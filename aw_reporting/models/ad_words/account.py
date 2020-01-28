from django.db import models
from django.db.models import Min
from django.db.models import Q

from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from userprofile.managers import UserRelatedManagerMixin


class AccountManager(models.Manager, UserRelatedManagerMixin):
    _account_id_ref = "id"


class Account(models.Model):
    objects = AccountManager()
    id = models.CharField(max_length=15, primary_key=True)
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
        ).order_by('id').distinct()

    @classmethod
    def user_objects(cls, user):
        return cls.objects.filter(
            Q(managers__id__in=cls.user_mcc_objects(user))
            | Q(id=DEMO_ACCOUNT_ID)
        )

    @property
    def start_date(self):
        return self.campaigns.aggregate(date=Min('start_date'))['date']

    @property
    def end_date(self):
        dates = self.campaigns.all().values_list('end_date', flat=True)
        if None not in dates and dates:
            return max(dates)
