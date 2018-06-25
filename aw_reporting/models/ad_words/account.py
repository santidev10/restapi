from django.db import models
from django.db.models import Min


class Account(models.Model):
    id = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=250, null=True)
    currency_code = models.CharField(max_length=5, null=True)
    timezone = models.CharField(max_length=100, null=True)
    can_manage_clients = models.BooleanField(default=False)
    is_test_account = models.BooleanField(default=False)
    managers = models.ManyToManyField("self")
    visible = models.BooleanField(default=True)
    update_time = models.DateTimeField(null=True)
    hourly_updated_at = models.DateTimeField(null=True)
    settings_updated_at = models.DateTimeField(null=True)

    def __str__(self):
        return "Account: {}".format(self.name)

    @classmethod
    def user_mcc_objects(cls, user):
        qs = Account.objects.filter(
            mcc_permissions__aw_connection__user_relations__user=user
        ).order_by('id').distinct()
        return qs

    @classmethod
    def user_objects(cls, user):
        manager_ids = set(
            Account.objects.filter(
                mcc_permissions__aw_connection__user_relations__user=user
            ).values_list('id', flat=True)
        )
        qs = cls.objects.filter(
            managers__id__in=manager_ids,
        )
        return qs

    @property
    def start_date(self):
        return self.campaigns.aggregate(date=Min('start_date'))['date']

    @property
    def end_date(self):
        dates = self.campaigns.all().values_list('end_date', flat=True)
        if None not in dates and dates:
            return max(dates)
