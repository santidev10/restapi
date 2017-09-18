import stripe
from django.contrib.postgres.fields import JSONField
from django.db import models
from django.utils import timezone
from django.utils.functional import cached_property

from payments.utils import CURRENCY_SYMBOLS
from userprofile.models import UserProfile


class StripeObject(models.Model):
    stripe_id = models.CharField(max_length=255, unique=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        abstract = True


class Plan(StripeObject):
    amount = models.DecimalField(decimal_places=2, max_digits=9)
    currency = models.CharField(max_length=15)
    interval = models.CharField(max_length=15)
    interval_count = models.IntegerField()
    name = models.CharField(max_length=150)
    statement_descriptor = models.TextField(blank=True)
    trial_period_days = models.IntegerField(null=True)
    metadata = JSONField(null=True)

    def __str__(self):
        return "{} ({}{})".format(self.name, CURRENCY_SYMBOLS.get(self.currency, ""), self.amount)


class Customer(StripeObject):
    user = models.OneToOneField(UserProfile, null=True, on_delete=models.CASCADE)
    account_balance = models.DecimalField(decimal_places=2, max_digits=9, null=True)
    currency = models.CharField(max_length=10, default="usd", blank=True)
    delinquent = models.BooleanField(default=False)
    default_source = models.TextField(blank=True)
    date_purged = models.DateTimeField(null=True, editable=False)

    @cached_property
    def stripe_customer(self):
        return stripe.Customer.retrieve(self.stripe_id)

    def __str__(self):
        return str(self.user)


class Subscription(StripeObject):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    application_fee_percent = models.DecimalField(decimal_places=2, max_digits=3, default=None, null=True)
    cancel_at_period_end = models.BooleanField(default=False)
    canceled_at = models.DateTimeField(blank=True, null=True)
    current_period_end = models.DateTimeField(blank=True, null=True)
    current_period_start = models.DateTimeField(blank=True, null=True)
    ended_at = models.DateTimeField(blank=True, null=True)
    plan = models.ForeignKey(Plan, on_delete=models.CASCADE)
    quantity = models.IntegerField()
    start = models.DateTimeField()
    status = models.CharField(max_length=25)  # trialing, active, past_due, canceled, or unpaid
    trial_end = models.DateTimeField(blank=True, null=True)
    trial_start = models.DateTimeField(blank=True, null=True)

    @property
    def stripe_subscription(self):
        return stripe.Customer.retrieve(self.customer.stripe_id).subscriptions.retrieve(self.stripe_id)

    @property
    def total_amount(self):
        return self.plan.amount * self.quantity

    def plan_display(self):
        return self.plan.name

    def status_display(self):
        return self.status.replace("_", " ").title()

    def delete(self, using=None):
        super(Subscription, self).delete(using=using)
        self.status = None
        self.quantity = 0
        self.amount = 0
