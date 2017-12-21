import stripe
from django.db.models import Q
from django.utils import timezone
from django.utils.encoding import smart_str

from payments import utils
from payments.models import Subscription, Customer, Plan


def cancel(subscription, at_period_end=True):
    """
    Cancels a subscription
    at_period_end: True, to cancel at the end, otherwise immediately cancel
    """
    sub = subscription.stripe_subscription.delete(at_period_end=at_period_end)
    sync_subscription_from_stripe_data(subscription.customer, sub)


def create(customer, plan, token, quantity=None, coupon=None):
    """
    Creates a subscription for the given customer
    """
    cu = customer.stripe_customer

    subscription_params = {}
    subscription_params["source"] = token
    subscription_params["plan"] = plan
    subscription_params["quantity"] = quantity
    subscription_params["coupon"] = coupon
    resp = cu.subscriptions.create(**subscription_params)

    return sync_subscription_from_stripe_data(customer, resp)


def has_active_subscription(customer):
    """
    Checks if the given customer has an active subscription
    """
    return Subscription.objects.filter(customer=customer).filter(
        Q(ended_at__isnull=True) | Q(ended_at__gt=timezone.now())).exists()


def is_period_current(subscription):
    """
    Tests if the provided subscription object for the current period
    """
    return subscription.current_period_end > timezone.now()


def is_status_current(subscription):
    """
    Tests if the provided subscription object has a status that means current
    """
    return subscription.status in ["active", "trialing", ]


def is_valid(subscription):
    """
    Tests if the provided subscription object is valid
    """
    if not is_status_current(subscription):
        return False

    if subscription.cancel_at_period_end and not is_period_current(subscription):
        return False

    return True


def retrieve(customer, sub_id):
    """
    Retrieve a subscription object from Stripe's API

    Stripe throws an exception if a subscription has been deleted that we are
    attempting to sync. In this case we want to just silently ignore that
    exception but pass on any other.
    """
    if not sub_id:
        return
    try:
        return customer.stripe_customer.subscriptions.retrieve(sub_id)
    except stripe.InvalidRequestError as e:
        if smart_str(e).find("does not have a subscription with ID") == -1:
            raise


def sync_subscription_from_stripe_data(customer, subscription):
    """
    Syncronizes data from the Stripe API for a subscription
    """
    defaults = dict(
        customer=customer,
        application_fee_percent=subscription["application_fee_percent"],
        cancel_at_period_end=subscription["cancel_at_period_end"],
        canceled_at=utils.convert_tstamp(subscription["canceled_at"]),
        current_period_start=utils.convert_tstamp(subscription["current_period_start"]),
        current_period_end=utils.convert_tstamp(subscription["current_period_end"]),
        ended_at=utils.convert_tstamp(subscription["ended_at"]),
        plan=Plan.objects.get(stripe_id=subscription["plan"]["id"]),
        quantity=subscription["quantity"],
        start=utils.convert_tstamp(subscription["start"]),
        status=subscription["status"],
        trial_start=utils.convert_tstamp(subscription["trial_start"]) if subscription["trial_start"] else None,
        trial_end=utils.convert_tstamp(subscription["trial_end"]) if subscription["trial_end"] else None
    )
    sub, created = Subscription.objects.get_or_create(
        stripe_id=subscription["id"],
        defaults=defaults
    )
    sub = utils.update_with_defaults(sub, defaults, created)
    return sub


def update(subscription, plan=None, quantity=None, prorate=True, coupon=None, charge_immediately=False):
    """
    Updates a subscription
    """
    stripe_subscription = subscription.stripe_subscription
    if plan:
        stripe_subscription.plan = plan
    if quantity:
        stripe_subscription.quantity = quantity
    if not prorate:
        stripe_subscription.prorate = False
    if coupon:
        stripe_subscription.coupon = coupon
    if charge_immediately:
        if utils.convert_tstamp(stripe_subscription.trial_end) > timezone.now():
            stripe_subscription.trial_end = 'now'
    sub = stripe_subscription.save()
    customer = Customer.objects.get(pk=subscription.customer.pk)
    sync_subscription_from_stripe_data(customer, sub)
