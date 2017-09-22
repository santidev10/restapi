import stripe
from django.utils import timezone
from django.utils.encoding import smart_str

from payments import utils
from payments.models import Customer
from payments.stripe_api import subscriptions


def can_charge(customer):
    if customer.date_purged is not None:
        return False
    if customer.default_source:
        return True
    return False


def create(user, card=None, plan=None):
    """
    Creates a Stripe customer.
    If a customer already exists, the existing customer will be returned.
    """
    stripe_customer = stripe.Customer.create(
        email=user.email,
        source=card,
        plan=plan
    )
    cus, created = Customer.objects.get_or_create(
        user=user,
        defaults={
            "stripe_id": stripe_customer["id"]
        }
    )
    if created:
        sync_customer(cus, stripe_customer)
    else:
        stripe.Customer.retrieve(stripe_customer["id"]).delete()
    return cus


def get_customer_for_user(user):
    return next(iter(Customer.objects.filter(user=user)), None)


def purge(customer):
    """
    Deletes the Stripe customer data and purges the linking of the transaction data to the user.
    """
    try:
        customer.stripe_customer.delete()
    except stripe.InvalidRequestError as e:
        if 'no such customer:' not in smart_str(e).lower():
            raise
    customer.user = None
    customer.date_purged = timezone.now()
    customer.save()


def set_default_source(customer, source):
    """
    Sets the default payment source for a customer
    """
    stripe_customer = customer.stripe_customer
    stripe_customer.default_source = source
    cu = stripe_customer.save()
    sync_customer(customer, cu=cu)


def sync_customer(customer, cu=None):
    """
    Syncronizes a local Customer object with details from the Stripe API
    """
    if cu is None:
        cu = customer.stripe_customer
    customer.account_balance = utils.convert_amount_for_db(cu["account_balance"], cu["currency"])
    customer.currency = cu["currency"] or ""
    customer.delinquent = cu["delinquent"]
    customer.default_source = cu["default_source"] or ""
    customer.save()
    for subscription in cu["subscriptions"]["data"]:
        subscriptions.sync_subscription_from_stripe_data(customer, subscription)


def link_customer(event):
    """
    Links a customer referenced in a webhook event message to the event object
    """
    cus_id = None
    customer_crud_events = [
        "customer.created",
        "customer.updated",
        "customer.deleted"
    ]
    if event.kind in customer_crud_events:
        cus_id = event.message["data"]["object"]["id"]
    else:
        cus_id = event.message["data"]["object"].get("customer", None)

    if cus_id is not None:
        customer = next(iter(Customer.objects.filter(stripe_id=cus_id)), None)
        if customer is not None:
            event.customer = customer
            event.save()
