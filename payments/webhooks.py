import json
import os

import stripe
from django.conf import settings
from django.core.mail import send_mail
from django.dispatch import Signal
from django.template.loader import render_to_string
from six import with_metaclass

from payments.stripe_api import customers, plans, subscriptions

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class WebhookRegistry(object):
    def __init__(self):
        self._registry = {}

    def register(self, webhook):
        self._registry[webhook.name] = {
            "webhook": webhook,
            "signal": Signal(providing_args=["event"])
        }

    def keys(self):
        return self._registry.keys()

    def get(self, name, default=None):
        try:
            return self[name]["webhook"]
        except KeyError:
            return default

    def get_signal(self, name, default=None):
        try:
            return self[name]["signal"]
        except KeyError:
            return default

    def signals(self):
        return {
            key: self.get_signal(key)
            for key in self.keys()
            }

    def __getitem__(self, name):
        return self._registry[name]


registry = WebhookRegistry()
del WebhookRegistry


class Registerable(type):
    def __new__(cls, clsname, bases, attrs):
        newclass = super(Registerable, cls).__new__(cls, clsname, bases, attrs)
        if getattr(newclass, "name", None) is not None:
            registry.register(newclass)
        return newclass


class Webhook(with_metaclass(Registerable, object)):
    def __init__(self, event):
        if event.kind != self.name:
            raise Exception(
                "The Webhook handler ({}) received the wrong type of Event ({})".format(self.name, event.kind))
        self.event = event

    def validate(self):
        evt = stripe.Event.retrieve(self.event.stripe_id)
        self.event.validated_message = json.loads(
            json.dumps(
                evt.to_dict(),
                sort_keys=True,
                cls=stripe.StripeObjectEncoder
            )
        )
        self.event.valid = self.event.webhook_message["data"] == self.event.validated_message["data"]
        self.event.save()

    def send_signal(self):
        signal = registry.get_signal(self.name)
        if signal:
            return signal.send(sender=self.__class__, event=self.event)

    def process(self):
        self.validate()
        if not self.event.valid or self.event.processed:
            return
        try:
            customers.link_customer(self.event)
            self.process_webhook()
            self.send_signal()
            self.event.processed = True
            self.event.save()
            self.proceed_event()
        except stripe.StripeError as e:
            # todo handle?
            pass

    def process_webhook(self):
        return

    def proceed_event(self):
        if hasattr(self, 'message'):
            self.event.customer.user.email_user('SaaS > Payment notifications',
                                                '{}\n\n'
                                                'Please do not respond to this email.'
                                                .format(self.message),
                                                from_email=settings.SENDER_EMAIL_ADDRESS)
            self.send_new_registration_email()

    def send_new_registration_email(self):

        context = {
            'user': 'Admin',
            'event': self.event,
            'message': self.message,
            'first_name': self.event.customer.user.first_name,
            'last_name': self.event.customer.user.last_name,
            'email': self.event.customer.user.email,
            'plan_name': self.event.customer.user.plan.payments_plan.name,
            'amount': self.event.customer.user.plan.payments_plan.amount,
            'plan_interval': self.event.customer.user.plan.payments_plan.interval,
            'create_at': self.event.customer.user.plan.payments_plan.created_at,
        }

        sender = settings.SENDER_EMAIL_ADDRESS
        to = settings.PAYMENT_ACTION_EMAIL_ADDRESSES

        subject = "Payment actions"
        text = "Dear Admin, \n\n" \
               "Event: {}\n\n" \
               "Message: {} \n\n".format(self.event, self.message)

        msg_html = render_to_string(os.path.join(BASE_DIR, 'payments/templates/subscription_email.html'),
                                    context=context)
        send_mail(subject, text, sender, to, fail_silently=True, html_message=msg_html)
        return


class AccountUpdatedWebhook(Webhook):
    name = "account.updated"
    description = "Occurs whenever an account status or property has changed."


class AccountApplicationDeauthorizeWebhook(Webhook):
    name = "account.application.deauthorized"
    description = "Occurs whenever a user deauthorizes an application. Sent to the related application only."


class AccountExternalAccountCreatedWebhook(Webhook):
    name = "account.external_account.created"
    description = "Occurs whenever an external account is created."


class AccountExternalAccountDeletedWebhook(Webhook):
    name = "account.external_account.deleted"
    description = "Occurs whenever an external account is deleted."


class AccountExternalAccountUpdatedWebhook(Webhook):
    name = "account.external_account.updated"
    description = "Occurs whenever an external account is updated."


class ApplicationFeeCreatedWebhook(Webhook):
    name = "application_fee.created"
    description = "Occurs whenever an application fee is created on a charge."


class ApplicationFeeRefundedWebhook(Webhook):
    name = "application_fee.refunded"
    description = "Occurs whenever an application fee is refunded, whether from refunding a charge or from refunding the application fee directly, including partial refunds."


class ApplicationFeeRefundUpdatedWebhook(Webhook):
    name = "application_fee.refund.updated"
    description = "Occurs whenever an application fee refund is updated."


class BalanceAvailableWebhook(Webhook):
    name = "balance.available"
    description = "Occurs whenever your Stripe balance has been updated (e.g. when a charge collected is available to be paid out). By default, Stripe will automatically transfer any funds in your balance to your bank account on a daily basis."


class BitcoinReceiverCreatedWebhook(Webhook):
    name = "bitcoin.receiver.created"
    description = "Occurs whenever a receiver has been created."


class BitcoinReceiverFilledWebhook(Webhook):
    name = "bitcoin.receiver.filled"
    description = "Occurs whenever a receiver is filled (that is, when it has received enough bitcoin to process a payment of the same amount)."


class BitcoinReceiverUpdatedWebhook(Webhook):
    name = "bitcoin.receiver.updated"
    description = "Occurs whenever a receiver is updated."


class BitcoinReceiverTransactionCreatedWebhook(Webhook):
    name = "bitcoin.receiver.transaction.created"
    description = "Occurs whenever bitcoin is pushed to a receiver."


class ChargeCapturedWebhook(Webhook):
    name = "charge.captured"
    description = "Occurs whenever a previously uncaptured charge is captured."


class ChargeFailedWebhook(Webhook):
    name = "charge.failed"
    description = "Occurs whenever a failed charge attempt occurs."


class ChargeRefundedWebhook(Webhook):
    name = "charge.refunded"
    description = "Occurs whenever a charge is refunded, including partial refunds."


class ChargeSucceededWebhook(Webhook):
    name = "charge.succeeded"
    description = "Occurs whenever a new charge is created and is successful."


class ChargeUpdatedWebhook(Webhook):
    name = "charge.updated"
    description = "Occurs whenever a charge description or metadata is updated."


class ChargeDisputeClosedWebhook(Webhook):
    name = "charge.dispute.closed"
    description = "Occurs when the dispute is resolved and the dispute status changes to won or lost."


class ChargeDisputeCreatedWebhook(Webhook):
    name = "charge.dispute.created"
    description = "Occurs whenever a customer disputes a charge with their bank (chargeback)."


class ChargeDisputeFundsReinstatedWebhook(Webhook):
    name = "charge.dispute.funds_reinstated"
    description = "Occurs when funds are reinstated to your account after a dispute is won."


class ChargeDisputeFundsWithdrawnWebhook(Webhook):
    name = "charge.dispute.funds_withdrawn"
    description = "Occurs when funds are removed from your account due to a dispute."


class ChargeDisputeUpdatedWebhook(Webhook):
    name = "charge.dispute.updated"
    description = "Occurs when the dispute is updated (usually with evidence)."


class CouponCreatedWebhook(Webhook):
    name = "coupon.created"
    description = "Occurs whenever a coupon is created."


class CouponDeletedWebhook(Webhook):
    name = "coupon.deleted"
    description = "Occurs whenever a coupon is deleted."


class CouponUpdatedWebhook(Webhook):
    name = "coupon.updated"
    description = "Occurs whenever a coupon is updated."


class CustomerCreatedWebhook(Webhook):
    name = "customer.created"
    description = "Occurs whenever a new customer is created."


class CustomerDeletedWebhook(Webhook):
    name = "customer.deleted"
    description = "Occurs whenever a customer is deleted."

    def process_webhook(self):
        customers.purge(self.event.customer)


class CustomerUpdatedWebhook(Webhook):
    name = "customer.updated"
    description = "Occurs whenever any property of a customer changes."

    def process_webhook(self):
        customers.sync_customer(self.event.customer)


class CustomerDiscountCreatedWebhook(Webhook):
    name = "customer.discount.created"
    description = "Occurs whenever a coupon is attached to a customer."


class CustomerDiscountDeletedWebhook(Webhook):
    name = "customer.discount.deleted"
    description = "Occurs whenever a customer's discount is removed."


class CustomerDiscountUpdatedWebhook(Webhook):
    name = "customer.discount.updated"
    description = "Occurs whenever a customer is switched from one coupon to another."


class CustomerSourceCreatedWebhook(Webhook):
    name = "customer.source.created"
    description = "Occurs whenever a new source is created for the customer."


class CustomerSourceDeletedWebhook(Webhook):
    name = "customer.source.deleted"
    description = "Occurs whenever a source is removed from a customer."


class CustomerSourceUpdatedWebhook(Webhook):
    name = "customer.source.updated"
    description = "Occurs whenever a source's details are changed."


class CustomerSubscriptionWebhook(Webhook):
    def process_webhook(self):
        if self.event.validated_message:
            subscriptions.sync_subscription_from_stripe_data(
                self.event.customer,
                self.event.validated_message["data"]["object"]
            )

        if self.event.customer:
            customers.sync_customer(self.event.customer, self.event.customer.stripe_customer)


class CustomerSubscriptionCreatedWebhook(CustomerSubscriptionWebhook):
    name = "customer.subscription.created"
    description = "Occurs whenever a customer with no subscription is signed up for a plan."
    message = 'Welcome with your first subscription, thank you for your choice'


class CustomerSubscriptionDeletedWebhook(CustomerSubscriptionWebhook):
    name = "customer.subscription.deleted"
    description = "Occurs whenever a customer ends their subscription."


class CustomerSubscriptionTrialWillEndWebhook(CustomerSubscriptionWebhook):
    name = "customer.subscription.trial_will_end"
    description = "Occurs three days before the trial period of a subscription is scheduled to end."


class CustomerSubscriptionUpdatedWebhook(CustomerSubscriptionWebhook):
    name = "customer.subscription.updated"
    description = "Occurs whenever a subscription changes. Examples would include switching from one plan to another, or switching status from trial to active."
    message = 'Your subscription have been updated'


class InvoiceCreatedWebhook(Webhook):
    name = "invoice.created"
    description = "Occurs whenever a new invoice is created. If you are using webhooks, Stripe will wait one hour after they have all succeeded to attempt to pay the invoice; the only exception here is on the first invoice, which gets created and paid immediately when you subscribe a customer to a plan. If your webhooks do not all respond successfully, Stripe will continue retrying the webhooks every hour and will not attempt to pay the invoice. After 3 days, Stripe will attempt to pay the invoice regardless of whether or not your webhooks have succeeded. See how to respond to a webhook."


class InvoicePaymentFailedWebhook(Webhook):
    name = "invoice.payment_failed"
    description = "Occurs whenever an invoice attempts to be paid, and the payment fails. This can occur either due to a declined payment, or because the customer has no active card. A particular case of note is that if a customer with no active card reaches the end of its free trial, an invoice.payment_failed notification will occur."
    message = 'Some problems with invoice'


class InvoicePaymentSucceededWebhook(Webhook):
    name = "invoice.payment_succeeded"
    description = "Occurs whenever an invoice attempts to be paid, and the payment succeeds."
    message = 'Payment succeeded'


class InvoiceUpdatedWebhook(Webhook):
    name = "invoice.updated"
    description = "Occurs whenever an invoice changes (for example, the amount could change)."


class InvoiceItemCreatedWebhook(Webhook):
    name = "invoiceitem.created"
    description = "Occurs whenever an invoice item is created."


class InvoiceItemDeletedWebhook(Webhook):
    name = "invoiceitem.deleted"
    description = "Occurs whenever an invoice item is deleted."


class InvoiceItemUpdatedWebhook(Webhook):
    name = "invoiceitem.updated"
    description = "Occurs whenever an invoice item is updated."


class OrderCreatedWebhook(Webhook):
    name = "order.created"
    description = "Occurs whenever an order is created."


class OrderPaymentFailedWebhook(Webhook):
    name = "order.payment_failed"
    description = "Occurs whenever payment is attempted on an order, and the payment fails."


class OrderPaymentSucceededWebhook(Webhook):
    name = "order.payment_succeeded"
    description = "Occurs whenever payment is attempted on an order, and the payment succeeds."


class OrderUpdatedWebhook(Webhook):
    name = "order.updated"
    description = "Occurs whenever an order is updated."


class PlanWebhook(Webhook):
    def process_webhook(self):
        plans.sync_plan(self.event.message["data"]["object"])


class PlanCreatedWebhook(PlanWebhook):
    name = "plan.created"
    description = "Occurs whenever a plan is created."


class PlanDeletedWebhook(Webhook):
    name = "plan.deleted"
    description = "Occurs whenever a plan is deleted."


class PlanUpdatedWebhook(PlanWebhook):
    name = "plan.updated"
    description = "Occurs whenever a plan is updated."


class ProductCreatedWebhook(Webhook):
    name = "product.created"
    description = "Occurs whenever a product is created."


class ProductUpdatedWebhook(Webhook):
    name = "product.updated"
    description = "Occurs whenever a product is updated."


class RecipientCreatedWebhook(Webhook):
    name = "recipient.created"
    description = "Occurs whenever a recipient is created."


class RecipientDeletedWebhook(Webhook):
    name = "recipient.deleted"
    description = "Occurs whenever a recipient is deleted."


class RecipientUpdatedWebhook(Webhook):
    name = "recipient.updated"
    description = "Occurs whenever a recipient is updated."


class SKUCreatedWebhook(Webhook):
    name = "sku.created"
    description = "Occurs whenever a SKU is created."


class SKUUpdatedWebhook(Webhook):
    name = "sku.updated"
    description = "Occurs whenever a SKU is updated."


class TransferCreatedWebhook(Webhook):
    name = "transfer.created"
    description = "Occurs whenever a new transfer is created."


class TransferFailedWebhook(Webhook):
    name = "transfer.failed"
    description = "Occurs whenever Stripe attempts to send a transfer and that transfer fails."


class TransferPaidWebhook(Webhook):
    name = "transfer.paid"
    description = "Occurs whenever a sent transfer is expected to be available in the destination bank account. If the transfer failed, a transfer.failed webhook will additionally be sent at a later time. Note to Connect users: this event is only created for transfers from your connected Stripe accounts to their bank accounts, not for transfers to the connected accounts themselves."


class TransferReversedWebhook(Webhook):
    name = "transfer.reversed"
    description = "Occurs whenever a transfer is reversed, including partial reversals."


class TransferUpdatedWebhook(Webhook):
    name = "transfer.updated"
    description = "Occurs whenever the description or metadata of a transfer is updated."


class PingWebhook(Webhook):
    name = "ping"
    description = "May be sent by Stripe at any time to see if a provided webhook URL is working."
