from django.conf.urls import url

from payments.api.views import PlanView, SubscriptionUpdateView, Webhook
from payments.api.views import SubscriptionCreateView
from payments.api.views import SubscriptionDeleteView
from payments.api.views import SubscriptionView

urlpatterns = [
    url(r"^payment_plans/$", PlanView.as_view(), name="payment_plans"),
    url(r"^subscriptions/$", SubscriptionView.as_view(), name="subscription_list"),
    url(r"^subscriptions/create/$", SubscriptionCreateView.as_view(), name="subscription_create"),
    url(r"^subscriptions/(?P<pk>\d+)/delete/$", SubscriptionDeleteView.as_view(), name="subscription_delete"),
    url(r"^subscriptions/(?P<pk>\d+)/update/$", SubscriptionUpdateView.as_view(), name="subscription_update"),
    url(r"^webhook/$", Webhook.as_view(), name="stripe_webhook"),
]
