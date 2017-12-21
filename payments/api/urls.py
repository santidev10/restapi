from django.conf.urls import url

from payments.api.views import PlanView, Webhook, PaymentSourceSetView

urlpatterns = [
    url(r"^payment_plans/$", PlanView.as_view(), name="payment_plans"),
    url(r"^payment_source/set/$", PaymentSourceSetView.as_view(), name="payment_source_set"),
    url(r"^webhook/$", Webhook.as_view(), name="stripe_webhook"),
]
