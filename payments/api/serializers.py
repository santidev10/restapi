from rest_framework.serializers import ModelSerializer

from payments.models import Plan
from payments.models import Subscription


class SubscriptionSerializer(ModelSerializer):
    class Meta:
        model = Subscription


class PlanSerializer(ModelSerializer):
    class Meta:
        model = Plan
