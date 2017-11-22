from rest_framework.serializers import ModelSerializer, SerializerMethodField

from payments.models import Plan
from payments.models import Subscription


class SubscriptionSerializer(ModelSerializer):

    plan = SerializerMethodField()

    class Meta:
        model = Subscription
        fields = (
            "stripe_id",
            "customer",
            "application_fee_percent",
            "cancel_at_period_end",
            "canceled_at",
            "current_period_end",
            "current_period_start",
            "ended_at",
            "plan",
            "quantity",
            "start",
            "status",
            "trial_end",
            "trial_start",
        )

    def get_plan(self, obj):
        return PlanSerializer(obj.plan).data


class PlanSerializer(ModelSerializer):
    class Meta:
        model = Plan
