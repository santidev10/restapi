from django.db.models import Max
from rest_framework.fields import SerializerMethodField
from rest_framework.serializers import ModelSerializer

from aw_reporting.api.serializers.simple_serializers import \
    MCCAccountSerializer
from aw_reporting.models import Account, AWConnectionToUserRelation
from aw_reporting.utils import safe_max


class AWAccountConnectionRelationsSerializer(ModelSerializer):
    mcc_accounts = SerializerMethodField()
    email = SerializerMethodField()
    update_time = SerializerMethodField()

    @staticmethod
    def get_update_time(obj):
        data = Account.objects \
            .filter(managers__mcc_permissions__aw_connection=obj.connection) \
            .aggregate(updated_at=Max("update_time"),
                       hourly_updated_at=Max("hourly_updated_at"))
        return safe_max((data["updated_at"], data["hourly_updated_at"]))

    @staticmethod
    def get_email(obj):
        return obj.connection.email

    @staticmethod
    def get_mcc_accounts(obj):
        qs = Account.objects \
            .filter(mcc_permissions__aw_connection__user_relations=obj) \
            .order_by("name")
        return MCCAccountSerializer(qs, many=True).data

    class Meta:
        model = AWConnectionToUserRelation
        fields = ("id", "email", "mcc_accounts", "created", "update_time")
