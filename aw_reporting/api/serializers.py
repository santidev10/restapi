from rest_framework.serializers import ModelSerializer, \
    SerializerMethodField
from aw_reporting.models import AWConnection, Account


class MCCAccountSerializer(ModelSerializer):
    class Meta:
        model = Account
        fields = ("id", "name", "currency_code", "timezone")


class AWAccountConnectionSerializer(ModelSerializer):
    mcc_accounts = SerializerMethodField()

    @staticmethod
    def get_mcc_accounts(obj):
        qs = Account.objects.filter(
            mcc_permissions__aw_connection=obj).order_by("name")
        return MCCAccountSerializer(qs, many=True).data

    class Meta:
        model = AWConnection
        fields = ("email", "mcc_accounts")


