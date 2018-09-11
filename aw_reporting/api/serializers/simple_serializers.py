from rest_framework.serializers import ModelSerializer

from aw_reporting.models import AdGroup, Account
from aw_reporting.models.salesforce import Category


class CategorySerializer(ModelSerializer):
    class Meta:
        model = Category
        fields = ("id", "name")


class AdGroupListSerializer(ModelSerializer):
    def get_queryset(self, *args, **kwargs):
        raise KeyError
    class Meta:
        model = AdGroup
        fields = ("id", "name", "status")


class MCCAccountSerializer(ModelSerializer):
    class Meta:
        model = Account
        fields = ("id", "name", "currency_code", "timezone")
