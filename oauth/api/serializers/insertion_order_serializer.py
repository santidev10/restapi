from rest_framework import serializers

from oauth.models import InsertionOrder


class InsertionOrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = InsertionOrder
        fields = "__all__"
