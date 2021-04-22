from rest_framework import serializers

from oauth.models import LineItem


class LineItemSerializer(serializers.ModelSerializer):
    class Meta:
        model = LineItem
        fields = ["id", "name", "insertion_order"]
