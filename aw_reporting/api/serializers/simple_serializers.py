from rest_framework.serializers import ModelSerializer

from aw_reporting.models.salesforce import Category


class CategorySerializer(ModelSerializer):
    class Meta:
        model = Category
        fields = (
            'id', 'name'
        )
