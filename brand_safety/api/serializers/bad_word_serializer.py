from rest_framework.serializers import ModelSerializer, CharField, ValidationError

from brand_safety.models import BadWord, BadWordCategory


class BadWordSerializer(ModelSerializer):
    category = CharField(max_length=80, required=True)

    def validate(self, data):
        category_name = data['category']
        category_ref = BadWordCategory.from_string(category_name)
        data['category_ref'] = category_ref
        return data

    class Meta:
        model = BadWord
        fields = ("id", "name", "category")
