from rest_framework.serializers import ModelSerializer, IntegerField, ValidationError

from brand_safety.models import BadWord, BadWordCategory


class BadWordSerializer(ModelSerializer):
    category_ref_id = IntegerField(required=True)

    def validate(self, data):
        """
        Accepts category_ref_id field for setting BadWordCategory values
        :param data: Request data to validate
        :return: dict -> Validated data
        """
        try:
            category_id = data["category_ref_id"]
        except KeyError:
            raise ValidationError('You must provide the field category as an id.')
        try:
            category = BadWordCategory.objects.get(id=category_id)
            data['category'] = category.name
            data['category_ref'] = category
        except BadWordCategory.DoesNotExist:
            raise ValidationError('The provided category_id {} was not found.'.format(category_id))
        return data

    class Meta:
        model = BadWord
        fields = ("id", "name", "category_ref_id")
