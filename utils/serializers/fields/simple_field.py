from rest_framework import serializers


# pylint: disable=abstract-method
class SimpleField(serializers.Field):
    def to_representation(self, value):
        return value
# pylint: enable=abstract-method
