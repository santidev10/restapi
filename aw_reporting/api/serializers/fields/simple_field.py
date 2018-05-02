from rest_framework import serializers


class SimpleField(serializers.FloatField):
    def to_representation(self, value):
        return value