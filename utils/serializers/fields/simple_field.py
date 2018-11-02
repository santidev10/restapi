from rest_framework import serializers


class SimpleField(serializers.Field):
    def to_representation(self, value):
        return value
