from rest_framework import serializers


class PercentField(serializers.FloatField):
    def to_representation(self, value):
        return super(PercentField, self).to_representation(value) * 100.
