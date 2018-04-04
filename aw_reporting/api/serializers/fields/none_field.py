from rest_framework.fields import SerializerMethodField


class NoneField(SerializerMethodField):
    def to_representation(self, value):
        return