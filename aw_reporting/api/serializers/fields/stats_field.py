from rest_framework.fields import SerializerMethodField


class StatField(SerializerMethodField):
    def to_representation(self, value):
        return self.parent.stats.get(value.id, {}).get(self.field_name)