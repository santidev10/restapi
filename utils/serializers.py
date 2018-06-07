from rest_framework.serializers import Serializer


class ExcludeFieldsMixin:
    def _fields_to_exclude(self):
        return tuple()

    def _filter_fields(self: Serializer):
        fields_to_exclude = self._fields_to_exclude()
        for field in fields_to_exclude:
            self.fields.pop(field, None)
