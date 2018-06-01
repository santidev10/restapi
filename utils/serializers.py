class ExcludeFieldsMixin:
    def _fields_to_exclude(self):
        return tuple()

    def _filter_fields(self, representation):
        fields_to_exclude = self._fields_to_exclude()
        for field in fields_to_exclude:
            representation.pop(field, None)
        return representation
