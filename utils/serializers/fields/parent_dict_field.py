from rest_framework.fields import Field


# pylint: disable=abstract-method
class ParentDictValueField(Field):
    def __init__(self, dict_key, property_key=None, *args, **kwargs):
        if "source" not in kwargs:
            kwargs["source"] = "id"
        super(ParentDictValueField, self).__init__(*args, **kwargs)
        self.dict_key = dict_key
        self.property_key = property_key

    def get_attribute(self, instance):
        parent_dict = getattr(self.parent, self.dict_key, {})
        key = super(ParentDictValueField, self).get_attribute(instance)
        return parent_dict.get(key, {}).get(self.property_key or self.field_name)

    def to_representation(self, value):
        return value
# pylint: enable=abstract-method
