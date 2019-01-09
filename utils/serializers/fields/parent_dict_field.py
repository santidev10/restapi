from rest_framework.fields import Field


class ParentDictValueField(Field):
    def __init__(self, dict_key, *args, **kwargs):
        super(ParentDictValueField, self).__init__(*args, **kwargs)
        self.dict_key = dict_key

    def get_attribute(self, instance):
        parent_dict = getattr(self.parent, self.dict_key, {})
        return parent_dict.get(instance.id, {}).get(self.field_name)

    def to_representation(self, value):
        return value
