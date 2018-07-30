from rest_framework.fields import SerializerMethodField


class ParentDictValueField(SerializerMethodField):
    def __init__(self, dict_key, *args, **kwargs):
        super(ParentDictValueField, self).__init__(*args, **kwargs)
        self.dict_key = dict_key

    def to_representation(self, value):
        parent_dict = getattr(self.parent, self.dict_key, {})
        return parent_dict.get(value.id, {}).get(self.field_name)
