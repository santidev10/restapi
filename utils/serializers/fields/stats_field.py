from utils.serializers.fields.parent_dict_field import ParentDictValueField


# pylint: disable=abstract-method
class StatField(ParentDictValueField):
    def __init__(self, *args, **kwargs):
        super(StatField, self).__init__(*args, dict_key="stats", **kwargs)
# pylint: enable=abstract-method
