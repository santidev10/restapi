from utils.serializers.fields import ParentDictValueField


class StatField(ParentDictValueField):
    def __init__(self, *args, **kwargs):
        super(StatField, self).__init__(*args, dict_key="stats", **kwargs)
