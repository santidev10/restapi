from utils.serializers.fields import ParentDictValueField


class StruckField(ParentDictValueField):
    def __init__(self, *args, **kwargs):
        super(StruckField, self).__init__(*args, dict_key="struck", **kwargs)
