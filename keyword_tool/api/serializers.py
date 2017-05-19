from keyword_tool.models import KeyWord, Interest, KeywordsList
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import SerializerMethodField


class InterestsSerializer(ModelSerializer):
    class Meta:
        model = Interest


class KeywordSerializer(ModelSerializer):
    keyword_text = SerializerMethodField()
    interests = InterestsSerializer(many=True, read_only=True)

    def __init__(self, *args, **kwargs):
        super(KeywordSerializer, self).__init__(*args, **kwargs)

    @staticmethod
    def get_keyword_text(obj):
        return obj.text

    class Meta:
        model = KeyWord
        fields = (
            "average_cpc", "competition", "interests",
            "keyword_text", "monthly_searches", "search_volume",
        )


class SavedListNameSerializer(ModelSerializer):
    is_owner = SerializerMethodField()

    def __init__(self, *args, **kwargs):
        self.request = kwargs.pop('request')
        super(SavedListNameSerializer, self).__init__(*args, **kwargs)

    def get_is_owner(self, obj):
        return obj.user_email == self.request.user.email

    class Meta:
        model = KeywordsList
        fields = ("id", "name", "is_owner")


class SavedListUpdateSerializer(ModelSerializer):
    class Meta:
        model = KeywordsList
        fields = ("name",)


class SavedListSerializer(SavedListNameSerializer):
    keywords = KeywordSerializer(many=True)

    class Meta:
        model = KeywordsList
        fields = SavedListNameSerializer.Meta.fields + ("keywords",)
