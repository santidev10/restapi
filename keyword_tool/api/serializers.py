from rest_framework.serializers import ModelSerializer, ValidationError
from rest_framework.serializers import SerializerMethodField

from keyword_tool.models import KeyWord, Interest, KeywordsList, AVAILABLE_KEYWORD_LIST_CATEGORIES


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
    top_keywords = SerializerMethodField()

    def __init__(self, *args, **kwargs):
        self.request = kwargs.pop('request')
        super(SavedListNameSerializer, self).__init__(*args, **kwargs)

    def get_is_owner(self, obj):
        return obj.user_email == self.request.user.email

    def get_top_keywords(self, obj):
        kw_ids = obj.keywords.through.objects.filter(keywordslist_id=obj.id).values_list('keyword__text', flat=True)
        return KeyWord.objects.filter(text__in=kw_ids).order_by('-search_volume').values_list('text', flat=True)[:10]

    def validate(self, data):
        """
        Check keyword list category
        """
        kw_list_category = data.get("category")
        user = self.request.user
        if kw_list_category is not None:
            if kw_list_category != "private" and not user.is_staff:
                raise ValidationError(
                    "Not valid category. Options are: private")
            elif kw_list_category not in AVAILABLE_KEYWORD_LIST_CATEGORIES:
                raise ValidationError(
                    "Not valid category. Options are: {}".format(
                        ", ".join(AVAILABLE_KEYWORD_LIST_CATEGORIES)))
        return data

    class Meta:
        model = KeywordsList
        fields = (
            "id", "name", "category", "is_owner", "top_keywords", "num_keywords",
            "average_volume", "average_cpc", "competition",
            "average_cpv", "average_view_rate", "average_ctrv",
        )


class SavedListUpdateSerializer(ModelSerializer):
    def __init__(self, *args, **kwargs):
        self.request = kwargs.pop('request')
        super(SavedListUpdateSerializer, self).__init__(*args, **kwargs)

    def validate(self, data):
        """
        Check keyword list category
        """
        kw_list_category = data.get("category")
        user = self.request.user
        if kw_list_category is not None:
            if kw_list_category != "private" and not user.is_staff:
                raise ValidationError(
                    "Not valid category. Options are: private")
            elif kw_list_category not in AVAILABLE_KEYWORD_LIST_CATEGORIES:
                raise ValidationError(
                    "Not valid category. Options are: {}".format(
                        ", ".join(AVAILABLE_KEYWORD_LIST_CATEGORIES)))
        return data

    class Meta:
        model = KeywordsList
        fields = ("name",)


class SavedListSerializer(SavedListNameSerializer):
    keywords = KeywordSerializer(many=True)

    class Meta:
        model = KeywordsList
        fields = SavedListNameSerializer.Meta.fields + ("keywords",)
