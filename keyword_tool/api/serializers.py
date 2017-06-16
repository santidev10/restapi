from rest_framework.serializers import ModelSerializer, ValidationError
from rest_framework.serializers import SerializerMethodField

from keyword_tool.models import KeyWord, Interest, KeywordsList, AVAILABLE_KEYWORD_LIST_CATEGORIES
from userprofile.models import UserProfile


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
    owner = SerializerMethodField()
    is_owner = SerializerMethodField()
    is_editable = SerializerMethodField()

    def __init__(self, *args, **kwargs):
        fields = kwargs.pop('fields', None)
        self.request = kwargs.pop('request')
        super(SavedListNameSerializer, self).__init__(*args, **kwargs)
        if fields is not None:
            requested_fields = set(fields)
            pre_defined_fields = set(self.fields.keys())
            difference = pre_defined_fields - requested_fields
            for field_name in difference:
                self.fields.pop(field_name)

    def get_owner(self, obj):
        user = UserProfile.objects.get(email=obj.user_email)
        return "{} {}".format(user.first_name, user.last_name)

    def get_is_owner(self, obj):
        return obj.user_email == self.request.user.email

    def get_is_editable(self, obj):
        user = self.request.user
        return user.is_staff or obj.user_email == user.email

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
            "id", "name", "category", "is_owner", "top_keywords_data", "num_keywords",
            "average_volume", "average_cpc", "competition",
            "average_cpv", "average_view_rate", "average_ctrv",
            "cum_average_volume_data", "cum_average_volume_per_kw_data", "is_editable",
            "owner", "created_at"
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
