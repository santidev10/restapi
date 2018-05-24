import itertools

from django.db.models import QuerySet
from rest_framework.serializers import ModelSerializer, ValidationError
from rest_framework.serializers import SerializerMethodField

from aw_reporting.adwords_api import load_web_app_settings
from aw_reporting.models import Account, dict_calculate_stats, \
    dict_norm_base_stats
from keyword_tool.api.utils import get_keywords_aw_stats
from keyword_tool.models import KeyWord, Interest, KeywordsList, \
    AVAILABLE_KEYWORD_LIST_CATEGORIES
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
            "average_cpc", "competition", "interests", "updated_at",
            "keyword_text", "monthly_searches", "search_volume", "interests_top_kw",
            "category", "thirty_days_views", "weekly_views", "daily_views", "views",
        )


class SavedListCreateSerializer(ModelSerializer):

    def __init__(self, *args, **kwargs):
        self.request = kwargs.pop('request')
        super(SavedListCreateSerializer, self).__init__(*args, **kwargs)

    def validate_category(self, kw_list_category):
        """
        Check keyword list category
        """
        user = self.request.user
        if kw_list_category is not None:
            if kw_list_category != "private" and not user.is_staff:
                raise ValidationError(
                    "Not valid category. Options are: private")
            elif kw_list_category not in AVAILABLE_KEYWORD_LIST_CATEGORIES:
                raise ValidationError(
                    "Not valid category. Options are: {}".format(
                        ", ".join(AVAILABLE_KEYWORD_LIST_CATEGORIES)))
        return kw_list_category

    class Meta:
        model = KeywordsList
        fields = (
            "user_email", "name", "category",
        )


class SavedListNameSerializer(SavedListCreateSerializer):
    owner = SerializerMethodField()
    is_owner = SerializerMethodField()
    is_editable = SerializerMethodField()
    average_cpv = SerializerMethodField()
    video_view_rate = SerializerMethodField()
    ctr_v = SerializerMethodField()

    def get_average_cpv(self, obj):
        return self.aw_stats.get(obj.id, {}).get("average_cpv")

    def get_video_view_rate(self, obj):
        return self.aw_stats.get(obj.id, {}).get("video_view_rate")

    def get_ctr_v(self, obj):
        return self.aw_stats.get(obj.id, {}).get("ctr_v")

    def __init__(self, *args, **kwargs):
        fields = kwargs.pop('fields', None)
        super(SavedListNameSerializer, self).__init__(*args, **kwargs)

        if fields is not None:
            requested_fields = set(fields)
            pre_defined_fields = set(self.fields.keys())
            difference = pre_defined_fields - requested_fields
            for field_name in difference:
                self.fields.pop(field_name)

        self.aw_stats = {}
        if args:
            list_ids = []
            objects = args[0]
            if isinstance(objects, KeywordsList):
                list_ids = (objects.id,)
            elif isinstance(objects, list) or isinstance(objects, QuerySet):
                list_ids = [i.id for i in objects]

            if list_ids:
                keywords_rows = KeyWord.objects.filter(
                    lists__id__in=list_ids).values("text", "lists__id")
                all_keywords = set(e["text"] for e in keywords_rows)
                accounts = set(Account.user_objects(self.request.user).values_list("id", flat=True))

                fields = ("sum_clicks", "sum_video_views", "sum_cost", "video_impressions", "video_clicks")
                stats = get_keywords_aw_stats(accounts, all_keywords, fields)
                kw_without_stats = all_keywords - set(stats.keys())
                if kw_without_stats:  # add CF account stats for keywords without stats
                    cf_accounts = Account.objects.filter(managers__id=load_web_app_settings()['cf_account_id'])
                    cf_stats = get_keywords_aw_stats(cf_accounts, kw_without_stats, fields)
                    stats.update(cf_stats)

                for list_id in list_ids:
                    list_stats = dict(zip(fields, itertools.repeat(0)))
                    for kw in filter(lambda r: r["lists__id"] == list_id, keywords_rows):
                        if kw["text"] in stats:
                            kw_stats = stats[kw["text"]]
                            for f in fields:
                                if not kw_stats.get(f):
                                    continue
                                list_stats[f] += kw_stats[f]
                    dict_norm_base_stats(list_stats)
                    dict_calculate_stats(list_stats)
                    self.aw_stats[list_id] = list_stats

    @staticmethod
    def get_owner(obj):
        try:
            user = UserProfile.objects.get(email=obj.user_email)
            return "{} {}".format(user.first_name, user.last_name)
        except UserProfile.DoesNotExist:
            return "Owner not found or deleted"

    def get_is_owner(self, obj):
        return obj.user_email == self.request.user.email

    def get_is_editable(self, obj):
        user = self.request.user
        return user.is_staff or obj.user_email == user.email

    class Meta:
        model = KeywordsList
        fields = (
            "id", "name", "category", "is_owner", "top_keywords_data", "num_keywords",
            "average_volume", "average_cpc", "competition",
            "average_cpv", "video_view_rate", "ctr_v",
            # minidash has been disabled: SAAS-1172 --->
            # "cum_average_volume_data", "cum_average_volume_per_kw_data",
            # <---
            "is_editable", "owner", "created_at"
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
