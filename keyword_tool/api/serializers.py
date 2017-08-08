from rest_framework.serializers import ModelSerializer, ValidationError
from rest_framework.serializers import SerializerMethodField
from aw_reporting.models import Account, dict_calculate_stats
from keyword_tool.models import KeyWord, Interest, KeywordsList, AVAILABLE_KEYWORD_LIST_CATEGORIES
from userprofile.models import UserProfile
from django.db.models import QuerySet


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
            "keyword_text", "monthly_searches", "search_volume", "interests_top_kw"
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
                accounts = set(Account.user_objects(self.request.user).values_list("id", flat=True))
                data = KeywordsList.keywords.through.objects.raw(
                    """
                    SELECT keywordslist_id AS id,
                    SUM(CASE WHEN "aw_reporting_keywordstatistic"."video_views" > 0 
                             THEN "aw_reporting_keywordstatistic"."impressions" 
                             ELSE NULL END) AS "video_impressions",
                    SUM("aw_reporting_keywordstatistic"."video_views") AS "sum_video_views", 
                    SUM("aw_reporting_keywordstatistic"."cost") AS "sum_cost", 
                    SUM("aw_reporting_keywordstatistic"."clicks") AS "sum_clicks",
                    SUM("aw_reporting_keywordstatistic"."impressions") AS "sum_impressions"
                    
                    FROM keyword_tool_keywordslist_keywords 
                    INNER JOIN "aw_reporting_keywordstatistic" 
                        ON "aw_reporting_keywordstatistic"."keyword" = keyword_tool_keywordslist_keywords.keyword_id 
                    INNER JOIN "aw_reporting_adgroup" 
                        ON ("aw_reporting_keywordstatistic"."ad_group_id" = "aw_reporting_adgroup"."id")
                    INNER JOIN "aw_reporting_campaign" 
                        ON ("aw_reporting_adgroup"."campaign_id" = "aw_reporting_campaign"."id")
                    
                    WHERE "keyword_tool_keywordslist_keywords"."keywordslist_id" IN ({}) 
                    AND "aw_reporting_campaign"."account_id" IN ({})
                    
                    GROUP BY "keyword_tool_keywordslist_keywords"."keywordslist_id" 
                    ORDER BY "keyword_tool_keywordslist_keywords"."keywordslist_id" ASC
                    
                    """.format(
                        ",".join(str(i) for i in list_ids),
                        "'{}'".format("', '".join(accounts)) if accounts else "NULL",
                    )
                )
                for i in data:
                    stat = dict(
                        video_views=i.sum_video_views,
                        cost=i.sum_cost,
                        clicks=i.sum_clicks,
                        video_impressions=i.video_impressions,
                        impressions=i.sum_impressions,
                    )
                    dict_calculate_stats(stat)
                    self.aw_stats[i.id] = stat

    def get_owner(self, obj):
        user = UserProfile.objects.get(email=obj.user_email)
        return "{} {}".format(user.first_name, user.last_name)

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
