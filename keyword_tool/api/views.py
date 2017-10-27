import logging

from django.contrib.auth.models import AnonymousUser
from django.core.management import call_command
from django.db.models import Count
from django.db.models import Q
from django.db.models.functions import Coalesce
from rest_framework.authtoken.models import Token
from rest_framework.generics import ListAPIView, GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED, \
    HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.adwords_api import optimize_keyword, load_web_app_settings
from keyword_tool.tasks import update_kw_list_stats
from keyword_tool.models import Query, KeywordsList, ViralKeywords
from keyword_tool.settings import PREDEFINED_QUERIES
from keyword_tool.tasks import update_kw_list_stats
from utils.api_paginator import CustomPageNumberPaginator
from .serializers import *
from keyword_tool.api.utils import get_keywords_aw_stats, get_keywords_aw_top_bottom_stats

logger = logging.getLogger(__name__)


class InterestsApiView(ListAPIView):
    permission_classes = tuple()
    serializer_class = InterestsSerializer
    queryset = Interest.objects.all().order_by('name')


class PredefinedQueriesApiView(APIView):
    permission_classes = tuple()

    @staticmethod
    def get(*_):
        return Response(data=PREDEFINED_QUERIES)


class KWPaginator(CustomPageNumberPaginator):
    page_size = 12


class OptimizeQueryApiView(ListAPIView):
    page_size = 12
    serializer_class = KeywordSerializer
    pagination_class = KWPaginator

    def sort(self, queryset):
        """
        Apply sorts
        """
        allowed_sorts = [
            "search_volume",
            "ctr",
            "ctr_v",
            "cpv",
            "view_rate",
            "competition",
            "average_cpc"
        ]

        def get_sort_prefix():
            """
            Define ascending or descending sort
            """
            reverse = "-"
            ascending = self.request.query_params.get("ascending")
            if ascending == "1":
                reverse = ""
            return reverse

        sorting = self.request.query_params.get("sort_by")
        if sorting not in allowed_sorts:
            return queryset

        # TODO uncomment when adwords stats will be created is saas
        # extra_selects = dict(
        #     cpv="SELECT "
        #         "CASE WHEN Sum(video_views) > 0 "
        #         "THEN COALESCE(1.0 * Sum(cost) / Sum(video_views), 0) "
        #         "ELSE 0 END "
        #         "FROM aw_campaign_keywordstatistic "
        #         "WHERE aw_campaign_keywordstatistic.keyword_id "
        #         "= keyword_tool_keyword.text",
        #     ctr="SELECT "
        #         "CASE WHEN Sum(impressions) > 0 "
        #         "THEN COALESCE(1.0 * Sum(clicks) / Sum(impressions), 0) "
        #         "ELSE 0 END "
        #         "FROM aw_campaign_keywordstatistic "
        #         "WHERE aw_campaign_keywordstatistic.keyword_id "
        #         "= keyword_tool_keyword.text",
        #     ctr_v="SELECT "
        #           "CASE WHEN Sum(video_views) > 0 "
        #           "THEN COALESCE(1.0 * Sum(clicks) / Sum(video_views), 0) "
        #           "ELSE 0 END "
        #           "FROM aw_campaign_keywordstatistic "
        #           "WHERE aw_campaign_keywordstatistic.keyword_id "
        #           "= keyword_tool_keyword.text",
        #     view_rate="SELECT "
        #               "CASE WHEN Sum(impressions) > 0 "
        #               "THEN COALESCE("
        #               "1.0 * Sum(video_views) / Sum(impressions), 0) "
        #               "ELSE 0 END "
        #               "FROM aw_campaign_keywordstatistic "
        #               "WHERE aw_campaign_keywordstatistic.keyword_id "
        #               "= keyword_tool_keyword.text",
        # )
        # if sort_by in extra_selects:
        #     queryset = queryset.extra(
        #         select={
        #             sort_by: extra_selects[sort_by]
        #         }
        #     )

        if sorting == 'search_volume':
            queryset = queryset.annotate(
                search_volume_not_null=Coalesce('search_volume', 0)
            )
            sorting = 'search_volume_not_null'

        if sorting == 'average_cpc':
            queryset = queryset.annotate(
                average_cpc_not_null=Coalesce('average_cpc', 0)
            )
            sorting = 'average_cpc_not_null'

        if sorting == 'competition':
            queryset = queryset.annotate(
                competition_not_null=Coalesce('competition', 0)
            )
            sorting = 'competition_not_null'

        if sorting:
            queryset = queryset.order_by("{}{}".format(
                get_sort_prefix(), sorting))

        return queryset

    def filter(self, queryset):
        query_params = self.request.query_params

        for field in ('volume', 'competition', 'average_cpc'):
            for pref in ('min', 'max'):
                f = "{pref}_{field}".format(field=field, pref=pref)
                if f in query_params:
                    db_f = "search_%s" % field \
                        if field == 'volume' else field
                    queryset = queryset.filter(
                        **{
                            "{}__{}".format(
                                db_f, 'gte' if pref == 'min' else 'lte'
                            ): query_params[f]
                        }
                    )

        if 'interests' in query_params:
            in_ids = [iid for iid in query_params['interests'].split(',')
                      if iid]
            if in_ids:
                queryset = queryset.filter(
                    interests__in=in_ids
                ).annotate(
                    interests_count=Count('interests__id')
                ).filter(interests_count=len(in_ids))

        if 'included' in query_params:
            included = [text
                        for text in query_params['included'].split(',')
                        if text]
            for sub_str in included:
                queryset = queryset.filter(text__icontains=sub_str)

        if 'excluded' in query_params:
            excluded = [text
                        for text in query_params['excluded'].split(',')
                        if text]
            for sub_str in excluded:
                queryset = queryset.exclude(text__icontains=sub_str)
        if 'search' in query_params:
            queryset = queryset.filter(
                text__icontains=query_params['search'].strip()
            )
        return queryset

    def get_queryset(self):
        """
        Get optimized keywords
        """
        query = self.kwargs.get("query", "").strip()
        try:
            query_obj = Query.objects.get(pk=query)
        except Query.DoesNotExist:
            query_obj = Query.create_from_aw_response(
                query,
                optimize_keyword([q.strip() for q in query.split(',')
                                  if q.strip()]),
            )
        queryset = query_obj.keywords.all()
        queryset = self.filter(queryset)
        queryset = self.sort(queryset)
        return queryset

    def get(self, *args, **kwargs):
        response = super(OptimizeQueryApiView, self).get(*args, **kwargs)
        if response.status_code == 200:
            self.add_ad_words_data(self.request, response.data['items'])
        return response

    @staticmethod
    def add_ad_words_data(request, items):
        from aw_reporting.models import Account, BASE_STATS, CALCULATED_STATS, \
            dict_norm_base_stats, dict_calculate_stats

        accounts = Account.user_objects(request.user)
        cf_accounts = Account.objects.filter(managers__id=load_web_app_settings()['cf_account_id'])
        keywords = set(i['keyword_text'] for i in items)
        stats = get_keywords_aw_stats(accounts, keywords)
        top_bottom_stats = get_keywords_aw_top_bottom_stats(accounts, keywords)

        kw_without_stats = keywords - set(stats.keys())
        if kw_without_stats:  # show CF account stats
            cf_stats = get_keywords_aw_stats(cf_accounts, kw_without_stats)
            stats.update(cf_stats)
            cf_top_bottom_stats = get_keywords_aw_top_bottom_stats(cf_accounts, kw_without_stats)
            top_bottom_stats.update(cf_top_bottom_stats)

        aw_fields = BASE_STATS + tuple(CALCULATED_STATS.keys()) + ("campaigns_count",)
        for item in items:
            item_stats = stats.get(item['keyword_text'])
            if item_stats:
                dict_norm_base_stats(item_stats)
                dict_calculate_stats(item_stats)
                del item_stats['keyword']
                item.update(item_stats)

                item_top_bottom_stats = top_bottom_stats.get(item['keyword_text'])
                item.update(item_top_bottom_stats)
            else:
                item.update({f: 0 if f == "campaigns_count" else None for f in aw_fields})


class KeywordGetApiView(APIView):
    queryset = KeyWord.objects.all()
    serializer_class = KeywordSerializer

    def get(self, *args, **kwargs):
        pk = self.kwargs.get('pk')
        try:
            obj = self.queryset.get(text=pk)
        except KeywordsList.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        serializer = self.serializer_class(obj)
        result = serializer.data

        # add ad words data for received keyword
        OptimizeQueryApiView.add_ad_words_data(request=self.request,
                                               items=[result, ])
        return Response(result)


class KeywordsListApiView(OptimizeQueryApiView):
    def get_queryset(self):
        queryset = KeyWord.objects.all()
        queryset = self.filter(queryset)
        queryset = self.sort(queryset)
        return queryset


class ViralKeywordsApiView(OptimizeQueryApiView):
    def get_queryset(self):
        viral_list = ViralKeywords.objects.all().values_list('keyword', flat=True)
        queryset = KeyWord.objects.filter(text__in=viral_list)
        queryset = self.filter(queryset)
        queryset = self.sort(queryset)
        return queryset


class ListParentApiView(APIView):
    pagination_class = KWPaginator

    @property
    def paginator(self):
        if not hasattr(self, '_paginator'):
            if self.pagination_class is None:
                self._paginator = None
            else:
                self._paginator = self.pagination_class()
        return self._paginator

    def paginate_queryset(self, queryset):
        flat = self.request.query_params.get("flat")
        if self.paginator is None or flat == '1':
            return None
        return self.paginator.paginate_queryset(queryset, self.request, view=self)

    def get_paginated_response(self, data):
        assert self.paginator is not None
        return self.paginator.get_paginated_response(data)

    @property
    def visible_list_qs(self):
        email = None
        mode = self.request.query_params.get('mode')
        if isinstance(self.request.user, AnonymousUser):
            token = self.request.query_params.get("auth_token")
            if token:
                try:
                    token_obj = Token.objects.get(key=token)
                except Token.DoesNotExist:
                    pass
                else:
                    email = token_obj.user.email
        else:
            email = self.request.user.email

        if email is None:
            return KeywordsList.objects.none()

        if self.request.user.is_staff:
            queryset = KeywordsList.objects.all()
        elif mode and mode == 'targeting':
            queryset = KeywordsList.objects.filter(Q(user_email=email) |
                                                   ~Q(category="private"))
        else:
            queryset = KeywordsList.objects.filter(user_email=email)

        return queryset

    def sort_list(self, queryset):
        """
        Sorting procedure
        """
        allowed_sorts = [
            "competition",
            "average_cpc",
            "average_volume"
        ]

        def get_sort_prefix():
            """
            Define ascending or descending sort
            """
            reverse = "-"
            ascending = self.request.query_params.get("ascending")
            if ascending == "1":
                reverse = ""
            return reverse
        sort_by = self.request.query_params.get("sort_by")
        if sort_by not in allowed_sorts:
            return queryset
        if sort_by == "competition":
            queryset = queryset.annotate(
                competition_not_null=Coalesce("competition", 0)
            )
            sort_by = "competition_not_null"
        elif sort_by == "average_cpc":
            queryset = queryset.annotate(
                average_cpc_not_null=Coalesce("average_cpc", 0)
            )
            sort_by = "average_cpc_not_null"
        elif sort_by == "average_volume":
            queryset = queryset.annotate(
                average_volume_not_null=Coalesce("average_volume", 0)
            )
            sort_by = "average_volume_not_null"
        if sort_by:
            queryset = queryset.order_by("{}{}".format(
                get_sort_prefix(), sort_by))
        return queryset

    def filter_list(self, queryset):
        filters = {}
        # category
        category = self.request.query_params.get("category")
        search = self.request.query_params.get("search")
        if search:
            filters['name__icontains'] = search
        if category:
            filters["category"] = category
        return queryset.filter(**filters)


class SavedListsGetOrCreateApiView(ListParentApiView):
    def get(self, request, *args, **kwargs):
        queryset = self.visible_list_qs
        queryset = self.sort_list(queryset)
        queryset = self.filter_list(queryset)
        fields = self.request.query_params.get("fields")
        page = self.paginate_queryset(queryset)

        if fields:
            kwargs["fields"] = set(fields.split(","))

        if page is not None:
            serializer = SavedListNameSerializer(page, many=True, request=request, **kwargs)
            return self.get_paginated_response(serializer.data)

        serializer = SavedListNameSerializer(queryset, many=True, request=request, **kwargs)
        return Response(serializer.data)

    def post(self, request, *args, **kwargs):
        keywords = self.request.data.get('keywords')
        if not keywords:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data="'keywords' is a required param")

        request.data['user_email'] = self.request.user.email
        serializer = SavedListCreateSerializer(data=request.data, request=request)
        serializer.is_valid(raise_exception=True)
        new_list = serializer.save()

        # pylint: disable=no-member
        keywords_relation = KeywordsList.keywords.through
        # pylint: enable=no-member
        kw_relations = [keywords_relation(keyword_id=kw_id,
                                          keywordslist_id=new_list.id)
                        for kw_id in keywords]
        keywords_relation.objects.bulk_create(kw_relations)

        update_kw_list_stats.delay(new_list, KeyWord)
        serializer = SavedListNameSerializer(new_list, request=request)
        return Response(status=HTTP_202_ACCEPTED, data=serializer.data)


class SavedListApiView(ListParentApiView):
    def get(self, request, *args, **kwargs):
        pk = self.kwargs.get('pk')
        try:
            obj = KeywordsList.objects.get(pk=pk)
        except KeywordsList.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        return Response(data=SavedListNameSerializer(obj, request=request).data)

    def put(self, request, *args, **kwargs):
        pk = self.kwargs.get('pk')
        email = self.request.user.email
        try:
            obj = KeywordsList.objects.get(
                pk=pk,
                user_email=email,
            )
        except KeywordsList.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        else:
            serializer = SavedListUpdateSerializer(
                instance=obj,
                data=request.data,
                partial=True,
                request=request
            )
            if not serializer.is_valid():
                return Response(data=serializer.errors,
                                status=HTTP_400_BAD_REQUEST)
            obj = serializer.save()

            return Response(data=SavedListNameSerializer(
                obj, request=request).data,
                            status=HTTP_202_ACCEPTED)

    def delete(self, *args, **kwargs):
        pk = self.kwargs.get('pk')
        try:
            obj = KeywordsList.objects.get(
                pk=pk,
            )
        except KeywordsList.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        else:
            res = obj.delete()
            return Response(data=res[0],
                            status=HTTP_202_ACCEPTED)


class SavedListKeywordsApiView(OptimizeQueryApiView, ListParentApiView):
    def get_queryset(self):
        pk = self.kwargs.get('pk')
        queryset = KeyWord.objects.filter(lists__pk=pk)
        queryset = self.filter(queryset)
        queryset = self.sort(queryset)
        return queryset

    def get(self, *args, **kwargs):
        pk = self.kwargs.get('pk')
        queryset = self.visible_list_qs
        try:
            queryset.get(pk=pk)
        except KeywordsList.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)
        return super(SavedListKeywordsApiView, self).get(*args, **kwargs)

    def post(self, *args, **kwargs):
        pk = self.kwargs.get('pk')
        email = self.request.user.email
        try:
            obj = KeywordsList.objects.get(
                pk=pk,
                user_email=email,
            )
        except KeywordsList.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        # get keywords queryset
        ids = dict(self.request.data).get('keywords')
        if not ids:
            return Response(status=HTTP_400_BAD_REQUEST)

        queryset = KeyWord.objects.filter(pk__in=ids) \
            .exclude(lists__id=obj.id)
        ids_to_save = set(queryset.values_list('text', flat=True))

        if ids_to_save:
            # pylint: disable=no-member
            keywords_relation = KeywordsList.keywords.through
            # pylint: enable=no-member
            kw_relations = [keywords_relation(keyword_id=kw_id,
                                              keywordslist_id=obj.id)
                            for kw_id in ids_to_save]
            keywords_relation.objects.bulk_create(kw_relations)

        update_kw_list_stats.delay(obj, KeyWord)
        return Response(status=HTTP_202_ACCEPTED,
                        data=dict(count=len(ids_to_save)))

    def delete(self, *args, **kwargs):
        pk = self.kwargs.get('pk')
        email = self.request.user.email
        try:
            obj = KeywordsList.objects.get(
                pk=pk,
                user_email=email,
            )
        except KeywordsList.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        # get keywords queryset
        ids = dict(self.request.data).get('keywords')
        if not ids:
            return Response(status=HTTP_400_BAD_REQUEST)

        queryset = self.sort(self.filter(obj.keywords.all()))
        queryset = queryset.filter(pk__in=ids)
        ids_to_save = set(queryset.values_list('text', flat=True))
        count = 0
        if ids_to_save:
            # pylint: disable=no-member
            keywords_relation = KeywordsList.keywords.through
            # pylint: enable=no-member
            count, details = keywords_relation.objects.filter(
                keywordslist_id=obj.id,
                keyword_id__in=ids_to_save,
            ).delete()
        update_kw_list_stats.delay(obj, KeyWord)
        return Response(status=HTTP_202_ACCEPTED, data=dict(count=count))


class ListsDuplicateApiView(GenericAPIView):
    def get_queryset(self):
        if self.request.user.is_staff:
            queryset = KeywordsList.objects.all()
        else:
            queryset = KeywordsList.objects.filter(
                Q(user_email=self.request.user.email) |
                ~Q(category="private"))
        return queryset

    def post(self, request, *args, **kwargs):
        kw_list = self.get_object()
        # pylint: disable=no-member
        keywords = kw_list.keywords.through.objects.filter(
            keywordslist_id=kw_list.id).values_list('keyword_id', flat=True)
        new_list = KeywordsList.objects.create(
            user_email=self.request.user.email,
            name='{} (copy)'.format(kw_list.name),
            category="private"
        )
        keywords_relation = KeywordsList.keywords.through
        # pylint: enable=no-member
        kw_relations = [keywords_relation(keyword_id=kw_id,
                                          keywordslist_id=new_list.id)
                        for kw_id in keywords]
        keywords_relation.objects.bulk_create(kw_relations)

        update_kw_list_stats.delay(new_list, KeyWord)

        return Response(status=HTTP_202_ACCEPTED,
                        data=SavedListNameSerializer(
                            new_list, request=request).data)


class ViralListBuildView(APIView):
    def post(self, *args, **kwargs):
        uc = dict(self.request.data).get('update_complete')
        if uc:
            call_command('generate_viral_keywords_list')
            return Response(status=HTTP_202_ACCEPTED)
        return Response(status=HTTP_400_BAD_REQUEST)
