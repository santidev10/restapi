import logging

from django.contrib.auth.models import AnonymousUser
from django.db.models import Count
from django.db.models import Q
from django.db.models.functions import Coalesce
from rest_framework.authtoken.models import Token
from rest_framework.generics import ListAPIView, GenericAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_202_ACCEPTED, \
    HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_reporting.adwords_api import optimize_keyword
from keyword_tool.models import Query, KeywordsList, ViralKeywords
from keyword_tool.settings import PREDEFINED_QUERIES
from utils.api_paginator import CustomPageNumberPaginator
from .serializers import *

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
    permission_classes = tuple()

    def sort(self, queryset):
        query_params = self.request.query_params

        if 'sort_by' in query_params and query_params['sort_by'] in (
                'search_volume', 'ctr', 'ctr_v', 'cpv', 'view_rate',
                'competition', 'average_cpc'):
            sort_by = query_params['sort_by']
        else:
            sort_by = 'search_volume'

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

        if sort_by == 'search_volume':
            queryset = queryset.annotate(
                search_volume_not_null=Coalesce('search_volume', 0)
            )
            sort_by = 'search_volume_not_null'

        if sort_by == 'average_cpc':
            queryset = queryset.annotate(
                average_cpc_not_null=Coalesce('average_cpc', 0)
            )
            sort_by = 'average_cpc_not_null'

        if sort_by == 'competition':
            queryset = queryset.annotate(
                competition_not_null=Coalesce('competition', 0)
            )
            sort_by = 'competition_not_null'

        if sort_by:
            queryset = queryset.order_by("-{}".format(sort_by))

        return queryset

    def filter(self, queryset):
        query_params = self.request.query_params

        for field in ('volume', 'competition'):
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
        # if response.status_code == 200:
        #     self.add_ad_words_data(response.data['items'])
        return response

        # TODO uncomment when adwords stats will be created is saas
        # @staticmethod
        # def add_ad_words_data(items):
        #     from aw_reporting.models import KeywordStatistic
        #     stats = KeywordStatistic.objects.filter(
        #         keyword_id__in=set(i['keyword_text'] for i in items)
        #     ).values('keyword_id').order_by('keyword_id').annotate(
        #         campaigns_count=Count('ad_group__campaign_id', distinct=True),
        #         **{n: Sum(n) for n in SUM_STATS + QUARTILE_STATS}
        #     )
        #     stats = {s['keyword_id']: s for s in stats}
        #
        #     for item in items:
        #         item_stats = stats.get(item['keyword_text'], {})
        #         item['campaigns_count'] = item_stats.get('campaigns_count', 0)
        #         for s in SUM_STATS + QUARTILE_STATS:
        #             item[s] = item_stats.get(s)
        #         dict_quartiles_to_rates(item)
        #         dict_add_calculated_stats(item)


class ViralKeywordsApiView(OptimizeQueryApiView):
    def get_queryset(self):
        viral_list = ViralKeywords.objects.all().values_list('keyword', flat=True)
        queryset = KeyWord.objects.filter(text__in=viral_list)
        queryset = self.filter(queryset)
        queryset = self.sort(queryset)
        return queryset


class ListParentApiView(APIView):
    permission_classes = tuple()

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
        sort_by = self.request.query_params.get('sort_by') or 'average_volume'

        if sort_by == 'competition':
            queryset = queryset.annotate(
                competition_not_null=Coalesce('competition', 0)
            )
            sort_by = 'competition_not_null'
        elif sort_by == 'average_cpc':
            queryset = queryset.annotate(
                average_cpc_not_null=Coalesce('average_cpc', 0)
            )
            sort_by = 'average_cpc_not_null'
        elif sort_by == 'average_volume':
            queryset = queryset.annotate(
                average_volume_not_null=Coalesce('average_volume', 0)
            )
            sort_by = 'average_volume_not_null'
        if sort_by:
            queryset = queryset.order_by("-{}".format(sort_by))
        return queryset

    def filter_list(self, queryset):
        filters = {}
        # category
        category = self.request.query_params.get("category")
        if category:
            filters["category"] = category
        return queryset.filter(**filters)

class SavedListsGetOrCreateApiView(ListParentApiView):
    def get(self, request, *args, **kwargs):
        queryset = self.visible_list_qs
        queryset = self.sort_list(queryset)
        queryset = self.filter_list(queryset)
        serializer = SavedListNameSerializer(queryset, many=True, request=request)
        return Response(serializer.data)

    def post(self, request, *args, **kwargs):
        name = self.request.data.get('name')
        keywords = self.request.data.get('keywords')
        category = self.request.data.get('category')

        if name and keywords:
            # create list
            new_list = KeywordsList.objects.create(
                user_email=self.request.user.email,
                name=name,
                category=category
            )
            # create relations
            keywords_relation = KeywordsList.keywords.through
            kw_relations = [keywords_relation(keyword_id=kw_id,
                                              keywordslist_id=new_list.id)
                            for kw_id in keywords]
            keywords_relation.objects.bulk_create(kw_relations)

            new_list.update_kw_list_stats.delay(new_list)
            serializer = SavedListNameSerializer(instance=new_list,
                                                 data=self.request.data,
                                                 request=request)
            serializer.is_valid(raise_exception=True)
            return Response(status=HTTP_202_ACCEPTED,
                            data=serializer.data)

        return Response(status=HTTP_400_BAD_REQUEST,
                        data="'name' and 'keywords' are required params")


class SavedListApiView(ListParentApiView):
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
        email = self.request.user.email
        try:
            obj = KeywordsList.objects.get(
                pk=pk,
                user_email=email,
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
            keywords_relation = KeywordsList.keywords.through
            kw_relations = [keywords_relation(keyword_id=kw_id,
                                              keywordslist_id=obj.id)
                            for kw_id in ids_to_save]
            keywords_relation.objects.bulk_create(kw_relations)

        obj.update_kw_list_stats.delay(obj)
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
            keywords_relation = KeywordsList.keywords.through
            count, details = keywords_relation.objects.filter(
                keywordslist_id=obj.id,
                keyword_id__in=ids_to_save,
            ).delete()
        obj.update_kw_list_stats.delay(obj)
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
        keywords = kw_list.keywords.through.objects.filter(
            keywordslist_id=kw_list.id).values_list('keyword_id', flat=True)
        new_list = KeywordsList.objects.create(
            user_email=self.request.user.email,
            name='{} (copy)'.format(kw_list.name),
            category="private"
        )
        keywords_relation = KeywordsList.keywords.through
        kw_relations = [keywords_relation(keyword_id=kw_id,
                                          keywordslist_id=new_list.id)
                        for kw_id in keywords]
        keywords_relation.objects.bulk_create(kw_relations)

        new_list.update_kw_list_stats.delay(new_list)

        return Response(status=HTTP_202_ACCEPTED,
                        data=SavedListNameSerializer(
                            new_list, request=request).data)
