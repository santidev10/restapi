import logging

from django.contrib.auth.models import AnonymousUser
from django.core.management import call_command
from django.db.models import Count
from django.db.models import Q
from django.db.models.functions import Coalesce
from rest_framework.authtoken.models import Token
from rest_framework.generics import ListAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_202_ACCEPTED
from rest_framework.status import HTTP_400_BAD_REQUEST
from rest_framework.views import APIView

from aw_reporting.adwords_api import optimize_keyword
from keyword_tool.api.utils import get_keywords_aw_top_bottom_stats
from keyword_tool.models import Query
from keyword_tool.models import ViralKeywords
from utils.api_paginator import CustomPageNumberPaginator
from .serializers import *

logger = logging.getLogger(__name__)


class KWPaginator(CustomPageNumberPaginator):
    page_size = 20


class BaseOptimizeQueryApiView(ListAPIView):
    page_size = 20
    serializer_class = KeywordSerializer
    pagination_class = KWPaginator

    def sort(self, queryset):
        """
        Apply sorts
        """
        allowed_sorts = [
            "search_volume",
            "daily_views",
            "weekly_views",
            "daily_views",
            "thirty_days_views",
            "ctr",
            "ctr_v",
            "cpv",
            "view_rate",
            "competition",
            "average_cpc"
        ]
        if self.request.method == "POST":
            query_params = self.request.data
        else:
            query_params = self.request.query_params

        def get_sort_prefix():
            """
            Define ascending or descending sort
            """
            reverse = "-"
            ascending = query_params.get("ascending")
            if ascending == "1":
                reverse = ""
            return reverse

        sorting = query_params.get("sort_by")
        if sorting not in allowed_sorts:
            return queryset

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
        if self.request.method == "POST":
            query_params = self.request.data
        else:
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
        if "ids" in query_params:
            queryset = queryset.filter(
                text__in=query_params.get("ids").split(",")
            )
        if 'category' in query_params:
            queryset = queryset.filter(
                category=query_params['category']
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
        response = super(BaseOptimizeQueryApiView, self).get(*args, **kwargs)
        if response.status_code == 200:
            flat = self.request.query_params.get("flat")
            fields = self.request.query_params.get("fields")
            if flat == "1" and fields is not None:
                return response
            elif flat == "1":
                self.add_ad_words_data(self.request, response.data)
                return response
            else:
                self.add_ad_words_data(
                    self.request, response.data.get("items"))
        return response

    def paginate_queryset(self, queryset):
        """
        Processing flat query param
        """
        flat = self.request.query_params.get("flat")
        if flat == "1":
            return None
        return super().paginate_queryset(queryset)

    @staticmethod
    def add_ad_words_data(request, items):
        from aw_reporting.models import Account, BASE_STATS, CALCULATED_STATS, \
            dict_norm_base_stats, dict_add_calculated_stats

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
                dict_add_calculated_stats(item_stats)
                del item_stats['keyword']
                item.update(item_stats)

                item_top_bottom_stats = top_bottom_stats.get(item['keyword_text'])
                item.update(item_top_bottom_stats)
            else:
                item.update({f: 0 if f == "campaigns_count" else None for f in aw_fields})


class ViralKeywordsApiView(BaseOptimizeQueryApiView):
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


class ViralListBuildView(APIView):
    def post(self, *args, **kwargs):
        uc = dict(self.request.data).get('update_complete')
        if uc:
            call_command('generate_viral_keywords_list')
            return Response(status=HTTP_202_ACCEPTED)
        return Response(status=HTTP_400_BAD_REQUEST)
