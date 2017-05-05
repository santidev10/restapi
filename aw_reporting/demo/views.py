from rest_framework.response import Response
from django.http import StreamingHttpResponse
from django.utils import timezone
from rest_framework.generics import ListAPIView, RetrieveAPIView, UpdateAPIView
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST, \
    HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_403_FORBIDDEN, HTTP_405_METHOD_NOT_ALLOWED
from rest_framework.views import APIView
from aw_reporting.api.serializers import *
from .models import *


class AnalyzeAccountsListApiView:
    @staticmethod
    def list(original_method):
        def method(view, request, **kwargs):
            # TODO: check if the user has
            # an active connected AdWords account,
            # if he has then return result of the original method
            if 1:
                account = DemoAccount()
                accounts = [
                    dict(
                        id=account.id,
                        name=account.name,
                        start_date=account.start_date,
                        end_date=account.end_date,
                        is_ongoing=True,
                        channels_count=1,
                        videos_count=1,
                    )
                ]
                return Response(status=HTTP_200_OK, data=accounts)
            else:
                return original_method(view, request, **kwargs)

        return method


class AnalyzeAccountCampaignsListApiView:
    @staticmethod
    def get(original_method):
        def method(*args, pk, **kwargs):

            if pk == DEMO_ACCOUNT_ID:
                account = DemoAccount()
                campaigns = [
                    dict(
                        id=c.id,
                        name=c.name,
                        start_date=c.start_date,
                        end_date=c.end_date,
                        status=c.status,
                        ad_groups=[
                            dict(id=a.id, name=a.name, status=a.status)
                            for a in c.ad_groups
                        ],
                    )
                    for c in account.campaigns
                ]
                return Response(status=HTTP_200_OK, data=campaigns)
            else:
                return original_method(*args, pk=pk, **kwargs)

        return method


class AnalyzeDetailsApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()

                account = DemoAccount()
                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])

                account.set_items_selected_proportion(
                    filters['campaigns'], filters['ad_groups'],
                )

                details = dict(
                    id=account.id,
                    name=account.name,
                    start_date=account.start_date,
                    end_date=account.end_date,
                    age=[dict(name=e, value=i+1)
                         for i, e in enumerate(reversed(AgeRanges))],
                    gender=[dict(name=e, value=i+1)
                            for i, e in enumerate(Genders)],
                    device=[dict(name=e, value=i+1)
                            for i, e in enumerate(reversed(Devices))],
                    channel=[],
                    creative=[],
                    video=[],
                    clicks=account.clicks,
                    cost=account.cost,
                    impressions=account.impressions,
                    video_views=account.video_views,
                    ctr=account.ctr,
                    ctr_v=account.ctr_v,
                    average_cpm=account.average_cpm,
                    average_cpv=account.average_cpv,
                    video_view_rate=account.video_view_rate,
                    average_position=account.average_position,
                    ad_network=account.ad_network,
                    video100rate=account.video100rate,
                    video25rate=account.video25rate,
                    video50rate=account.video50rate,
                    video75rate=account.video75rate,
                )
                return Response(status=HTTP_200_OK, data=details)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AnalyzeChartApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()
                account = DemoAccount()
                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                account.set_items_selected_proportion(
                    filters['campaigns'], filters['ad_groups'],
                )
                charts = []
                return Response(status=HTTP_200_OK, data=charts)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method
