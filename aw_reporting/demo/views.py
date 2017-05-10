from rest_framework.response import Response
from django.http import StreamingHttpResponse, HttpResponse
from django.utils import timezone
from rest_framework.generics import ListAPIView, RetrieveAPIView, UpdateAPIView
from rest_framework.status import HTTP_404_NOT_FOUND, HTTP_400_BAD_REQUEST, \
    HTTP_200_OK, HTTP_202_ACCEPTED, HTTP_403_FORBIDDEN, HTTP_405_METHOD_NOT_ALLOWED
from rest_framework.views import APIView
from aw_reporting.api.serializers import *
from aw_reporting.demo.excel_reports import DemoAnalyzeWeeklyReport
from .models import *
from .charts import DemoChart


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
                            for a in c.children
                        ],
                    )
                    for c in account.children
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
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )
                return Response(status=HTTP_200_OK, data=account.details)
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
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )
                charts_obj = DemoChart(account, filters)
                return Response(status=HTTP_200_OK, data=charts_obj.charts)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AnalyzeChartItemsApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, dimension, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()
                account = DemoAccount()
                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )
                filters['dimension'] = dimension
                charts_obj = DemoChart(account, filters)
                return Response(status=HTTP_200_OK,
                                data=charts_obj.chart_items)
            else:
                return original_method(view, request, pk=pk,
                                       dimension=dimension, **kwargs)

        return method


class AnalyzeExportApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()
                account = DemoAccount()
                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )

                def data_generator():
                    data = account.details
                    yield view.column_names
                    yield ['Summary'] + [data.get(n)
                                         for n in view.column_keys]
                    for dimension in view.tabs:
                        filters['dimension'] = dimension
                        charts_obj = DemoChart(account, filters)
                        items = charts_obj.chart_items
                        for data in items['items']:
                            yield [dimension.capitalize()] + \
                                  [data[n] for n in view.column_keys]

                return view.stream_response(account.name, data_generator)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AnalyzeExportWeeklyReport:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()
                account = DemoAccount()
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )
                report = DemoAnalyzeWeeklyReport(account)

                response = HttpResponse(
                    report.get_content(),
                    content_type='application/vnd.openxmlformats-'
                                 'officedocument.spreadsheetml.sheet'
                )
                response[
                    'Content-Disposition'
                ] = 'attachment; filename="Channel Factory {} Weekly ' \
                    'Report {}.xlsx"'.format(
                        account.name,
                        datetime.now().date().strftime("%m.%d.%y")
                )
                return response
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method
