from datetime import datetime

from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.demo.excel_reports import DemoAnalyzeWeeklyReport
from aw_creation.demo.views import OptimizationAccountListApiView
from .charts import DemoChart
from .models import DemoAccount, DEMO_ACCOUNT_ID


class AnalyzeAccountsListApiView(OptimizationAccountListApiView):
    pass


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
                filters['segmented'] = True
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


class TrackFiltersListApiView:

    @staticmethod
    def get(original_method):
        def method(view, *args, **kwargs):

            response = original_method(view, *args, **kwargs)
            data = response.data
            if not data['accounts']:
                account = DemoAccount()
                data['accounts'] = [
                    dict(
                        id=account.id,
                        name=account.name,
                        start_date=account.start_date,
                        end_date=account.end_date,
                        campaigns=[
                            dict(
                                id=c.id,
                                name=c.name,
                                start_date=c.start_date,
                                end_date=c.end_date,
                            )
                            for c in account.children
                        ]
                    )
                ]
            return response
        return method


class TrackChartApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, **kwargs):
            if request.user.aw_connections.count():
                return original_method(view, request, **kwargs)
            else:
                filters = view.get_filters()
                account = DemoAccount()
                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                campaigns = [filters['campaign']] \
                    if filters['campaign'] else []
                account.filter_out_items(campaigns, None)
                charts_obj = DemoChart(account, filters)
                return Response(status=HTTP_200_OK,
                                data=charts_obj.charts)
        return method


class TrackAccountsDataApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, **kwargs):
            if request.user.aw_connections.count():
                return original_method(view, request, **kwargs)
            else:
                filters = view.get_filters()
                account = DemoAccount()
                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                campaigns = [filters['campaign']] \
                    if filters['campaign'] else []
                account.filter_out_items(campaigns, None)
                charts_obj = DemoChart(account, filters)

                del filters['dimension']
                data = charts_obj.chart_lines(account, filters)
                trend = data[0]['trend']
                latest_1d_data = trend[-1:]
                latest_5d_data = trend[-5:]

                accounts = [
                    dict(
                        id=account.id,
                        label=account.name,
                        average_1d=sum(i['value'] for i in latest_1d_data)
                        / len(latest_1d_data) if len(latest_1d_data)
                        else None,
                        average_5d=sum(i['value'] for i in latest_5d_data)
                        / len(latest_5d_data) if len(latest_5d_data)
                        else None,
                        trend=trend,
                    )
                ]
                return Response(status=HTTP_200_OK, data=accounts)
        return method
