from datetime import datetime

from django.conf import settings
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK

from aw_reporting.demo.charts import DemoChart
from aw_reporting.demo.excel_reports import DemoAnalyticsWeeklyReport
from aw_reporting.demo.models import DemoAccount, DEMO_ACCOUNT_ID
from utils.views import xlsx_response


class AnalyzeAccountsListApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, **kwargs):
            response = original_method(view, request, **kwargs)
            if response.status_code == HTTP_200_OK:
                demo = DemoAccount()
                filters = view.get_filters()
                if demo.account_passes_filters(filters):
                    response.data['items'].insert(0, demo.account_details)
                    response.data['items_count'] += 1
            return response

        return method


class AnalyzeDetailsApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()

                account = DemoAccount()
                data = account.account_details
                data["details"] = account.details

                account.set_period_proportion(filters["start_date"],
                                              filters["end_date"])
                account.filter_out_items(
                    filters["campaigns"], filters["ad_groups"],
                )
                data["overview"] = account.overview
                for key in ("plan_video_views", "delivered_video_views",
                            "plan_cost", "delivered_cost",
                            "delivered_impressions", "plan_impressions"):
                    del data["overview"][key]
                return Response(status=HTTP_200_OK, data=data)
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


class AnalyzeExportWeeklyReportApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()
                account = DemoAccount()
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )
                report = DemoAnalyticsWeeklyReport(account)
                hide_brand_name = settings.CUSTOM_AUTH_FLAGS \
                    .get(request.user.email.lower(), {}) \
                    .get("hide_brand_name", False)
                report.hide_logo = hide_brand_name
                brand_name = "" if hide_brand_name else "Channel Factory"
                title = " ".join([f for f in [
                    brand_name,
                    account.name,
                    "Weekly Report",
                    datetime.now().date().strftime("%m.%d.%y")
                ] if f])
                return xlsx_response(title, report.get_content())
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


def get_demo_account_data():
    account = DemoAccount()
    return dict(
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


class TrackFiltersListApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, **kwargs):
            if request.user.aw_connections.count():
                return original_method(view, request, **kwargs)
            else:
                data = dict(
                    accounts=[get_demo_account_data()],
                    **view._get_static_filters()
                )
            return Response(data=data)

        return method


class GlobalTrendsFiltersApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, **kwargs):
            response = original_method(view, request, **kwargs)
            if response.status_code == HTTP_200_OK \
                    and len(response.data.get("accounts", [])) > 0:
                return response
            data = dict(
                accounts=[get_demo_account_data()],
                am=[],
                ad_ops=[],
                sales=[],
                brands=[],
                categories=[],
                **view._get_static_filters()
            )
            return Response(data=data)

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
