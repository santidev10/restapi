import json
from datetime import datetime

from django.shortcuts import get_object_or_404
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK
from rest_framework.status import HTTP_403_FORBIDDEN
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import AdScheduleRule
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import Language
from aw_creation.models import LocationRule
from aw_creation.models import TargetingItem
from aw_reporting.demo.charts import DemoChart
from aw_reporting.demo.excel_reports import DemoAnalyzeWeeklyReport
from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.demo.models import DemoAccount
from aw_reporting.models import CONVERSIONS
from aw_reporting.models import VIEW_RATE_STATS
from userprofile.models import UserSettingsKey
from userprofile.models import get_default_settings
from utils.views import xlsx_response

DEMO_READ_ONLY = dict(error="You are not allowed to change this entity")


class AnalyticsAccountCreationListApiView:

    @staticmethod
    def get(original_method):

        def method(view, request, **kwargs):
            response = original_method(view, request, **kwargs)
            if response.status_code == HTTP_200_OK:
                user = request.user
                user_settings = user.aw_settings \
                    if hasattr(user, "aw_settings") \
                    else get_default_settings()
                demo_account_visible = user_settings.get(
                    UserSettingsKey.DEMO_ACCOUNT_VISIBLE,
                    False
                )
                demo = DemoAccount()
                filters = request.query_params
                if demo_account_visible and \
                        demo.account_passes_filters(filters):
                    response.data['items'].insert(0, demo.header_data_analytics)
                    response.data['items_count'] += 1
            return response

        return method


class DashboardAccountCreationListApiView:

    @staticmethod
    def get(original_method):

        def method(view, request, **kwargs):
            response = original_method(view, request, **kwargs)
            if response.status_code == HTTP_200_OK:
                user = request.user
                user_settings = user.aw_settings \
                    if hasattr(user, "aw_settings") \
                    else get_default_settings()
                demo_account_visible = user_settings.get(
                    UserSettingsKey.DEMO_ACCOUNT_VISIBLE,
                    False
                )
                demo = DemoAccount()
                filters = request.query_params
                if demo_account_visible and \
                        demo.account_passes_filters(filters):
                    response.data['items'].insert(0, demo.header_data_dashboard)
                    response.data['items_count'] += 1
            return response

        return method


class AccountCreationSetupApiView:
    @staticmethod
    def get(original_method):

        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                demo = DemoAccount()
                return Response(data=demo.creation_details_full)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def update(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def delete(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AdCreationDuplicateApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AdGroupCreationDuplicateApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class CampaignCreationDuplicateApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AccountCreationDuplicateApiView:
    @staticmethod
    def post(original_method):

        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                demo = DemoAccount()
                data = demo.creation_details_full

                acc_data = dict(
                    name=view.increment_name(data['name'], view.get_queryset().values_list("name", flat=True)),
                    owner=view.request.user,
                )
                for f in view.account_fields:
                    acc_data[f] = data[f]
                acc_duplicate = AccountCreation.objects.create(**acc_data)

                for c in data['campaign_creations']:
                    camp_data = dict()
                    for f in view.campaign_fields:
                        if f.endswith("_raw"):
                            camp_data[f] = json.dumps(
                                [i['id'] for i in c[f[:-4]]])
                        elif f in ("type", "delivery_method"):
                            camp_data[f] = c[f]["id"]
                        elif f == "bid_strategy_type":
                            camp_data[f] = CampaignCreation.CPV_STRATEGY
                        else:
                            camp_data[f] = c[f]
                    c_duplicate = CampaignCreation.objects.create(
                        account_creation=acc_duplicate, **camp_data
                    )
                    for l in c["languages"]:
                        lang, _ = Language.objects.get_or_create(
                            pk=l['id'], defaults=l)
                        c_duplicate.languages.add(lang)
                    for r in c["location_rules"]:
                        LocationRule.objects.create(
                            campaign_creation=c_duplicate,
                            **{f: r[f]['id'] if type(r[f]) is dict else r[f]
                               for f in view.loc_rules_fields}
                        )

                    for i in c['frequency_capping']:
                        FrequencyCap.objects.create(
                            campaign_creation=c_duplicate,
                            **{f: i[f]['id'] if type(i[f]) is dict else i[f]
                               for f in view.freq_cap_fields}
                        )
                    for i in c['ad_schedule_rules']:
                        AdScheduleRule.objects.create(
                            campaign_creation=c_duplicate,
                            **{f: i[f]['id'] if type(i[f]) is dict else i[f]
                               for f in view.ad_schedule_fields}
                        )
                    for a in c['ad_group_creations']:
                        ag_data = {}
                        for f in view.ad_group_fields:
                            if f.endswith("_raw"):
                                ag_data[f] = json.dumps(
                                    [i["id"] for i in a[f[:-4]]]
                                )
                            elif f == "video_ad_format":
                                ag_data[f] = a[f]['id']
                            else:
                                ag_data[f] = a[f]
                        a_duplicate = AdGroupCreation.objects.create(
                            campaign_creation=c_duplicate, **ag_data
                        )
                        for list_type, item_groups in a["targeting"].items():
                            for k, items in item_groups.items():
                                for i in items:
                                    TargetingItem.objects.create(
                                        ad_group_creation=a_duplicate,
                                        type=list_type,
                                        is_negative=i['is_negative'],
                                        criteria=i['criteria'],
                                    )
                        for ad in a['ad_creations']:
                            AdCreation.objects.create(
                                ad_group_creation=a_duplicate,
                                **{f: ad[f] for f in view.ad_fields}
                            )

                account_data = view.serializer_class(acc_duplicate).data
                return Response(data=account_data)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class CampaignCreationListSetupApiView:
    @staticmethod
    def get(original_method):

        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                demo = DemoAccount()
                data = [c.creation_details for c in demo.children]
                return Response(data=data)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class CampaignCreationSetupApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                demo = DemoAccount()
                for c in demo.children:
                    if c.id == pk:
                        return Response(data=c.creation_details)
                return Response(status=HTTP_404_NOT_FOUND)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def update(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AdGroupCreationSetupApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                demo = DemoAccount()
                for c in demo.children:
                    for a in c.children:
                        if a.id == pk:
                            return Response(data=a.creation_details)
                return Response(status=HTTP_404_NOT_FOUND)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def update(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AdGroupCreationListSetupApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                demo = DemoAccount()
                for c in demo.children:
                    if c.id == pk:
                        return Response(data=[a.creation_details for a in c.children])
                return Response(status=HTTP_404_NOT_FOUND)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AdCreationSetupApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                demo = DemoAccount()
                for c in demo.children:
                    for ag in c.children:
                        for a in ag.children:
                            if a.id == pk:
                                return Response(data=a.creation_details)
                return Response(status=HTTP_404_NOT_FOUND)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def update(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AdCreationAvailableAdFormatsApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=[AdGroupCreation.IN_STREAM_TYPE])
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AdCreationListSetupApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                demo = DemoAccount()
                for c in demo.children:
                    for ag in c.children:
                        if ag.id == pk:
                            return Response(data=[a.creation_details for a in ag.children])
                return Response(status=HTTP_404_NOT_FOUND)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method

    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


def show_demo_data(request, pk):
    return not request.user.aw_connections.count() or \
           get_object_or_404(AccountCreation, pk=pk).status == AccountCreation.STATUS_PENDING


class AnalyticsAccountCreationCampaignsListApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID or show_demo_data(request, pk):
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
                        campaign_creation_id=c.id,
                    )
                    for c in account.children
                ]
                return Response(status=HTTP_200_OK, data=campaigns)
            return original_method(view, request, pk=pk, **kwargs)

        return method


class DashboardAccountCreationCampaignsListApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
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
                        campaign_creation_id=c.id,
                    )
                    for c in account.children
                ]
                return Response(status=HTTP_200_OK, data=campaigns)
            return original_method(view, request, pk=pk, **kwargs)

        return method


class AnalyticsAccountCreationDetailsAPIView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:

                account = DemoAccount()
                data = account.header_data_analytics
                data['details'] = account.details

                return Response(status=HTTP_200_OK, data=data)
            return original_method(view, request, pk=pk, **kwargs)

        return method


class DashboardAccountCreationDetailsAPIView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:

                account = DemoAccount()
                data = account.header_data_dashboard
                data['details'] = account.details

                return Response(status=HTTP_200_OK, data=data)
            return original_method(view, request, pk=pk, **kwargs)

        return method


class BaseAccountCreationOverviewAPIView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                account = DemoAccount()
                filters = view.get_filters()
                account.filter_out_items(filters["campaigns"], filters["ad_groups"])
                return Response(data=account.overview)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class DashboardAccountCreationOverviewAPIView(BaseAccountCreationOverviewAPIView):
    pass


class AnalyticsAccountCreationOverviewAPIView(BaseAccountCreationOverviewAPIView):
    pass


class AnalyticsPerformanceChartApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID or show_demo_data(request, pk):
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


class DashboardPerformanceChartApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                view.filter_hidden_sections()
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


class AnalyticsPerformanceChartItemsApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, dimension, **kwargs):
            if pk == DEMO_ACCOUNT_ID or show_demo_data(request, pk):
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


class DashboardPerformanceChartItemsApiView:
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


class AnalyticsPerformanceExportApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID or show_demo_data(request, pk):
                filters = view.get_filters()
                account = DemoAccount()
                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )

                def data_generator():
                    data = account.details
                    yield {**{"tab": "Summary"}, **data}

                    for dimension in view.tabs:
                        filters['dimension'] = dimension
                        charts_obj = DemoChart(account, filters)
                        items = charts_obj.chart_items["items"]
                        for data in items:
                            yield {**{"tab": dimension}, **data}

                return view.build_response(account.name, data_generator)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class DashboardPerformanceExportApiView:
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
                    yield {**{"tab": "Summary"}, **data}

                    for dimension in view.tabs:
                        filters['dimension'] = dimension
                        charts_obj = DemoChart(account, filters)
                        items = charts_obj.chart_items["items"]
                        for data in items:
                            yield {**{"tab": dimension}, **data}

                return view.build_response(account.name, data_generator)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class AnalyticsPerformanceExportWeeklyReportApiView:
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

                title = "Channel Factory {} Weekly Report {}".format(
                    account.name,
                    datetime.now().date().strftime("%m.%d.%y")
                )
                return xlsx_response(title, report.get_content())
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class DashboardPerformanceExportWeeklyReportApiView:
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

                title = "Channel Factory {} Weekly Report {}".format(
                    account.name,
                    datetime.now().date().strftime("%m.%d.%y")
                )
                return xlsx_response(title, report.get_content())
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class PerformanceTargetingFiltersAPIView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID or show_demo_data(request, pk):
                account = DemoAccount()
                filters = view.get_static_filters()
                filters["start_date"] = account.start_date
                filters["end_date"] = account.end_date
                filters["campaigns"] = [
                    dict(
                        id=c.id,
                        name=c.name,
                        start_date=c.start_date,
                        end_date=c.end_date,
                        status=c.status,
                        ad_groups=[
                            dict(
                                id=a.id,
                                name=a.name,
                                status=a.status,
                            )
                            for a in c.children
                        ],
                    )
                    for c in account.children
                ]
                return Response(data=filters)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class PerformanceTargetingReportAPIView:

    @staticmethod
    def get_object(original_method):
        def method(view):
            pk = view.kwargs["pk"]
            if pk == DEMO_ACCOUNT_ID:
                account = DemoAccount()
                data = view.request.data
                account.set_period_proportion(data.get("start_date"),
                                              data.get("end_date"))
                account.filter_out_items(
                    data.get("campaigns"), data.get("ad_groups"),
                )
                return account
            else:
                return original_method(view)

        return method

    @staticmethod
    def get_items(original_method):

        def method(view, targeting, account):
            if account is not None and account.id == DEMO_ACCOUNT_ID:
                exclude_keys = VIEW_RATE_STATS + CONVERSIONS
                result = []
                for campaign in account.children:
                    for ad_group in campaign.children:
                        data_account = DemoAccount()
                        data = view.request.data
                        data_account.set_period_proportion(data.get("start_date"), data.get("end_date"))
                        data_account.filter_out_items([campaign.id], [ad_group.id])

                        for dimension in targeting:
                            charts_obj = DemoChart(account, dict(dimension="channel"))
                            items = charts_obj.chart_items['items']

                            for i in items:
                                i["video_impressions"] = i["impressions"]
                                for k in exclude_keys:
                                    del i[k]
                                if 'id' not in i:
                                    i['id'] = i['name']
                                i["item"] = dict(id=i['id'], name=i["name"])

                                del i['name'], i['id']
                                if "thumbnail" in i:
                                    i["item"]["thumbnail"] = i["thumbnail"]
                                    del i['thumbnail']

                                i["targeting"] = "{}s".format(dimension.capitalize())
                                i["campaign"] = dict(id=campaign.id, name=campaign.name, status=campaign.status)
                                i["ad_group"] = dict(id=ad_group.id, name=ad_group.name)
                                i["video_clicks"] = 100
                            result.extend(items)

                return result
            else:
                return original_method(view, targeting, account)

        return method


class PerformanceTargetingItemAPIView:

    @staticmethod
    def update(original_method):
        def method(view, request, **kwargs):
            ad_group_id = kwargs["ad_group_id"]
            if DEMO_ACCOUNT_ID in ad_group_id:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, **kwargs)

        return method


class AdGroupCreationTargetingExportApiView:
    @staticmethod
    def get_data(original_method):
        def method(view):
            pk = view.kwargs.get("pk")
            if DEMO_ACCOUNT_ID in pk:
                demo = DemoAccount()
                for c in demo.children:
                    for a in c.children:
                        if a.id == pk:
                            list_type = view.kwargs.get("list_type")
                            sub_list_type = view.kwargs.get("sub_list_type")
                            data = a.get_targeting_list(list_type, sub_list_type)
                            return data
                return Response(status=HTTP_404_NOT_FOUND)
            else:
                return original_method(view)

        return method


class AdGroupTargetingListImportApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method
