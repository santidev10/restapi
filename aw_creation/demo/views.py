from aw_reporting.demo.models import DemoAccount, DEMO_ACCOUNT_ID
from aw_reporting.demo.charts import DemoChart
from aw_reporting.demo.excel_reports import DemoAnalyzeWeeklyReport
from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, LocationRule, AdScheduleRule, FrequencyCap, \
    Language, TargetingItem, AdCreation
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, HTTP_404_NOT_FOUND
from django.http import HttpResponse
from datetime import datetime
import json
DEMO_READ_ONLY = dict(errors="You are not allowed to change this entity")


class AccountCreationListApiView:

    @staticmethod
    def get(original_method):

        def method(view, request, **kwargs):
            response = original_method(view, request, **kwargs)
            if response.status_code == HTTP_200_OK:
                demo = DemoAccount()
                filters = view.get_filters()
                if demo.account_passes_filters(filters):
                    response.data['items'].insert(0, demo.header_data)
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
                    name=view.get_duplicate_name(data['name']),
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
                        elif f in ("video_ad_format", "delivery_method"):
                            camp_data[f] = c[f]["id"]
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


class PerformanceAccountCampaignsListApiView:
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


class PerformanceAccountDetailsApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                filters = view.get_filters()

                account = DemoAccount()
                data = account.header_data
                data['details'] = account.details

                account.set_period_proportion(filters['start_date'],
                                              filters['end_date'])
                account.filter_out_items(
                    filters['campaigns'], filters['ad_groups'],
                )
                data['overview'] = account.overview
                return Response(status=HTTP_200_OK, data=data)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class PerformanceChartApiView:
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


class PerformanceChartItemsApiView:
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


class PerformanceExportApiView:
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


class PerformanceExportWeeklyReport:
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


class OptimizationSettingsApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, kpi, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                demo = DemoAccount()
                data = dict(
                    id=demo.id,
                    name=demo.name,
                    campaign_creations=[
                        dict(
                            id=c.id,
                            name=c.name,
                            value=None,
                            ad_group_creations=[
                                dict(
                                    id=a.id,
                                    name=a.name,
                                    value=getattr(
                                        a,
                                        "optimization_{}_value".format(kpi)
                                    ),
                                )
                                for a in c.children
                            ]
                        )
                        for c in demo.children
                    ],
                )
                return Response(data=data)
            else:
                return original_method(view, request,
                                       pk=pk, kpi=kpi, **kwargs)

        return method

    @staticmethod
    def put(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class OptimizationFiltersApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                demo = DemoAccount()
                data = [
                    dict(
                        id=c.id,
                        name=c.name,
                        ad_group_creations=[
                            dict(
                                id=a.id,
                                name=a.name,
                            )
                            for a in c.children
                        ]
                    )
                    for c in demo.children
                ]
                return Response(data=data)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class OptimizationTargetingApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, kpi, list_type, **kwargs):
            if pk == DEMO_ACCOUNT_ID:
                acc = DemoAccount()
                value = getattr(acc, "optimization_{}_value".format(kpi))

                filters = view.get_filters()
                filters['dimension'] = list_type
                acc.filter_out_items(
                    filters.get('campaign_creation_id__in'),
                    filters.get('id__in'),
                )
                charts_obj = DemoChart(acc, filters)
                items = charts_obj.chart_items['items']
                for i in items:
                    i['bigger_than_value'] = i[kpi] and i[kpi] > value
                    i['criteria'] = i.get("id") or i['name']
                    del i['video25rate']
                    del i['video50rate']
                    del i['video75rate']
                    del i['video100rate']

                response = dict(
                    items=items,
                    value=value,
                )
                return Response(data=response)
            else:
                return original_method(view, request, pk=pk, kpi=kpi,
                                       list_type=list_type, **kwargs)

        return method


class AdGroupTargetingListApiView:
    @staticmethod
    def get(original_method):
        def method(view, request, pk, list_type, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                demo = DemoAccount()
                for c in demo.children:
                    for a in c.children:
                        if a.id == pk:
                            data = a.get_targeting_list(list_type)
                            return Response(data=data)
                return Response(status=HTTP_404_NOT_FOUND)
            else:
                return original_method(view, request, pk=pk,
                                       list_type=list_type, **kwargs)

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

    @staticmethod
    def delete(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)
        return method


class AdGroupTargetingListExportApiView:
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
                            data = a.get_targeting_list(list_type)
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


class AdGroupTargetingListImportListsApiView:
    @staticmethod
    def post(original_method):
        def method(view, request, pk, **kwargs):
            if DEMO_ACCOUNT_ID in pk:
                return Response(data=DEMO_READ_ONLY,
                                status=HTTP_403_FORBIDDEN)
            else:
                return original_method(view, request, pk=pk, **kwargs)
        return method
