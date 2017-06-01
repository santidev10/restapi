from aw_reporting.demo.models import DemoAccount, DEMO_ACCOUNT_ID
from aw_reporting.demo.charts import DemoChart
from aw_creation.models import AccountCreation, CampaignCreation, \
    AdGroupCreation, LocationRule, AdScheduleRule, FrequencyCap, \
    Language, TargetingItem
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN
import json
DEMO_READ_ONLY = "You are not allowed to change this entity"


class OptimizationAccountListApiView:

    @staticmethod
    def get(original_method):

        def method(view, request, **kwargs):
            response = original_method(view, request, **kwargs)
            if response.status_code == HTTP_200_OK:
                demo = DemoAccount()
                response.data['items'].insert(0, demo.creation_details)
                response.data['items_count'] += 1
            return response
        return method


class OptimizationAccountApiView:
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


class OptimizationAccountDuplicateApiView:
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
                    if f == "video_networks_raw":
                        acc_data[f] = json.dumps(
                            [i['id'] for i in data['video_networks']])
                    elif f in ("video_ad_format", "delivery_method",
                               "bidding_type", "type", "goal_type"):
                        acc_data[f] = data[f]["id"]
                    else:
                        acc_data[f] = data[f]
                acc_duplicate = AccountCreation.objects.create(**acc_data)

                for c in data['campaign_creations']:
                    camp_data = dict()
                    for f in view.campaign_fields:
                        if f == "devices_raw":
                            camp_data[f] = json.dumps(
                                [i['id'] for i in c['devices']])
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
                        for list_type, items in a["targeting"].items():
                            for i in items:
                                TargetingItem.objects.create(
                                    ad_group_creation=a_duplicate,
                                    type=list_type,
                                    is_negative=i['is_negative'],
                                    criteria=i['criteria'],
                                )

                account_data = view.serializer_class(acc_duplicate).data
                return Response(data=account_data)
            else:
                return original_method(view, request, pk=pk, **kwargs)

        return method


class OptimizationCampaignListApiView:
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


