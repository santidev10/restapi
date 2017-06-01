from aw_reporting.demo.models import DemoAccount, DEMO_ACCOUNT_ID
from aw_reporting.demo.charts import DemoChart
from rest_framework.response import Response
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN

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
                data = demo.creation_details
                data.update(
                    is_ended=False,
                    is_paused=False,
                    is_approved=True,
                    budget=sum(
                        c.budget
                        for c in demo.children
                    ),
                    campaign_creations=[
                        c.creation_details
                        for c in demo.children
                    ],
                )
                return Response(data=data)
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


