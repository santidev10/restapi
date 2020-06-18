from decimal import Decimal

from django.db.models import Q
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.generics import RetrieveUpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.api.serializers import CampaignCreationSetupSerializer
from aw_creation.api.serializers import CampaignCreationUpdateSerializer
from aw_creation.api.serializers import FrequencyCapUpdateSerializer
from aw_creation.api.serializers import OptimizationLocationRuleUpdateSerializer
from aw_creation.models import AdScheduleRule
from aw_creation.models import CampaignCreation
from aw_creation.models import FrequencyCap
from aw_creation.models import LocationRule
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from utils.permissions import MediaBuyingAddOnPermission
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class CampaignCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.settings_my_aw_accounts"),
            MediaBuyingAddOnPermission
        ),
    )

    @swagger_auto_schema(
        operation_description="Update campaign creation",
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Campaign creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    def put(self, request, *args, **kwargs):
        return super().put(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CampaignCreation.objects \
            .filter(Q(account_creation__owner=self.request.user) | Q(campaign__account_id=DEMO_ACCOUNT_ID)) \
            .filter(is_deleted=False)
        return queryset

    def delete(self, *args, **_):
        instance = self.get_object()

        campaigns_count = self.get_queryset().filter(
            account_creation=instance.account_creation).count()
        if campaigns_count < 2:
            return Response(status=HTTP_400_BAD_REQUEST,
                            data=dict(
                                error="You cannot delete the only campaign"))
        instance.is_deleted = True
        instance.save()
        return Response(status=HTTP_204_NO_CONTENT)

    def update(self, request, *args, **kwargs):
        """
        PUT method handler: Entire update of CampaignCreation
        :param request: request.data -> dict of full CampaignCreation object to PUT
        """

        partial = False
        instance = self.get_object()
        serializer = CampaignCreationUpdateSerializer(
            instance, data=request.data, partial=partial
        )
        serializer.is_valid(raise_exception=True)
        obj = serializer.save()
        self.update_related_models(obj.id, request.data)

        return self.retrieve(self, request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        """
        PATCH method handler: Partial updating of CampaignCreations
        :param request: request.data -> dict of fields and values to PATCH
        """
        partial = True
        instance = self.get_object()
        serializer = CampaignCreationUpdateSerializer(
            instance, data=request.data, partial=partial
        )
        serializer.is_valid(raise_exception=True)
        obj = serializer.save()
        self.update_related_models(obj.id, request.data)

        return self.retrieve(self, request, *args, **kwargs)

    @staticmethod
    def update_related_models(uid, data):
        if "ad_schedule_rules" in data:
            schedule_qs = AdScheduleRule.objects.filter(
                campaign_creation_id=uid
            )
            fields = ("day", "from_hour", "from_minute", "to_hour",
                      "to_minute")
            existed = set(schedule_qs.values_list(*fields))
            sent = set(
                tuple(r.get(f, 0) for f in fields)
                for r in data["ad_schedule_rules"]
            )
            to_delete = existed - sent
            for i in to_delete:
                filters = dict(zip(fields, i))
                schedule_qs.filter(**filters).delete()

            to_create = sent - existed
            bulk = []
            for i in to_create:
                filters = dict(zip(fields, i))
                bulk.append(
                    AdScheduleRule(campaign_creation_id=uid, **filters)
                )
            AdScheduleRule.objects.bulk_create(bulk)

        if "frequency_capping" in data:
            rules = {
                r["event_type"]: r
                for r in data.get("frequency_capping", [])
            }
            for event_type, _ in FrequencyCap.EVENT_TYPES:
                rule = rules.get(event_type)
                if rule is None:  # if rule isn"t sent
                    FrequencyCap.objects.filter(
                        campaign_creation=uid,
                        event_type=event_type,
                    ).delete()
                else:
                    rule["campaign_creation"] = uid
                    try:
                        instance = FrequencyCap.objects.get(
                            campaign_creation=uid,
                            event_type=event_type,
                        )
                    except FrequencyCap.DoesNotExist:
                        instance = None
                    serializer = FrequencyCapUpdateSerializer(
                        instance=instance, data=rule)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()

        if "location_rules" in data:
            queryset = LocationRule.objects.filter(
                campaign_creation_id=uid
            )
            fields = ("geo_target_id", "latitude", "longitude")
            existed = set(queryset.values_list(*fields))
            sent = set(
                (r.get("geo_target"),
                 Decimal(r["latitude"]) if "latitude" in r else None,
                 Decimal(r["longitude"]) if "longitude" in r else None)
                for r in data["location_rules"]
            )
            to_delete = existed - sent
            for rule in to_delete:
                filters = dict(zip(fields, rule))
                queryset.filter(**filters).delete()

            # create or update
            for rule in data["location_rules"]:
                rule["campaign_creation"] = uid
                try:
                    instance = queryset.get(
                        geo_target_id=rule.get("geo_target"),
                        latitude=rule.get("latitude"),
                        longitude=rule.get("longitude"),
                    )
                except LocationRule.DoesNotExist:
                    instance = None
                serializer = OptimizationLocationRuleUpdateSerializer(
                    instance=instance, data=rule)
                serializer.is_valid(raise_exception=True)
                serializer.save()
