from django.db.models import Q
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework.generics import RetrieveUpdateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_204_NO_CONTENT
from rest_framework.status import HTTP_400_BAD_REQUEST

from aw_creation.api.serializers import AdCreationSetupSerializer
from aw_creation.api.serializers import AdCreationUpdateSerializer
from aw_creation.api.views.utils.is_demo import is_demo_ad_creation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from utils.permissions import MediaBuyingAddOnPermission
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class AdCreationSetupApiView(RetrieveUpdateAPIView):
    serializer_class = AdCreationSetupSerializer
    permission_classes = (or_permission_classes(
        user_has_permission("userprofile.settings_my_aw_accounts"),
        MediaBuyingAddOnPermission),)

    @swagger_auto_schema(
        operation_description="Get Ad creation",
        manual_parameters=[
            openapi.Parameter(
                name="id",
                required=True,
                in_=openapi.IN_PATH,
                description="Ad creation id",
                type=openapi.TYPE_STRING,
            ),
        ],
    )
    @forbidden_for_demo(is_demo_ad_creation)
    def patch(self, request, *args, **kwargs):
        return super().patch(request, *args, **kwargs)

    def get_queryset(self):
        queryset = AdCreation.objects.filter(
            Q(ad__ad_group__campaign__account_id=DEMO_ACCOUNT_ID)
            | Q(ad_group_creation__campaign_creation__account_creation__owner=self.request.user)) \
            .filter(is_deleted=False, )
        return queryset

    # pylint: disable=too-many-return-statements
    def update(self, request, *args, **kwargs):
        partial = kwargs.pop("partial", False)
        instance = self.get_object()
        data = request.data
        serializer = self.get_serializer(instance, data=data, partial=partial)
        serializer.is_valid(raise_exception=True)

        # validate video ad format and video duration
        video_ad_format = data.get(
            "video_ad_format") or instance.ad_group_creation.video_ad_format
        if video_ad_format == AdGroupCreation.BUMPER_AD:
            # data is get from multipart form data, all values are strings
            video_duration = data.get("video_duration")
            video_duration = instance.video_duration if video_duration is None else float(
                video_duration)
            if video_duration > 6:
                return Response(
                    dict(error="Bumper ads video must be 6 seconds or less"),
                    status=HTTP_400_BAD_REQUEST)

        if "video_ad_format" in data:
            set_ad_format = data["video_ad_format"]
            ad_group_creation = instance.ad_group_creation
            campaign_creation = ad_group_creation.campaign_creation
            video_ad_format = ad_group_creation.video_ad_format
            if set_ad_format != video_ad_format:
                # ad group restrictions
                if ad_group_creation.is_pulled_to_aw:
                    return Response(
                        dict(error="{} is the only available ad type for this ad group".format(video_ad_format)),
                        status=HTTP_400_BAD_REQUEST,
                    )

                if ad_group_creation.ad_creations.filter(is_deleted=False).count() > 1:
                    return Response(dict(error="Ad type cannot be changed for only one ad within an ad group"),
                                    status=HTTP_400_BAD_REQUEST)

                # Invalid if the campaign bid strategy type is Target CPA and the ad long headline
                # and short headline have not been set
                if campaign_creation.bid_strategy_type == CampaignCreation.TARGET_CPA_STRATEGY \
                    and (data.get("long_headline") is None or data.get("short_headline") is None):
                    return Response(
                        dict(error="You must provide a short headline and long headline.", ),
                        status=HTTP_400_BAD_REQUEST
                    )

                # campaign restrictions
                set_bid_strategy = None
                if set_ad_format == AdGroupCreation.BUMPER_AD and \
                    campaign_creation.bid_strategy_type != CampaignCreation.MAX_CPM_STRATEGY:
                    set_bid_strategy = CampaignCreation.MAX_CPM_STRATEGY
                elif set_ad_format in (AdGroupCreation.IN_STREAM_TYPE,
                                       AdGroupCreation.DISCOVERY_TYPE) and \
                    campaign_creation.bid_strategy_type != CampaignCreation.MAX_CPV_STRATEGY:
                    set_bid_strategy = CampaignCreation.MAX_CPV_STRATEGY

                if set_bid_strategy:
                    if campaign_creation.is_pulled_to_aw:
                        return Response(
                            dict(error="You cannot use an ad of {} type in this campaign".format(set_bid_strategy)),
                            status=HTTP_400_BAD_REQUEST,
                        )

                    if AdCreation.objects.filter(ad_group_creation__campaign_creation=campaign_creation).count() > 1:
                        return Response(dict(error="Ad bid type cannot be changed for only one ad within a campaign"),
                                        status=HTTP_400_BAD_REQUEST)

                    CampaignCreation.objects.filter(id=campaign_creation.id) \
                        .update(bid_strategy_type=set_bid_strategy)
                    campaign_creation.bid_strategy_type = set_bid_strategy

                AdGroupCreation.objects.filter(id=ad_group_creation.id) \
                    .update(video_ad_format=set_ad_format)
                ad_group_creation.video_ad_format = set_ad_format

        serializer = AdCreationUpdateSerializer(
            instance, data=request.data, partial=partial,
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()
        return self.retrieve(self, request, *args, **kwargs)

    # pylint: enable=too-many-return-statements

    @forbidden_for_demo(is_demo_ad_creation)
    def delete(self, *args, **_):
        instance = self.get_object()

        count = self.get_queryset().filter(
            ad_group_creation=instance.ad_group_creation).count()
        if count < 2:
            return Response(dict(error="You cannot delete the only item"),
                            status=HTTP_400_BAD_REQUEST)
        instance.is_deleted = True
        instance.save()
        return Response(status=HTTP_204_NO_CONTENT)
