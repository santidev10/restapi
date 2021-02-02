from django.db.models import Q
from rest_framework.generics import ListCreateAPIView
from rest_framework.response import Response
from rest_framework.status import HTTP_201_CREATED
from rest_framework.status import HTTP_404_NOT_FOUND

from aw_creation.api.serializers import AppendCampaignCreationSerializer
from aw_creation.api.serializers import CampaignCreationSetupSerializer
from aw_creation.api.views.utils.is_demo import is_demo
from aw_creation.models import AccountCreation
from aw_creation.models import AdCreation
from aw_creation.models import AdGroupCreation
from aw_creation.models import CampaignCreation
from aw_creation.models import default_languages
from aw_reporting.demo.data import DEMO_ACCOUNT_ID
from aw_reporting.demo.views import forbidden_for_demo
from userprofile.constants import StaticPermissions


class CampaignCreationListSetupApiView(ListCreateAPIView):
    serializer_class = CampaignCreationSetupSerializer
    permission_classes = (StaticPermissions.has_perms(StaticPermissions.MEDIA_BUYING),)

    def get_queryset(self):
        pk = self.kwargs.get("pk")
        queryset = CampaignCreation.objects.filter(Q(campaign__account_id=DEMO_ACCOUNT_ID)
                                                   | Q(account_creation__owner=self.request.user)) \
            .filter(account_creation_id=pk,
                    is_deleted=False, )
        return queryset

    @forbidden_for_demo(is_demo)
    def create(self, request, *args, **kwargs):
        try:
            account_creation = AccountCreation.objects.get(
                pk=kwargs.get("pk"),
                owner=request.user,
                is_managed=True,
            )
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        count = CampaignCreation.objects.filter(
            account_creation__owner=self.request.user,
            account_creation_id=kwargs.get("pk"),
        ).count()
        data = dict(
            name="Campaign {}".format(count + 1),
            account_creation=account_creation.id,
        )
        serializer = AppendCampaignCreationSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        campaign_creation = serializer.save()

        for language in default_languages():
            campaign_creation.languages.add(language)

        ad_group_creation = AdGroupCreation.objects.create(
            name="AdGroup 1",
            campaign_creation=campaign_creation,
        )
        AdCreation.objects.create(
            name="Ad 1",
            ad_group_creation=ad_group_creation,
        )

        data = self.get_serializer(instance=campaign_creation).data
        return Response(data, status=HTTP_201_CREATED)
