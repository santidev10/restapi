from django.contrib.auth import get_user_model
from rest_framework.generics import UpdateAPIView
from rest_framework.status import HTTP_200_OK

from aw_reporting.api.serializers.pacing_report_opportunity_update_serializer import \
    PacingReportOpportunityUpdateSerializer
from aw_reporting.models import Opportunity
from userprofile.models import UserSettingsKey


class PacingReportOpportunityUpdateApiView(UpdateAPIView):
    serializer_class = PacingReportOpportunityUpdateSerializer

    def get_queryset(self):
        queryset = Opportunity.objects.all()
        user = self.request.user
        user_settings = user.aw_settings
        if user_settings.get(UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY):
            visible_ids = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
            queryset = queryset.filter(
                placements__adwords_campaigns__account_id__in=visible_ids
            ).distinct()
        return queryset

    def update(self, request, *args, **kwargs):
        response = super(PacingReportOpportunityUpdateApiView, self).update(
            request, *args, **kwargs)
        if response.status_code == HTTP_200_OK:
            response.data['thumbnail'] = None
            ad_ops = self.get_object().ad_ops_manager
            if ad_ops:
                profile_images = get_user_model().objects.filter(
                    email=ad_ops.email,
                    profile_image_url__isnull=False,
                ).exclude(profile_image_url="").values_list(
                    "profile_image_url", flat=True)
                if profile_images:
                    response.data['thumbnail'] = profile_images[0]
        return response
