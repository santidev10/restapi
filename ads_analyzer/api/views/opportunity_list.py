from datetime import datetime

from django.conf import settings
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAdminUser

from ads_analyzer.api.serializers.opportunity_serializer import OpportunitySerializer
from aw_reporting.models import Opportunity
from utils.datetime import now_in_default_tz
from utils.datetime import start_of_year
from utils.permissions import or_permission_classes
from utils.permissions import user_has_permission


class OpportunityListAPIView(ListAPIView):
    permission_classes = (
        or_permission_classes(
            user_has_permission("userprofile.view_opportunity_list"),
        ),
    )
    queryset = Opportunity.objects.have_campaigns_from(
        min_start_date=start_of_year(
            year=now_in_default_tz().year - settings.SHOW_CAMPAIGNS_FOR_LAST_YEARS
        )
    )
    serializer_class = OpportunitySerializer


