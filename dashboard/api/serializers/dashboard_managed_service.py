from rest_framework.serializers import Serializer
from utils.serializers.fields import PercentField
from rest_framework.serializers import FloatField

class DashboardManagedServiceOpportunitySerializer(Serializer):
    ctr = PercentField()
    video_view_rate = PercentField()
    video_quartile_100_rate = PercentField()


class DashboardManagedServiceOpportunityAdminSerialzer(DashboardManagedServiceOpportunitySerializer):
    margin = PercentField()
    pacing = PercentField()
    cpv = FloatField()


