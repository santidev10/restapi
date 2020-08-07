from rest_framework.serializers import Serializer
from utils.serializers.fields import PercentField
from rest_framework.serializers import FloatField
from rest_framework.serializers import CharField


class DashboardManagedServiceAveragesSerializer(Serializer):
    ctr = PercentField()
    video_view_rate = PercentField()
    video_quartile_100_rate = PercentField()


class DashboardManagedServiceAveragesAdminSerializer(DashboardManagedServiceAveragesSerializer):
    margin = PercentField()
    pacing = PercentField()
    cpv = FloatField()


class BaseOpportunitySerializer(Serializer):
    name = CharField(max_length=250)
    aw_cid = CharField()


class DashboardManagedServiceOpportunitySerializer(
    BaseOpportunitySerializer,
    DashboardManagedServiceAveragesSerializer):
    pass


class DashboardManagedServiceOpportunityAdminSerializer(
    BaseOpportunitySerializer,
    DashboardManagedServiceAveragesAdminSerializer):
    pass
