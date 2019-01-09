from rest_framework.serializers import CharField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import PrimaryKeyRelatedField

from aw_reporting.models import Opportunity, User


class PacingReportOpportunityUpdateSerializer(ModelSerializer):
    region = CharField(source="territory")
    am = PrimaryKeyRelatedField(source="account_manager",
                                queryset=User.objects.all())
    sales = PrimaryKeyRelatedField(source="sales_manager",
                                   queryset=User.objects.all())
    ad_ops = PrimaryKeyRelatedField(source="ad_ops_manager",
                                    queryset=User.objects.all())

    class Meta:
        model = Opportunity
        fields = (
            "region", "am", "sales", "ad_ops", "category", "notes",
        )
