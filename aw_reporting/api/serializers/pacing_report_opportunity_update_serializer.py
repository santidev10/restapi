from rest_framework.serializers import CharField
from rest_framework.serializers import JSONField
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import PrimaryKeyRelatedField

from aw_reporting.models import Opportunity
from aw_reporting.models import User


class PacingReportOpportunityUpdateSerializer(ModelSerializer):
    region = CharField(source="territory")
    am = PrimaryKeyRelatedField(source="account_manager",
                                queryset=User.objects.all())
    sales = PrimaryKeyRelatedField(source="sales_manager",
                                   queryset=User.objects.all())
    ad_ops = PrimaryKeyRelatedField(source="ad_ops_manager",
                                    queryset=User.objects.all())
    config = JSONField(required=False, allow_null=True)

    class Meta:
        model = Opportunity
        fields = (
            "region", "am", "sales", "ad_ops", "category", "notes", "cpm_buffer", "cpv_buffer", "config",
        )

    def validate_config(self, val):
        val = val or {}
        config = self.instance.config or {}
        config.update(val)
        return config
