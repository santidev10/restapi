import re

from django.db.models import Max
from django.db.models import Min
from rest_framework.response import Response
from rest_framework.status import HTTP_404_NOT_FOUND
from rest_framework.views import APIView

from aw_creation.models import AccountCreation
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign


class PerformanceTargetingFiltersAPIView(APIView):
    def get_queryset(self):
        user = self.request.user
        return AccountCreation.objects.user_related(user)

    @staticmethod
    def get_campaigns(item):
        campaign_creation_ids = set(item.campaign_creations.filter(is_deleted=False).values_list("id", flat=True))

        campaign_fields = (
            "name",
            "id",
            "ad_groups__name",
            "ad_groups__id",
            "status",
            "ad_groups__status",
            "start_date",
            "end_date",
        )
        rows = Campaign.objects.filter(account__account_creation=item) \
            .values(*campaign_fields) \
            .order_by("name", "id", "ad_groups__name", "ad_groups__id")
        campaigns = []
        for row in rows:

            campaign_creation_id = None
            cid_search = re.match(r"^.*#(\d+)$", row["name"])
            if cid_search:
                cid = int(cid_search.group(1))
                if cid in campaign_creation_ids:
                    campaign_creation_id = cid

            if not campaigns or row["id"] != campaigns[-1]["id"]:
                campaigns.append(
                    dict(
                        id=row["id"],
                        name=row["name"],
                        start_date=row["start_date"],
                        end_date=row["end_date"],
                        status=row["status"],
                        ad_groups=[],
                        campaign_creation_id=campaign_creation_id,
                    )
                )
            if row["ad_groups__id"] is not None:
                campaigns[-1]["ad_groups"].append(
                    dict(
                        id=row["ad_groups__id"],
                        name=row["ad_groups__name"],
                        status=row["ad_groups__status"],
                    )
                )
        return campaigns

    @staticmethod
    def get_static_filters():
        filters = dict(
            targeting=[
                dict(id=t, name="{}s".format(t.capitalize()))
                for t in ("channel", "video", "keyword", "topic", "interest")
            ],
            group_by=[
                dict(id="account", name="All Campaigns"),
                dict(id="campaign", name="Individual Campaigns"),
            ],
        )
        return filters

    def get(self, request, pk, **_):
        try:
            item = self.get_queryset().get(pk=pk)
        except AccountCreation.DoesNotExist:
            return Response(status=HTTP_404_NOT_FOUND)

        dates = AdGroupStatistic.objects \
            .filter(ad_group__campaign__account=item.account) \
            .aggregate(min_date=Min("date"), max_date=Max("date"), )
        filters = self.get_static_filters()
        filters["start_date"] = dates["min_date"]
        filters["end_date"] = dates["max_date"]
        filters["campaigns"] = self.get_campaigns(item)
        return Response(data=filters)
