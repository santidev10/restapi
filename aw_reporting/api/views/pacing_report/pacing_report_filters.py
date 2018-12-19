from datetime import timedelta

from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.api.serializers import CategorySerializer
from aw_reporting.models.salesforce import User, SalesForceGoalTypes, Category, Opportunity
from utils.datetime import now_in_default_tz


class PacingReportFiltersApiView(APIView):

    @staticmethod
    def get(*a, **_):
        start_date = now_in_default_tz().date().replace(day=1)
        end_date = (start_date + timedelta(days=31)).replace(
            day=1) - timedelta(days=1)

        period = [
            dict(
                id="{}_{}".format(which, period),
                name="{} {}".format(label.capitalize(), period.capitalize()),
            )
            for which, label in (("this", "current"), ("next", "next"))
            for period in ("month", "quarter", "year")
        ]
        period.append(dict(id="custom", name="Custom"))

        active_users = User.objects.filter(is_active=True)
        sales = active_users.exclude(sold_opportunities__isnull=True)
        ams = active_users.exclude(managed_opportunities__isnull=True)
        ad_ops = active_users.exclude(ad_managed_opportunities__isnull=True)

        territories = Opportunity.objects.values_list('territory', flat=True).distinct()

        filters = dict(
            category=CategorySerializer(Category.objects.all().order_by('id'),
                                        many=True).data,
            region=[dict(id=territory, name=territory) for territory in territories],
            goal_type=[dict(id=n, name=r) for n, r in
                       enumerate(SalesForceGoalTypes[:3])],
            am=_map_users(ams),
            sales=_map_users(sales),
            ad_ops=_map_users(ad_ops),
            period=period,
            start=start_date,
            end=end_date,
            status=[
                dict(id=status, name=status.capitalize())
                for status in ("active", "completed", "upcoming")
            ],
        )

        return Response(filters)


def _map_users(users_qs):
    return [dict(id=u.id, name=u.name) for u in users_qs]
