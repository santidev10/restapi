from datetime import timedelta

from rest_framework.response import Response
from rest_framework.views import APIView

from aw_reporting.api.serializers import CategorySerializer
from aw_reporting.models.salesforce import User, SalesForceRegions, \
    SalesForceGoalTypes, UserRole, Category
from utils.datetime import now_in_default_tz


class PacingReportFiltersView(APIView):

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

        active_users = User.objects.filter(is_active=True,
                                           role__isnull=False).values("id",
                                                                      "name",
                                                                      "role__name")

        filters = dict(
            category=CategorySerializer(Category.objects.all().order_by('id'),
                                        many=True).data,
            region=[dict(id=n, name=r) for n, r in
                    enumerate(SalesForceRegions)],
            goal_type=[dict(id=n, name=r) for n, r in
                       enumerate(SalesForceGoalTypes[:3])],
            ad_ops=[
                dict(id=u['id'], name=u['name']) for u in filter(
                    lambda e: e['role__name'] == UserRole.AD_OPS_NAME,
                    active_users
                )
            ],
            am=[
                dict(id=u['id'], name=u['name']) for u in filter(
                    lambda e: e['role__name'] == UserRole.ACCOUNT_MANAGER_NAME,
                    active_users
                )
            ],
            sales=[
                dict(id=u['id'], name=u['name']) for u in filter(
                    lambda e: e['role__name'] not in (
                    UserRole.ACCOUNT_MANAGER_NAME, UserRole.AD_OPS_NAME),
                    active_users
                )
            ],
            period=period,
            start=start_date,
            end=end_date,
            status=[
                dict(id=status, name=status.capitalize())
                for status in ("active", "completed", "upcoming")
            ],
        )

        return Response(filters)
