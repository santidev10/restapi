from datetime import timedelta

from aw_reporting.api.serializers import CategorySerializer
from aw_reporting.calculations.territories import get_salesforce_territories
from aw_reporting.models.salesforce import Category
from aw_reporting.models.salesforce import SalesForceGoalTypes
from aw_reporting.models.salesforce import User
from utils.datetime import now_in_default_tz

def _map_users(users_qs):
    return [dict(id=u.id, name=u.name) for u in users_qs]

def get_pacing_report_filters():
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

    territories = get_salesforce_territories()

    filters = dict(
        category=CategorySerializer(Category.objects.all().order_by("id"),
                                    many=True).data,
        region=[dict(id=territory, name=territory) for territory in territories],
        goal_type=[dict(id=n, name=r) for n, r in
                   enumerate(SalesForceGoalTypes[:3])],
        am=_map_users(ams),
        sales=_map_users(sales),
        ad_ops=_map_users(ad_ops),
        period=period,
        start=str(start_date),
        end=str(end_date),
        status=[
            dict(id=status, name=status.capitalize())
            for status in ("active", "completed", "upcoming")
        ],
    )

    return filters

