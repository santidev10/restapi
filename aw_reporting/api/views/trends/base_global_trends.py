from django.conf import settings
from django.db.models import Q

from aw_reporting.demo.models import DEMO_ACCOUNT_ID
from aw_reporting.models import Account


def get_account_queryset(user):
    global_trends_account_id = settings.CHANNEL_FACTORY_ACCOUNT_ID
    queryset = Account.objects.get_queryset_for_user(user)

    queryset = queryset.filter(Q(managers__id=global_trends_account_id) | Q(id=DEMO_ACCOUNT_ID)) \
        .filter(can_manage_clients=False) \
        .order_by("name")
    return queryset


def get_filters(request):
    data = request.query_params
    am = data.get("am")
    am_ids = am.split(",") if am is not None else None

    ad_ops = data.get("ad_ops")
    ad_ops_ids = ad_ops.split(",") if ad_ops is not None else None

    sales = data.get("sales")
    sales_ids = sales.split(",") if sales is not None else None

    goal_types = data.get("goal_type")
    goal_type_ids = goal_types.split(",") \
        if goal_types is not None else None

    brands = data.get("brands")
    brands = brands.split(",") if brands is not None else None

    categories = data.get("category")
    category_ids = categories.split(",") if categories is not None else None

    regions = data.get("region")
    territories = [territory for territory in regions.split(",")] \
        if regions is not None else None

    return dict(
        am_ids=am_ids,
        ad_ops_ids=ad_ops_ids,
        sales_ids=sales_ids,
        goal_type_ids=goal_type_ids,
        brands=brands,
        category_ids=category_ids,
        territories=territories,
    )
