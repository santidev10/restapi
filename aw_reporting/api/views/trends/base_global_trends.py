from django.conf import settings

from aw_reporting.models import Account
from userprofile.models import UserSettingsKey


def get_account_queryset(user):
    global_trends_account_id = settings.CHANNEL_FACTORY_ACCOUNT_ID
    queryset = Account.objects.all()
    user_settings = user.get_aw_settings()
    if user_settings.get(UserSettingsKey.GLOBAL_ACCOUNT_VISIBILITY):
        visible_accounts = user_settings.get(UserSettingsKey.VISIBLE_ACCOUNTS)
        queryset = queryset.filter(id__in=visible_accounts)

    queryset = queryset.filter(
        managers__id=global_trends_account_id,
        can_manage_clients=False) \
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
    region_ids = [int(r_id) for r_id in regions.split(",")] \
        if regions is not None else None

    return dict(
        am_ids=am_ids,
        ad_ops_ids=ad_ops_ids,
        sales_ids=sales_ids,
        goal_type_ids=goal_type_ids,
        brands=brands,
        category_ids=category_ids,
        region_ids=region_ids,
    )
