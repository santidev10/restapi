from aw_reporting.models import Account
from aw_reporting.settings import InstanceSettings, InstanceSettingsKey


def get_account_queryset():
    global_trends_accounts_id = InstanceSettings() \
        .get(InstanceSettingsKey.GLOBAL_TRENDS_ACCOUNTS)
    return Account.objects \
        .filter(managers__id__in=global_trends_accounts_id)


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

    geo_locations = data.get("geo_locations")
    geo_location_ids = geo_locations.split(",") \
        if geo_locations is not None else None
    geo_locations_condition = data.get("geo_locations_condition")

    return dict(
        am_ids=am_ids,
        ad_ops_ids=ad_ops_ids,
        sales_ids=sales_ids,
        goal_type_ids=goal_type_ids,
        brands=brands,
        category_ids=category_ids,
        geo_location_ids=geo_location_ids,
        geo_location_condition=geo_locations_condition,
    )
