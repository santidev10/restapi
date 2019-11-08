from collections import defaultdict


def format_click_types_report(report, unique_field_name, ref_id_name="AdGroupId"):
    """
    :param report: click types report
    :param unique_field_name: Device, Age, Gender, Location, etc.
    :param ref_id_name:
    :return {"ad_group_id+unique_field+date": [Row(), Row() ...], ... }
    """
    if not report:
        return {}
    tracking_click_types = dict(TRACKING_CLICK_TYPES)
    report = [row for row in report if row.ClickType in tracking_click_types.keys()]
    result = defaultdict(list)
    for row in report:
        key = prepare_click_type_key(row, ref_id_name, unique_field_name)
        value = {"click_type": tracking_click_types.get(row.ClickType), "clicks": int(row.Clicks)}
        result[key] = result[key] + [value]
    return result


def update_stats_with_click_type_data(
        stats, click_type_data, row_obj, unique_field_name, ignore_a_few_records=False,
        ref_id_name="AdGroupId"):
    if click_type_data:
        key = prepare_click_type_key(row_obj, ref_id_name, unique_field_name)
        if ignore_a_few_records:
            try:
                key_data = click_type_data.pop(key)
            except KeyError:
                return stats
        else:
            key_data = click_type_data.get(key)
        if key_data:
            for obj in key_data:
                stats[obj.get("click_type")] = obj.get("clicks")
    return stats


def prepare_click_type_key(row, ref_id_name, unique_field_name):
    return "{}{}{}".format(getattr(row, ref_id_name), getattr(row, unique_field_name), row.Date)


def quart_views(row, n):
    per = getattr(row, "VideoQuartile%dRate" % n)
    impressions = int(row.Impressions)
    return float(per.rstrip("%")) / 100 * impressions


def get_base_stats(row, quartiles=False):
    stats = dict(
        impressions=int(row.Impressions),
        video_views=int(row.VideoViews),
        clicks=int(row.Clicks),
        cost=float(row.Cost) / 1000000,
        conversions=float(row.Conversions.replace(",", "")),
        all_conversions=float(row.AllConversions.replace(",", ""))
        if hasattr(row, "AllConversions") else 0,
        view_through=int(row.ViewThroughConversions),
    )
    if quartiles:
        stats.update(
            video_views_25_quartile=self.quart_views(row, 25),
            video_views_50_quartile=self.quart_views(row, 50),
            video_views_75_quartile=self.quart_views(row, 75),
            video_views_100_quartile=self.quart_views(row, 100),
        )
    return stats


TRACKING_CLICK_TYPES = (
    ("Website", "clicks_website"),
    ("Call-to-Action overlay", "clicks_call_to_action_overlay"),
    ("App store", "clicks_app_store"),
    ("Cards", "clicks_cards"),
    ("End cap", "clicks_end_cap")
)
DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME = "Criteria"
DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS = (
    "AdGroupId",
    "Date",
    "Criteria",
    "Clicks",
    "ClickType",
)