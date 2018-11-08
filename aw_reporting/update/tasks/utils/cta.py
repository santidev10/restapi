from collections import defaultdict

__all__ = [
    "DAILY_STATISTICS_CLICK_TYPE_REPORT_FIELDS",
    "DAILY_STATISTICS_CLICK_TYPE_REPORT_UNIQUE_FIELD_NAME",
    "format_click_types_report",
    "update_stats_with_click_type_data",
]


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
        key = prepare_click_type_report_key(row, ref_id_name, unique_field_name)
        value = {"click_type": tracking_click_types.get(row.ClickType), "clicks": int(row.Clicks)}
        result[key] = result[key] + [value]
    return result


def update_stats_with_click_type_data(
        stats, click_type_data, row_obj, unique_field_name, ignore_a_few_records=False,
        ref_id_name="AdGroupId"):
    if click_type_data:
        key = prepare_click_type_report_key(row_obj, ref_id_name, unique_field_name)
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


def prepare_click_type_report_key(row, ref_id_name, unique_field_name):
    return "{}{}{}".format(getattr(row, ref_id_name), getattr(row, unique_field_name), row.Date)


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
