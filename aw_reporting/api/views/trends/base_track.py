from datetime import datetime

from rest_framework.views import APIView

from aw_reporting.models import DATE_FORMAT


class TrackApiBase(APIView):
    indicators = (
        ("average_cpv", "CPV"),
        ("average_cpm", "CPM"),
        ("video_view_rate", "View Rate"),
        ("ctr", "CTR(i)"),
        ("ctr_v", "CTR(v)"),
        ("impressions", "Impressions"),
        ("video_views", "Views"),
        ("clicks", "Clicks"),
        ("cost", "Costs"),
    )
    breakdowns = (
        ("daily", "Daily"),
        ("hourly", "Hourly"),
    )
    dimensions = (
        ("creative", "Creatives"),
        ("device", "Devices"),
        ("age", "Ages"),
        ("gender", "Genders"),
        ("video", "Top videos"),
        ("channel", "Top channels"),
        ("interest", "Top interests"),
        ("topic", "Top topics"),
        ("keyword", "Top keywords"),
    )

    def get_filters(self):
        data = self.request.query_params
        start_date = data.get("start_date")
        end_date = data.get("end_date")
        accounts = data.get("accounts")
        accounts = accounts.split("-") if accounts else None
        apex_deal = data.get("apex_deal")
        filters = dict(
            account=int(data.get("account")),
            accounts=accounts,
            campaign=data.get("campaign"),
            indicator=data.get("indicator", self.indicators[0][0]),
            breakdown=data.get("breakdown"),
            dimension=data.get("dimension"),
            start_date=datetime.strptime(start_date, DATE_FORMAT).date()
            if start_date else None,
            end_date=datetime.strptime(end_date, DATE_FORMAT).date()
            if end_date else None,
        )
        if apex_deal is not None:
            filters["apex_deal"] = apex_deal == "1"
        return filters
