from datetime import datetime

from rest_framework.views import APIView


class PacingReportHelper(APIView):
    date_format = '%Y-%m-%d'

    def get_filters(self):
        r_data = self.request.GET

        start = r_data.get('start_date')
        if start:
            start = datetime.strptime(start, self.date_format).date()

        end = r_data.get('end_date')
        if end:
            end = datetime.strptime(end, self.date_format).date()

        filters = dict(
            start_date=start,
            end_date=end,
            goal_type_id=r_data.get('goal_type'),
            region_id=r_data.get('region'),
            category_id=r_data.get('category'),
            ad_ops_id=r_data.get('ad_ops'),
            am_id=r_data.get('am'),
            sales_id=r_data.get('sales'),
            period=r_data.get('period'),
            status=r_data.get('status'),
        )
        return filters

    @staticmethod
    def multiply_percents(data):

        def multiply_item_fields(i):
            if not isinstance(i, dict):
                return
            for sub in ("placements", "flights", "campaigns"):
                if sub in i:
                    for it in i[sub]:
                        multiply_item_fields(it)
            for f in ("margin", "pacing", "video_view_rate", "ctr"):
                v = i.get(f, 0)
                if v:
                    i[f] = v * 100

        for item in data:
            multiply_item_fields(item)
