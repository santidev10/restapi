from datetime import datetime

from aw_reporting.models import Campaign
from aw_reporting.tools.forecast_tool.forecast_tool_estimate import ForecastToolEstimate
from aw_reporting.tools.forecast_tool.forecast_tool_filtering import ForecastToolFiltering
from utils.datetime import build_periods
from utils.datetime import now_in_default_tz

DATE_FORMAT = "%Y-%m-%d"


class ForecastTool:
    default_margin = 30

    def __init__(self, today=None, **kwargs):
        self.today = today or now_in_default_tz().date()
        kwargs.update(self._get_date_kwargs(kwargs))
        kwargs["margin"] = kwargs.get("margin") or self.default_margin
        self.kwargs = kwargs
        self.filter = ForecastToolFiltering(kwargs)
        self._campaigns_qs = self.filter.apply(
            self._get_campaigns_queryset())
        self.estimate_tool = ForecastToolEstimate(
            kwargs, self.get_campaigns_queryset())

    @classmethod
    def get_filters(cls):
        return ForecastToolFiltering.get_filters()

    @property
    def estimate(self):
        return self.estimate_tool.estimate()

    def _get_date_kwargs(self, kwargs):
        quarters = kwargs.get("quarters")
        start = datetime.strptime(kwargs["start"], DATE_FORMAT).date() \
            if kwargs.get("start") else None
        end = datetime.strptime(kwargs["end"], DATE_FORMAT).date() \
            if kwargs.get("end") else None
        compare_yoy = kwargs.get("compare_yoy", False)
        periods = build_periods(quarters, start, end, compare_yoy, self.today)
        return dict(
            periods=periods,
            start=min(d for d, _ in periods or [(None, None)]),
            end=max(d for _, d in periods or [(None, None)])
        )

    def _get_campaigns_queryset(self):
        return Campaign.objects

    def get_campaigns_queryset(self):
        return self._campaigns_qs
