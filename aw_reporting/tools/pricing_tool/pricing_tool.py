from datetime import datetime

from aw_reporting.tools.pricing_tool.pricing_tool_estimate import \
    PricingToolEstimate
from aw_reporting.tools.pricing_tool.pricing_tool_filtering import \
    PricingToolFiltering
from aw_reporting.tools.pricing_tool.pricing_tool_serializer import \
    PricingToolSerializer
from utils.datetime import build_periods
from utils.datetime import now_in_default_tz

DATE_FORMAT = "%Y-%m-%d"


class PricingTool:
    def __init__(self, today=None, user=None, **kwargs):
        self.today = today or now_in_default_tz().date()
        kwargs.update(self._get_date_kwargs(kwargs))
        kwargs["margin"] = kwargs.get("margin") or 30
        self.kwargs = kwargs
        self.__opportunities_qs = None
        self.user = user

        self.filter = PricingToolFiltering(kwargs)
        self.serializer = PricingToolSerializer(kwargs)
        self.estimate_tool = PricingToolEstimate(kwargs)

    @classmethod
    def clean_filters_interests(cls):
        return PricingToolFiltering.clean_filters_interests()

    @classmethod
    def get_filters(cls, user=None, reuse_interests=False):
        return PricingToolFiltering.get_filters(user=user, reuse_interests=reuse_interests)

    @property
    def estimate(self):
        self.estimate_tool.set_opportunities(self.get_opportunities_queryset(ordering_by_aw_budget=False))
        return self.estimate_tool.estimate()

    def get_opportunities_data(self, opportunities):
        return self.serializer.get_opportunities_data(opportunities)

    def get_campaigns_data(self, campaigns_ids):
        return self.serializer.get_campaigns_data(campaigns_ids)

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

    def get_opportunities_queryset(self, ordering_by_aw_budget=True):
        if self.__opportunities_qs is None:
            self.__opportunities_qs = self.filter.apply(user=self.user, ordering_by_aw_budget=ordering_by_aw_budget)
        return self.__opportunities_qs
