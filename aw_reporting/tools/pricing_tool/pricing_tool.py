from datetime import datetime

from django.db.models import Sum

from aw_reporting.models import Opportunity
from aw_reporting.tools.pricing_tool.pricing_tool_estimate import \
    PricingToolEstimate
from aw_reporting.tools.pricing_tool.pricing_tool_filtering import \
    PricingToolFiltering
from aw_reporting.tools.pricing_tool.pricing_tool_serializer import \
    PricingToolSerializer
from utils.datetime import now_in_default_tz, build_periods

DATE_FORMAT = "%Y-%m-%d"


class PricingTool:
    def __init__(self, today=None, user=None, **kwargs):
        self.today = today or now_in_default_tz().date()
        kwargs.update(self._get_date_kwargs(kwargs))
        kwargs['margin'] = kwargs.get('margin') or 30
        self.kwargs = kwargs
        self.filter = PricingToolFiltering(kwargs)
        self.serializer = PricingToolSerializer(kwargs)
        self._opportunities_qs = self.filter.apply(
            self._get_opportunity_queryset(user))
        self.estimate_tool = PricingToolEstimate(
            kwargs, self.get_opportunities_queryset())

    @classmethod
    def get_filters(cls, user=None):
        return PricingToolFiltering.get_filters(user=user)

    @property
    def estimate(self):
        return self.estimate_tool.estimate()

    def get_opportunities_data(self, opportunities, user):
        return self.serializer.get_opportunities_data(opportunities, user)

    def _get_date_kwargs(self, kwargs):
        quarters = kwargs.get('quarters')
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

    def _get_opportunity_queryset(self, user):
        return Opportunity.objects.have_campaigns(user=user) \
            .annotate(aw_budget=Sum("placements__adwords_campaigns__cost")) \
            .order_by("-aw_budget")

    def get_opportunities_queryset(self):
        return self._opportunities_qs