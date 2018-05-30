from datetime import datetime

from django.db.models import Sum

from aw_reporting.models import Opportunity, Campaign
from aw_reporting.tools.pricing_tool.pricing_tool_estimate import \
    PricingToolEstimate
from aw_reporting.tools.pricing_tool.pricing_tool_filtering import \
    PricingToolFiltering
from aw_reporting.tools.pricing_tool.pricing_tool_serializer import \
    PricingToolSerializer
from utils.datetime import now_in_default_tz, build_periods


def model_to_filter(mod):
    return [dict(id=i.pk, name=str(i.name))
            for i in mod.objects.all().order_by('name')]


DATE_FORMAT = "%Y-%m-%d"


class PricingTool:
    def __init__(self, today=None, **kwargs):
        self.today = today or now_in_default_tz().date()
        kwargs.update(self._get_date_kwargs(kwargs))
        kwargs['margin'] = kwargs.get('margin') or 30
        self.kwargs = kwargs
        self.filter = PricingToolFiltering(kwargs)
        self.serializer = PricingToolSerializer(kwargs)
        self._opportunities_qs = self.filter.apply(
            self._get_opportunity_queryset())
        self.estimate_tool = PricingToolEstimate(
            kwargs, self.get_opportunities_queryset())

    @classmethod
    def get_filters(cls):
        return PricingToolFiltering.get_filters()

    @property
    def estimate(self):
        return self.estimate_tool.estimate()

    def get_opportunities_data(self, opportunities):
        return self.serializer.get_opportunities_data(opportunities)

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

    def _get_opportunity_queryset(self):
        queryset = Opportunity.objects.have_campaigns() \
            .filter(placements__adwords_campaigns__in=Campaign.objects.all()) \
            .annotate(aw_budget=Sum("placements__adwords_campaigns__cost")) \
            .order_by("-aw_budget")

        # todo: get rid of implicit filtering related items
        queryset = queryset \
            .filter(placements__adwords_campaigns__in=Campaign.objects.all())
        return queryset

    def get_opportunities_queryset(self):
        return self._opportunities_qs
