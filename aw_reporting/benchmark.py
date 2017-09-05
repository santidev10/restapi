from django.db.models import IntegerField
from django.db.models import Sum, Case, When, F

from aw_reporting.models import Account
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import Campaign


class BenchMarkChart:
    run_command = ('calc param', 'calc param', 'result value', 'сalculation method', 'chart name')

    def __init__(self, request, annotate, aggregate, product_type):
        self.annotate = annotate
        self.aggregate = aggregate
        self.product_type = product_type
        self.accounts_ids = Account.user_objects(request.user).values_list("id", flat=True)
        self.campaigns_ids = Campaign.objects.filter(account_id__in=self.accounts_ids).values_list('id', flat=True)
        self.options = self.prepare_query_params(request.query_params)

    def prepare_query_params(self, params):
        options = {}
        if params.get('start_date'):
            options['start_date'] = params['start_date']
        if params.get('end_date'):
            options['end_date'] = params['end_date']
        if params.get('product_type') and self.product_type:
            options['product_type'] = params['product_type']
        options['frequency'] = params.get('frequency', 'month')

        return options

    def get_chart(self, calc_val_a, calc_val_b, output_field, method):
        queryset = self.get_queryset()
        if self.annotate:
            for item in queryset:
                param_a = item.get(calc_val_a)
                param_b = item.get(calc_val_b)
                if param_a and param_b:
                    item[output_field] = getattr(self, method)(param_a, param_b)
        if self.aggregate:
            param_a = queryset.get(calc_val_a)
            param_b = queryset.get(calc_val_b)
            if param_a and param_b:
                queryset[output_field] = getattr(self, method)(param_a, param_b)
        return queryset

    def get_queryset(self):
        queryset = AdGroupStatistic.objects.all()
        queryset = self.filter_queryset(queryset)
        queryset = self.prepare_timing(queryset)
        queryset = self.prepare_queryset(queryset)
        return queryset

    def prepare_timing(self, queryset):
        """
        Group by year, quarter, month, week, day
        """
        frequency = self.options['frequency']
        queryset = queryset.extra({frequency: "Extract({} from date)".format(frequency)}) \
            .values(frequency) \
            .order_by(frequency)
        return queryset

    def prepare_queryset(self, queryset):
        return queryset

    def filter_queryset(self, queryset):
        filters = {}
        if self.options.get('start_date'):
            filters['date__gte'] = self.options['start_date']
        if self.options.get('end_date'):
            filters['date__lte'] = self.options['end_date']
        if self.campaigns_ids:
            filters['ad_group__campaign__id__in'] = self.campaigns_ids
        if self.options.get('product_type'):
            filters['ad_group__type'] = self.options['product_type']
        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def get_video_view_rate(self, views, impressions):
        return views / impressions * 100

    def get_average_cpm_cost(self, cost, impressions):
        return cost / impressions

    def get_average_cpv_cost(self, cost, video_views):
        return cost / video_views

    def get_average_cpm_click(self, click, impressions):
        return click / impressions * 100

    def get_average_cpv_click(self, click, video_views):
        return click / video_views * 100

    def get_engagement_rate(self, engagements, impressions):
        return engagements / impressions * 100

    def get_viewability_rate(self, active_view_impressions, impressions):
        return active_view_impressions / impressions * 100


class ViewRateChart(BenchMarkChart):
    run_command = ('sum_video_views',
                   'video_impressions',
                   'video_view_rate',
                   'get_video_view_rate',
                   'view_rate_chart')

    def filter_queryset(self, queryset):
        queryset = super().filter_queryset(queryset)
        filters = {}
        filters['video_views__gt'] = 0
        queryset = queryset.filter(**filters)
        return queryset

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_video_views'] = Sum(F('video_views'))
        data['video_impressions'] = Sum(
            Case(
                When(
                    video_views__gt=0,
                    then="impressions",
                ),
                output_field=IntegerField()
            )
        )
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)


class ClickRateCpmChart(BenchMarkChart):
    run_command = ('sum_clicks',
                   'sum_impressions',
                   'average_cpm',
                   'get_average_cpm_click',
                   'click_rate_cpm_chart')

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_clicks'] = Sum(F('clicks'))
        data['sum_impressions'] = Sum(F('impressions'))
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)


class ClickRateCpvChart(BenchMarkChart):
    run_command = ('sum_clicks',
                   'sum_video_views',
                   'average_cpv',
                   'get_average_cpv_click',
                   'click_rate_cpv_chart')

    def filter_queryset(self, queryset):
        queryset = super().filter_queryset(queryset)
        filters = {}
        filters['video_views__gt'] = 0
        queryset = queryset.filter(**filters)
        return queryset

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_clicks'] = Sum(F('clicks'))
        data['sum_video_views'] = Sum(F('video_views'))
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)


class AverageCostRateCpmChart(BenchMarkChart):
    run_command = ('sum_cost',
                   'sum_impressions',
                   'average_cpm',
                   'get_average_cpm_cost',
                   'average_cost_rate_cpm_chart')

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_cost'] = Sum(F('cost'))
        data['sum_impressions'] = Sum(F('impressions'))
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)


class AverageCostRateCpvChart(BenchMarkChart):
    run_command = ('sum_cost',
                   'sum_video_views',
                   'average_cpv',
                   'get_average_cpv_cost',
                   'average_cost_rate_cpv_chart')

    def filter_queryset(self, queryset):
        queryset = super().filter_queryset(queryset)
        filters = {}
        filters['video_views__gt'] = 0
        queryset = queryset.filter(**filters)
        return queryset

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_cost'] = Sum(F('cost'))
        data['sum_video_views'] = Sum(F('video_views'))
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)


class ViewabilityRateChart(BenchMarkChart):
    run_command = ('sum_active_view_impressions',
                   'sum_impressions',
                   'viewability_rate',
                   'get_viewability_rate',
                   'viewability_rate_chart')

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_active_view_impressions'] = Sum(F('active_view_impressions'))
        data['sum_impressions'] = Sum(F('impressions'))
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)


class EngagementRateChart(BenchMarkChart):
    run_command = ('sum_engagements',
                   'sum_impressions',
                   'engagement_rate',
                   'get_engagement_rate',
                   'engagement_rate_chart')

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_engagements'] = Sum(F('engagements'))
        data['sum_impressions'] = Sum(F('impressions'))
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)


class QuartileCompletionRateChart(BenchMarkChart):
    run_command = (['video_views_25_quartile',
                    'video_views_50_quartile',
                    'video_views_75_quartile',
                    'video_views_100_quartile'],
                   'sum_impressions',
                   'auto',
                   'get_video_view_rate',
                   'view_rate_%_chart')

    def prepare_queryset(self, queryset):
        data = {}
        data['sum_impressions'] = Sum(F('impressions'))
        data['video_views_25_quartile'] = Sum(F('video_views_25_quartile'))
        data['video_views_50_quartile'] = Sum(F('video_views_50_quartile'))
        data['video_views_75_quartile'] = Sum(F('video_views_75_quartile'))
        data['video_views_100_quartile'] = Sum(F('video_views_100_quartile'))
        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)

    def get_chart(self, calc_val_a, calc_val_b, output_field, method):
        queryset = self.get_queryset()
        if self.aggregate:
            for view_type in calc_val_a:
                param_a = queryset.get(view_type)
                param_b = queryset.get(calc_val_b)
                if param_a and param_b:
                    queryset[view_type] = getattr(self, method)(param_a, param_b)
        if self.annotate:
            for item in queryset:
                for view_type in calc_val_a:
                    param_a = item.get(view_type)
                    param_b = item.get(calc_val_b)
                    if param_a and param_b:
                        item[view_type] = getattr(self, method)(param_a, param_b)
        return queryset


class ChartsHandler:
    charts_pool = (
        ViewRateChart,
        ClickRateCpmChart,
        ClickRateCpvChart,
        AverageCostRateCpmChart,
        AverageCostRateCpvChart,
        QuartileCompletionRateChart,
        EngagementRateChart,
        ViewabilityRateChart,
    )

    def __init__(self, request):
        self.request = request

    def base_charts(self):
        charts = {}
        for class_name in self.charts_pool:
            *params, chart_name = class_name.run_command
            if class_name == QuartileCompletionRateChart:
                charts[chart_name] = class_name(self.request,
                                                annotate=False,
                                                aggregate=True,
                                                product_type=False).get_chart(*params)
                continue
            charts[chart_name] = class_name(self.request,
                                            annotate=True,
                                            aggregate=False,
                                            product_type=False).get_chart(*params)
        return charts

    def product_charts(self):
        charts = {}
        timing = self.request.query_params.get('timing')
        for class_name in self.charts_pool:
            *params, chart_name = class_name.run_command
            if timing == '1':
                charts[chart_name] = class_name(self.request,
                                                annotate=True,
                                                aggregate=False,
                                                product_type=True).get_chart(*params)
            elif timing == '0':
                charts[chart_name] = class_name(self.request,
                                                annotate=False,
                                                aggregate=True,
                                                product_type=True).get_chart(*params)
        return charts
