import itertools
from datetime import datetime
from operator import itemgetter

from django.db.models import Func
from django.db.models import IntegerField, CharField
from django.db.models import Q
from django.db.models import Sum, Case, When, F

from aw_reporting.models import Account
from aw_reporting.models import AdGroup
from aw_reporting.models import AdGroupStatistic
from aw_reporting.models import AgeRangeStatistic
from aw_reporting.models import AudienceStatistic
from aw_reporting.models import Campaign
from aw_reporting.models import GenderStatistic
from aw_reporting.models import KeywordStatistic
from aw_reporting.models import RemarkStatistic
from aw_reporting.models import TopicStatistic
from aw_reporting.models import VideoCreativeStatistic

class Monday(Func):

    function = 'Monday'
    template = "date_trunc('week', %(field_name)s)::date"
    output_field = CharField()

    def as_sqlite(self, compiler, connection, *args, **kwargs):
        return super(Monday, self).as_sql(
            compiler, connection,
            template=
            "date(%(field_name)s, "
            "CASE WHEN (strftime('%%w', %(field_name)s) + 0) > 0 "
            "THEN ('-' || "
            "(strftime('%%w', %(field_name)s) - 1) || ' days') "
            "ELSE '-6 days' END)",
            *args, **kwargs
        )
CREATIVE_LENGTH_FILTERS = {
    1: [0, 6000],
    2: [6000, 15000],
    3: [16000, 30000],
    4: [31000, 60000],
    5: [6000, 120000],
    6: [120000, 999999999],
    7: 'all'
}


class BenchMarkChart:
    def __init__(self, request, filtered_groups, annotate, aggregate, product_type):
        self.annotate = annotate
        self.aggregate = aggregate
        self.filtered_ad_groups = filtered_groups
        self.product_type = product_type
        # self.accounts_ids = Account.user_objects(request.user).values_list("id", flat=True)
        self.accounts_ids = Account.objects.all().values_list("id", flat=True)
        self.campaigns_ids = Campaign.objects.filter(account_id__in=self.accounts_ids).values_list('id', flat=True)
        self.options = self.prepare_query_params(request.query_params)

    def prepare_query_params(self, params):
        options = {}
        if params.get('start_date'):
            options['start_date'] = params['start_date']
        if params.get('end_date'):
            options['end_date'] = params['end_date']
        if params.get('quarters'):
            options['start_date'], options['end_date'] = self.get_quarters_date(params['quarters'])
        if params.get('product_type') and self.product_type:
            options['product_type'] = params['product_type']
        if params.get('device_id') and self.product_type:
            options['device_id'] = params['device_id'].split(',')
        if params.get('ad_group_ids'):
            options['ad_group_ids'] = params['ad_group_ids']
        options['frequency'] = params.get('frequency', 'month')

        return options

    def get_quarters_date(self, quarters):
        year = datetime.now().date().year
        quarters = quarters.split(',')
        quarter_days = dict(
            Q1=((1, 1), (3, 31)),
            Q2=((4, 1), (6, 30)),
            Q3=((7, 1), (9, 30)),
            Q4=((10, 1), (12, 31)),
        )
        first, *rest, last = list((itertools.chain(*[quarter_days[q] for q in quarters])))
        return datetime(year, *first).date(), datetime(year, *last).date()

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
        queryset = queryset.extra({frequency: "concat(extract(isoyear from aw_reporting_AdGroupStatistic.date), "
                                              "extract({} from aw_reporting_AdGroupStatistic.date))".format(frequency)}) \
            .values(frequency) \
            .order_by(frequency)
        return queryset

    def prepare_queryset(self, queryset):
        return queryset

    def filter_queryset(self, queryset):
        filters = {}
        if self.options.get('start_date'):
            filters['date__gte'] = self.options['start_date']
        else:
            filters['date__gte'] = datetime(datetime.now().date().year, 1, 1).date()
        if self.options.get('end_date'):
            filters['date__lte'] = self.options['end_date']
        if self.campaigns_ids:
            filters['ad_group__campaign__id__in'] = self.campaigns_ids
        if self.filtered_ad_groups:
            filters['ad_group_id__in'] = self.filtered_ad_groups
        if self.options.get('product_type'):
            filters['ad_group__type'] = self.options['product_type']
        if self.options.get('device_id'):
            filters['device_id__in'] = self.options['device_id']
        if filters:
            queryset = queryset.filter(**filters)
        return queryset

    def get_video_view_rate(self, views, impressions):
        if views and impressions:
            return views / impressions * 100

    def get_average_cpm_cost(self, cost, impressions):
        if cost and impressions:
            return cost / impressions

    def get_average_cpv_cost(self, cost, video_views):
        if cost and video_views:
            return cost / video_views

    def get_average_cpm_click(self, click, impressions):
        if click and impressions:
            return click / impressions * 100

    def get_average_cpv_click(self, click, video_views):
        if click and video_views:
            return click / video_views * 100

    def get_engagement_rate(self, engagements, impressions):
        if engagements and impressions:
            return engagements / impressions * 100

    def get_viewability_rate(self, active_view_impressions, impressions):
        if active_view_impressions and impressions:
            return active_view_impressions / impressions * 100


class ViewsBasedChart(BenchMarkChart):
    def filter_queryset(self, queryset):
        queryset = super().filter_queryset(queryset)
        filters = {}
        filters['video_views__gt'] = 0
        queryset = queryset.filter(**filters)
        return queryset

    def prepare_queryset(self, queryset):
        data = {}

        # For CPV cost rate
        data['sum_cost'] = Sum(F('cost'))

        # For CPV click rate
        data['sum_clicks'] = Sum(F('clicks'))

        # for View rate
        data['video_impressions'] = Sum(
            Case(
                When(
                    video_views__gt=0,
                    then="impressions",
                ),
                output_field=IntegerField()
            )
        )

        # For result value calculation
        data['sum_video_views'] = Sum(F('video_views'))

        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)

    def get_chart(self):
        queryset = self.get_queryset()
        if self.annotate:
            for item in queryset:
                sum_cost = item.get('sum_cost')
                sum_clicks = item.get('sum_clicks')
                video_impressions = item.get('video_impressions')
                sum_video_views = item.get('sum_video_views')
                item['video_view_rate'] = self.get_video_view_rate(sum_video_views, video_impressions)
                item['cpv_cost_rate'] = self.get_video_view_rate(sum_cost, video_impressions)
                item['cpv_click_rate'] = self.get_video_view_rate(sum_clicks, video_impressions)

        if self.aggregate:
            sum_cost = queryset.get('sum_cost')
            sum_clicks = queryset.get('sum_clicks')
            video_impressions = queryset.get('video_impressions')
            sum_video_views = queryset.get('sum_video_views')
            queryset['video_view_rate'] = self.get_video_view_rate(sum_video_views, video_impressions)
            queryset['cpv_cost_rate'] = self.get_video_view_rate(sum_cost, video_impressions)
            queryset['cpv_click_rate'] = self.get_video_view_rate(sum_clicks, video_impressions)
        return queryset


class ImpressionsBasedChart(BenchMarkChart):
    def prepare_queryset(self, queryset):
        data = {}
        # For Engagement Rate
        data['sum_engagements'] = Sum(F('engagements'))

        # For Viewability rate
        data['sum_active_view_impressions'] = Sum(F('active_view_impressions'))

        # For CPM cost rate
        data['sum_cost'] = Sum(F('cost'))

        # For CPM Click rate
        data['sum_clicks'] = Sum(F('clicks'))

        # For Quartile completion rate
        data['video_views_25_quartile'] = Sum(F('video_views_25_quartile'))
        data['video_views_50_quartile'] = Sum(F('video_views_50_quartile'))
        data['video_views_75_quartile'] = Sum(F('video_views_75_quartile'))
        data['video_views_100_quartile'] = Sum(F('video_views_100_quartile'))

        # For result value calculation
        data['sum_impressions'] = Sum(F('impressions'))

        if self.annotate:
            return queryset.annotate(**data)
        if self.aggregate:
            return queryset.aggregate(**data)

    def get_chart(self):
        queryset = self.get_queryset()
        if self.annotate:
            for item in queryset:
                sum_engagements = item.get('sum_engagements')
                sum_active_view_impressions = item.get('sum_active_view_impressions')
                sum_cost = item.get('sum_cost')
                sum_clicks = item.get('sum_clicks')
                sum_impressions = item.get('sum_impressions')
                video_views_25_quartile = item.get('video_views_25_quartile')
                video_views_50_quartile = item.get('video_views_50_quartile')
                video_views_75_quartile = item.get('video_views_75_quartile')
                video_views_100_quartile = item.get('video_views_100_quartile')

                item['viewability_chart'] = self.get_viewability_rate(sum_active_view_impressions, sum_impressions)
                item['engagement_rate_chart'] = self.get_engagement_rate(sum_engagements, sum_impressions)
                item['average_cpm_cost_chart'] = self.get_average_cpm_cost(sum_cost, sum_impressions)
                item['average_cpm_click_chart'] = self.get_average_cpm_click(sum_clicks, sum_impressions)
                item['video_views_25_quartile'] = self.get_video_view_rate(video_views_25_quartile, sum_impressions)
                item['video_views_50_quartile'] = self.get_video_view_rate(video_views_50_quartile, sum_impressions)
                item['video_views_75_quartile'] = self.get_video_view_rate(video_views_75_quartile, sum_impressions)
                item['video_views_100_quartile'] = self.get_video_view_rate(video_views_100_quartile, sum_impressions)

        if self.aggregate:
            sum_engagements = queryset.get('sum_engagements')
            sum_active_view_impressions = queryset.get('sum_active_view_impressions')
            sum_cost = queryset.get('sum_cost')
            sum_clicks = queryset.get('sum_clicks')
            sum_impressions = queryset.get('sum_impressions')
            video_views_25_quartile = queryset.get('video_views_25_quartile')
            video_views_50_quartile = queryset.get('video_views_50_quartile')
            video_views_75_quartile = queryset.get('video_views_75_quartile')
            video_views_100_quartile = queryset.get('video_views_100_quartile')

            queryset['viewability_chart'] = self.get_viewability_rate(sum_active_view_impressions, sum_impressions)
            queryset['engagement_rate_chart'] = self.get_engagement_rate(sum_engagements, sum_impressions)
            queryset['average_cpm_cost_chart'] = self.get_average_cpm_cost(sum_cost, sum_impressions)
            queryset['average_cpm_click_chart'] = self.get_average_cpm_click(sum_clicks, sum_impressions)
            queryset['video_views_25_quartile'] = self.get_video_view_rate(video_views_25_quartile, sum_impressions)
            queryset['video_views_50_quartile'] = self.get_video_view_rate(video_views_50_quartile, sum_impressions)
            queryset['video_views_75_quartile'] = self.get_video_view_rate(video_views_75_quartile, sum_impressions)
            queryset['video_views_100_quartile'] = self.get_video_view_rate(video_views_100_quartile, sum_impressions)
        return queryset


class FiltersHandler:
    def __init__(self, options):
        self.pool = []
        self.events = self.fill_event_map(options)

    def main(self):
        run_queue = [(k, v) for (k, v) in self.events.items() if v is not None]
        for method, param in run_queue:
            getattr(self, method)(param)
        if self.pool:
            first, *rest = self.pool
            result = set(first).intersection(*rest)
            return result
        return None

    def fill_event_map(self, options):
        result = {}
        targeting = options.get('targeting', [])
        if options.get('age_range'):
            result['age_range_statistics'] = options.get('age_range').split(',')
        if options.get('gender'):
            result['gender_statistics'] = options.get('gender').split(',')
        if options.get('topic'):
            result['topic_statistics'] = options.get('topic').split(',')
        if 'topic_targeting' in targeting:
            result['topic_statistics_targeting'] = True
        if options.get('interests'):
            result['interests_statistics'] = options.get('interests').split(',')
        if 'interests_targeting' in targeting:
            result['interests_statistics_targeting'] = True
        if 'remarketing_targeting' in targeting:
            result['remarketing_statistics'] = True
        if 'keywords_targeting' in targeting:
            result['keywords_statistics'] = True
        if options.get('creative_length'):
            duration = []
            creative_length_ids_list = options.get('creative_length').split(',')
            for creative_length_id in creative_length_ids_list:
                duration.append(CREATIVE_LENGTH_FILTERS[int(creative_length_id)])
            result['creative_length'] = duration
        return result

    def age_range_statistics(self, age_range_ids=None):
        if age_range_ids and 'all' not in age_range_ids:
            age_range_ids_list = list(
                AgeRangeStatistic.objects.filter(age_range_id__in=age_range_ids).values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(age_range_ids_list)
        else:
            age_range_ids_list = list(
                AgeRangeStatistic.objects.all().values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(age_range_ids_list)

    def gender_statistics(self, gender_ids=None):
        if gender_ids and 'all' not in gender_ids:
            gender_ids_list = list(
                GenderStatistic.objects.filter(gender_id__in=gender_ids).values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(gender_ids_list)
        else:
            gender_ids_list = list(
                GenderStatistic.objects.all().values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(gender_ids_list)

    def topic_statistics(self, topic_ids=None):
        if topic_ids:
            topic_statistics_ids_list = list(
                TopicStatistic.objects.filter(topic_id__in=topic_ids).values_list(
                    'ad_group__id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(topic_statistics_ids_list)

    def topic_statistics_targeting(self, topic=None):
        if topic:
            topic_statistics_ids_list = list(
                TopicStatistic.objects.all().values_list(
                    'ad_group__id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(topic_statistics_ids_list)

    def interests_statistics(self, interests_ids=None):
        if interests_ids:
            topic_statistics_ids_list = list(
                AudienceStatistic.objects.filter(audience_id__in=interests_ids).values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(topic_statistics_ids_list)

    def interests_statistics_targeting(self, interests=None):
        if interests:
            topic_statistics_ids_list = list(
                AudienceStatistic.objects.all().values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(topic_statistics_ids_list)

    def remarketing_statistics(self, remark=None):
        if remark:
            remarketing_ids_list = list(
                RemarkStatistic.objects.all().values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(remarketing_ids_list)

    def keywords_statistics(self, keywords):
        if keywords:
            keywords_ids_list = list(
                KeywordStatistic.objects.all().values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(keywords_ids_list)

    def creative_length(self, duration):
        if duration and 'all' not in duration:
            qr = None
            for pair in duration:
                l_border, r_border = pair
                q = (Q(**{'videos_stats__creative__duration__gt': l_border,
                          'videos_stats__creative__duration__lte': r_border}))
                qr = qr | q if qr else q
            creative_ids = list(AdGroup.objects.filter(qr).values_list('id', flat=True).order_by('id').distinct())
            self.pool.append(creative_ids)
        else:
            creative_ids = list(
                VideoCreativeStatistic.objects.all().values_list(
                    'ad_group_id', flat=True).order_by('ad_group_id').distinct())
            self.pool.append(creative_ids)


class ChartsHandler:
    impressions_based_charts = (
        'viewability_chart',
        'engagement_rate_chart',
        'average_cpm_cost_chart',
        'average_cpm_click_chart',
        'video_views_25_quartile',
        'video_views_50_quartile',
        'video_views_75_quartile',
        'video_views_100_quartile'
    )
    views_based_charts = (
        'video_view_rate',
        'cpv_cost_rate',
        'cpv_click_rate'
    )

    def __init__(self, request):
        self.request = request

    def base_charts(self):
        views_chart = ViewsBasedChart(self.request, None, annotate=True, aggregate=False,
                                      product_type=False).get_chart()
        impr_chart = ImpressionsBasedChart(self.request, None, annotate=True, aggregate=False,
                                           product_type=False).get_chart()
        result = self.charts_aggregator(impr_chart=impr_chart, views_chart=views_chart, timing=True)
        return result

    def product_charts(self):
        timing = self.request.query_params.get('timing')
        ad_group_ids = FiltersHandler(self.request.query_params).main()
        if timing == '1':
            views_chart = ViewsBasedChart(self.request, ad_group_ids, annotate=True, aggregate=False,
                                          product_type=True).get_chart()
            impr_chart = ImpressionsBasedChart(self.request, ad_group_ids, annotate=True, aggregate=False,
                                               product_type=True).get_chart()
            result = self.charts_aggregator(impr_chart=impr_chart, views_chart=views_chart, timing=True)
        else:
            views_chart = ViewsBasedChart(self.request, ad_group_ids, annotate=False, aggregate=True,
                                          product_type=True).get_chart()
            impr_chart = ImpressionsBasedChart(self.request, ad_group_ids, annotate=False, aggregate=True,
                                               product_type=True).get_chart()
            result = self.charts_aggregator(impr_chart=impr_chart, views_chart=views_chart)
        return result

    def charts_aggregator(self, impr_chart, views_chart, timing=None):
        charts = {}
        for impr_chart_type in self.impressions_based_charts:
            charts.update(self.charts_builder(impr_chart, impr_chart_type, self.request.query_params.get('frequency', 'month'), timing))
        for view_chart_type in self.views_based_charts:
            charts.update(self.charts_builder(views_chart, view_chart_type, self.request.query_params.get('frequency', 'month'), timing))
        return charts

    def charts_builder(self, chart, result_param, frequency, timing=None):
        result = {}
        result_chart_data = []
        if timing:
            for item in chart:
                param = item.get(result_param)
                timing_date = item.get(frequency)
                year, chart_frequency = timing_date[:4], timing_date[4:]
                if self.is_year_from_past(year):
                    continue
                picklerick = {'title': param, 'value': int(chart_frequency)}
                result_chart_data.append(picklerick)
        else:
            result_chart_data.append({'value': v for k, v in chart.items() if k == result_param})
        result_chart_data = sorted(result_chart_data, key=itemgetter('value'))
        result[result_param] = result_chart_data
        return result

    def is_year_from_past(self, year):
        start_date = self.request.query_params.get('start_date') or datetime(datetime.now().date().year, 1, 1).date()
        return start_date.year > datetime(int(year), 1, 1).year