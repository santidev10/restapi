from collections import defaultdict
from datetime import timedelta, datetime

from django.db.models import FloatField, Avg, F
from django.db.models.sql.query import get_field_names_from_opts

from aw_reporting.models import *
from aw_reporting.utils import get_dates_range
from singledb.connector import SingleDatabaseApiConnector, \
    SingleDatabaseApiConnectorException
from utils.datetime import now_in_default_tz, as_datetime
from utils.lang import flatten

logger = logging.getLogger(__name__)

TOP_LIMIT = 10


class TrendId:
    HISTORICAL = "historical"
    PLANNED = "planned"


class Indicator:
    CPV = "average_cpv"
    CPM = "average_cpm"
    VIEW_RATE = "video_view_rate"
    CTR = "ctr"
    CTR_V = "ctr_v"
    IMPRESSIONS = "impressions"
    VIEWS = "video_views"
    CLICKS = "clicks"
    COSTS = "cost"


class Breakdown:
    HOURLY = "hourly"
    DAILY = "daily"


INDICATORS_HAVE_PLANNED = (Indicator.CPM, Indicator.CPV, Indicator.IMPRESSIONS,
                           Indicator.VIEWS, Indicator.COSTS)


class DeliveryChart:

    def __init__(self, accounts, account=None, campaigns=None, campaign=None,
                 ad_groups=None,
                 indicator=None, dimension=None, breakdown=Breakdown.DAILY,
                 start_date=None, end_date=None,
                 additional_chart=None, segmented_by=None,
                 date=True, am_ids=None, ad_ops_ids=None, sales_ids=None,
                 goal_type_ids=None, brands=None, category_ids=None,
                 region_ids=None, **_):
        if account and account in accounts:
            accounts = [account]

        if campaign:
            campaigns = [campaign]

        if not campaigns and accounts:
            campaigns = Campaign.objects.filter(
                account_id__in=accounts
            ).values_list('id', flat=True)

        self.params = dict(
            accounts=accounts,
            campaigns=campaigns,
            ad_groups=ad_groups,
            indicator=indicator,
            dimension=dimension,
            breakdown=breakdown,
            start=start_date,
            end=end_date,
            segmented_by=segmented_by,
            date=date,
            am_ids=am_ids,
            ad_ops_ids=ad_ops_ids,
            sales_ids=sales_ids,
            goal_type_ids=goal_type_ids,
            brands=brands,
            category_ids=category_ids,
            region_ids=region_ids,
        )

        if additional_chart is None:
            additional_chart = bool(dimension)
        self.additional_chart = additional_chart
        self.additional_chart_type = 'pie' if indicator in SUM_STATS and \
                                              dimension in (
                                                  'ad', 'age', 'gender',
                                                  'creative',
                                                  'device') \
            else 'bar'

    # chart data ---------------
    def get_response(self):
        chart_type_kwargs = dict(
            additional_chart=self.additional_chart,
            additional_chart_type=self.additional_chart_type,
        )
        if self.params['segmented_by']:
            charts = self.get_segmented_data(
                self.get_chart_data, self.params['segmented_by'],
                **chart_type_kwargs
            )
        else:
            charts = [
                dict(
                    id=TrendId.HISTORICAL,
                    title="",
                    data=self.get_chart_data(),
                    **chart_type_kwargs
                )
            ]
            planned_data = self._get_planned_data()
            if planned_data is not None:
                charts.append(dict(
                    id=TrendId.PLANNED,
                    title="",
                    data=[planned_data],
                    additional_chart=False,
                    additional_chart_type="bar"
                ))

        return charts

    def get_segmented_data(self, method, segmented_by, **kwargs):
        items = defaultdict(lambda: {'campaigns': []})
        if self.params['ad_groups']:
            qs = Campaign.objects.filter(
                ad_groups__id__in=self.params['ad_groups'],
            ).distinct()
        elif self.params['campaigns']:
            qs = Campaign.objects.filter(
                pk__in=self.params['campaigns'],
            )
        else:
            qs = Campaign.objects.none()

        for i in qs.values('id', 'name'):
            item = items[i['id']]
            item['name'] = i['name']
            item['campaigns'].append(i['id'])

        result = []
        if len(items) > 1:  # summary for >1 items
            sum_key = 'Summary for %d %s' % (len(items), segmented_by)
            result.append(
                dict(
                    title=sum_key,
                    data=method(),
                    **kwargs
                )
            )

        items = sorted(items.values(), key=lambda n: n['name'])
        for i in items:
            self.params['campaigns'] = i['campaigns']
            result.append(
                dict(
                    title=i['name'],
                    data=method(),
                    **kwargs
                )
            )
        return result

    def _plan_placement_value_for_date(self, placement, date) -> tuple:
        if placement["start"] > date or placement["end"] < date:
            return 0,
        indicator = self.params.get("indicator")
        total_days = (placement["end"] - placement["start"]).days + 1
        if indicator in (Indicator.IMPRESSIONS, Indicator.VIEWS):
            return placement["ordered_units"] / total_days,
        if indicator == Indicator.COSTS:
            return placement["total_cost"] / total_days,
        if indicator == Indicator.CPV:
            return placement["total_cost"], placement["ordered_units"]
        if indicator == Indicator.CPM:
            return placement["total_cost"], placement["ordered_units"] / 1000.
        return 0,

    def _plan_value_for_date(self, placements, date):
        values = [self._plan_placement_value_for_date(p, date) for p in
                  placements]
        numerators = [val[0] for val in values]
        denominators = [val[1] if len(val) > 1 else 0 for val in values]

        numerator = sum(numerators) if numerators else 0
        denominator = sum(denominators) if denominators else 1

        if denominator == 0:
            denominator = 1

        return dict(value=numerator / denominator,
                    label=date)

    def _extend_to_day(self, item):
        divider = 1 \
            if self.params["indicator"] in (Indicator.CPV, Indicator.CPM) \
            else 24
        value = item["value"] * 1. / divider
        start_of_day = as_datetime(item["label"])
        return [dict(value=value, label=start_of_day + timedelta(hours=i))
                for i in range(24)]

    def _get_planned_data(self):
        if self.params.get("indicator") not in INDICATORS_HAVE_PLANNED:
            return

        placements = self.get_placements()
        placements_start = placements.aggregate(Min("start"))['start__min']

        if placements_start is None:
            return

        placements = placements.values("start",
                                       "end",
                                       "total_cost",
                                       "ordered_units")

        start = max(placements_start, self.params.get("start"))
        end = self.params.get("end")

        total_days = (end - start).days + 1
        trend = [
            self._plan_value_for_date(placements, start + timedelta(days=i))
            for i in range(total_days)
        ]
        value = sum([r.get('value') for r in trend]) if trend else None
        average = value / total_days if value is not None and total_days else 0
        breakdown = self.params['breakdown']
        if breakdown == Breakdown.HOURLY:
            trend = flatten([self._extend_to_day(i) for i in trend])
        return dict(
            value=value,
            average=average,
            trend=trend,
            label="Planned"
        )

    def get_chart_data(self):
        params = self.params

        dimension = self.params['dimension']
        method = getattr(self, "_get_%s_data" % dimension, None)
        breakdown = self.params['breakdown']

        if method:
            items_by_label = method()
        elif breakdown == Breakdown.HOURLY:
            group_by = ('date', 'hour')
            data = self.get_raw_stats(
                CampaignHourlyStatistic.objects.all(), group_by, False
            )
            items_by_label = dict(Summary=data)
        else:
            group_by = ['date']
            data = self.get_raw_stats(
                AdGroupStatistic.objects.all(), group_by
            )
            items_by_label = dict(Summary=data)

        fields = self.get_fields()
        values_func = self.get_values_func()
        chart_items = []

        #
        for label, items in items_by_label.items():
            results = []
            summaries = defaultdict(float)
            for item in items:
                if 'label' in item:
                    label = item['label']
                    del item['label']

                value = values_func(item)
                if value is not None:
                    if "hour" in item:
                        date = item['date']
                        point_label = datetime(
                            year=date.year, month=date.month,
                            day=date.day, hour=item['hour'],
                        )
                    else:
                        point_label = item['date']

                    results.append(
                        {
                            'label': point_label,
                            'value': value
                        }
                    )
                    for f in fields:
                        v = item[f]
                        if v:
                            summaries[f] += v

            # if not empty chart
            if any(i['value'] for i in results):
                value = values_func(summaries)
                days = len(results)
                average = (value / days
                           if params['indicator'] in SUM_STATS and days
                           else value)

                chart_item = dict(
                    value=values_func(summaries),
                    trend=results,
                    average=average,
                    label=label
                )
                chart_items.append(
                    chart_item
                )

        if params['indicator'] in SUM_STATS:
            self.fill_missed_dates(chart_items)

        # sort by label
        chart_items = sorted(chart_items, key=lambda i: i['label'])

        return chart_items

    def get_values_func(self):
        indicator = self.params['indicator']

        if indicator in CALCULATED_STATS:
            info = CALCULATED_STATS[indicator]
            receipt = info['receipt']

            def value_func(data):
                dict_norm_base_stats(data)
                return receipt(**data)

        elif indicator in SUM_STATS:
            def value_func(data):
                dict_norm_base_stats(data)
                return data.get(indicator)
        else:
            raise ValueError("Unexpected indicator: %s" % indicator)
        return value_func

    # chart items -------------
    def get_items(self):
        self.params['date'] = False
        segmented_by = self.params['segmented_by']
        if segmented_by:
            return self.get_segmented_data(
                self._get_items, segmented_by
            )
        else:
            return self._get_items()

    def _get_items(self):
        daily_method = getattr(
            self, "_get_%s_data" % self.params['dimension']
        )
        data = daily_method()
        response = {
            'items': [],
            'summary': defaultdict(float)
        }
        average_positions = []

        for label, stats in data.items():
            if not stats:
                continue
            stat = stats[0]
            dict_norm_base_stats(stat)

            for n, v in stat.items():
                if v is not None and type(v) is not str and n != 'id':
                    if n == 'average_position':
                        average_positions.append(v)
                    else:
                        response['summary'][n] += v

            dict_calculate_stats(stat)
            dict_quartiles_to_rates(stat)
            del stat['video_impressions']

            if 'label' in stat:
                stat['name'] = stat['label']
                del stat['label']
            else:
                stat['name'] = label
            response['items'].append(
                stat
            )

        dict_calculate_stats(response['summary'])
        if 'video_impressions' in response['summary']:
            del response['summary']['video_impressions']
        if average_positions:
            response['summary']['average_position'] = sum(
                average_positions) / len(average_positions)
        dict_quartiles_to_rates(response['summary'])

        top_by = self.get_top_by()
        response['items'] = sorted(
            response['items'],
            key=lambda i: i[top_by] if i[top_by] else 0,
            reverse=True,
        )
        return response

    def get_external_cost(self, stat):
        external_rates = self.params['external_rates']
        external_cpv = external_rates['contracted_cpv']
        external_cpm = external_rates['contracted_cpm']
        cost = 0
        if external_cpv and stat['sum_video_views']:
            cost += float(external_cpv) * stat['sum_video_views']

        if external_cpm:
            if 'cpm_impressions' in stat:
                impressions = stat['cpm_impressions']
            else:
                impressions = stat['sum_impressions']
            if impressions:
                cost += float(external_cpm) / 1000 * impressions
        return cost

    # common ---
    @staticmethod
    def get_camp_link(queryset):
        fields = get_field_names_from_opts(queryset.model._meta)
        camp_link = "campaign"
        if camp_link not in fields:
            if 'ad_group' in fields:
                camp_link = "ad_group__campaign"
            elif 'ad' in fields:
                camp_link = 'ad__ad_group__campaign'
        return camp_link

    @staticmethod
    def get_ad_group_link(queryset):
        if queryset.model is AdStatistic:
            return "ad__ad_group"
        else:
            return "ad_group"

    def get_placements(self):
        queryset = OpPlacement.objects.all()
        filters = {"adwords_campaigns__account_id__in": self.params['accounts']}
        if self.params['start']:
            filters['end__gte'] = self.params['start']
        if self.params['end']:
            filters['start__lte'] = self.params['end']

        if self.params["am_ids"] is not None:
            filters["opportunity__account_manager_id__in"] = self.params[
                "am_ids"]

        if self.params["ad_ops_ids"] is not None:
            filters["opportunity__ad_ops_manager_id__in"] = self.params[
                "ad_ops_ids"]

        if self.params["sales_ids"] is not None:
            filters["opportunity__sales_manager_id__in"] = self.params[
                "sales_ids"]

        if self.params["brands"] is not None:
            filters["opportunity__brand__in"] = self.params["brands"]

        if self.params["goal_type_ids"] is not None:
            filters["goal_type_id__in"] = self.params["goal_type_ids"]

        if self.params["category_ids"] is not None:
            filters["opportunity__category_id__in"] = self.params[
                "category_ids"]

        if self.params["region_ids"] is not None:
            filters["opportunity__region_id__in"] = self.params["region_ids"]

        indicator = self.params["indicator"]
        if indicator in (Indicator.CPM, Indicator.IMPRESSIONS):
            filters["goal_type_id"] = SalesForceGoalType.CPM
        if indicator in (Indicator.CPV, Indicator.VIEWS):
            filters["goal_type_id"] = SalesForceGoalType.CPV

        if filters:
            queryset = queryset.filter(**filters)

        return queryset.distinct()

    def filter_queryset(self, queryset):
        camp_link = self.get_camp_link(queryset)
        opp_link = "%s__salesforce_placement__opportunity" % camp_link
        filters = {"%s__account_id__in" % camp_link: self.params['accounts']}
        if self.params['start']:
            filters['date__gte'] = self.params['start']
        if self.params['end']:
            filters['date__lte'] = self.params['end']

        if self.params['ad_groups']:
            ad_group_link = self.get_ad_group_link(queryset)
            filters["%s_id__in" % ad_group_link] = self.params['ad_groups']

        if self.params['campaigns']:
            filters["%s_id__in" % camp_link] = self.params['campaigns']

        if self.params['indicator'] in (Indicator.CPV, Indicator.CTR_V,
                                        Indicator.VIEW_RATE):
            filters['video_views__gt'] = 0

        if self.params["am_ids"] is not None:
            filters["%s__account_manager_id__in" % opp_link] = self.params["am_ids"]

        if self.params["ad_ops_ids"] is not None:
            filters["%s__ad_ops_manager_id__in" % opp_link] = self.params["ad_ops_ids"]

        if self.params["sales_ids"] is not None:
            filters["%s__sales_manager_id__in" % opp_link] = self.params["sales_ids"]

        if self.params["brands"] is not None:
            filters["%s__brand__in" % opp_link] = self.params["brands"]

        if self.params["goal_type_ids"] is not None:
            filters[
                "%s__salesforce_placement__goal_type_id__in" % camp_link] = \
                self.params["goal_type_ids"]

        if self.params["category_ids"] is not None:
            filters["%s__category_id__in" % opp_link] = self.params["category_ids"]

        if self.params["region_ids"] is not None:
            filters["%s__region_id__in" % opp_link] = self.params["region_ids"]

        if filters:
            queryset = queryset.filter(**filters)

        return queryset

    def add_annotate(self, queryset):
        if not self.params['date']:
            kwargs = dict(**all_stats_aggregate)
            if queryset.model is AdStatistic:
                kwargs['average_position'] = Avg(
                    Case(
                        When(
                            average_position__gt=0,
                            then=F('average_position'),
                        ),
                        output_field=FloatField(),
                    )
                )
        else:
            kwargs = {}
            fields = self.get_fields()
            all_sum_stats = SUM_STATS + CONVERSIONS + QUARTILE_STATS
            for v in fields:
                if v in all_sum_stats:
                    kwargs["sum_%s" % v] = Sum(v)
                elif v in base_stats_aggregate:
                    kwargs[v] = base_stats_aggregate[v]
        return queryset.annotate(**kwargs)

    @staticmethod
    def fill_missed_dates(init_data):
        prev_date = None
        for element in init_data:
            trend = []
            for item in element['trend']:
                curr_date = item['label']
                if prev_date:
                    days = (curr_date - prev_date).days
                    if days > 1:
                        dates_gen = get_dates_range(prev_date, curr_date)
                        for date in list(dates_gen)[1:-1]:
                            trend.append(
                                {
                                    'label': date,
                                    'value': 0
                                }
                            )
                trend.append(item)
                prev_date = curr_date

            element['trend'] = trend

    def get_fields(self):

        if not self.params['date']:
            item_stats = SUM_STATS + QUARTILE_STATS
            return item_stats

        indicator = self.params['indicator']

        if indicator in CALCULATED_STATS:
            info = CALCULATED_STATS[indicator]
            fields = info['dependencies']

        elif indicator in SUM_STATS:
            fields = (indicator,)
        else:
            raise ValueError("Unexpected indicator: %s" % indicator)

        return fields

    def get_top_by(self):
        if self.params['indicator'] == Indicator.COSTS:
            return 'cost'
        return 'impressions'

    def get_top_data(self, queryset, key):
        group_by = [key]

        date = self.params['date']
        if date:
            top_by = self.get_top_by()
            top_data = self.filter_queryset(queryset).values(key).annotate(
                top_by=Sum(top_by)
            ).order_by('-top_by')[:TOP_LIMIT]
            ids = [i[key] for i in top_data]

            queryset = queryset.filter(**{'%s__in' % key: ids})
            stats = self.get_raw_stats(
                queryset, group_by, date=True
            )

        else:
            stats = self.get_raw_stats(
                queryset, group_by
            )

        return stats

    def get_raw_stats(self, queryset, group_by, date=None):
        if date is None:
            date = self.params['date']
        if date:
            group_by.append('date')
        queryset = self.filter_queryset(queryset)
        queryset = queryset.values(*group_by).order_by(*group_by)
        return self.add_annotate(queryset)

    def _get_creative_data(self):
        result = defaultdict(list)
        raw_stats = self.get_raw_stats(
            VideoCreativeStatistic.objects.all(), ['creative_id'],
            date=self.params['date']
        )
        if raw_stats:
            connector = SingleDatabaseApiConnector()
            try:
                ids = [s['creative_id'] for s in raw_stats]
                items = connector.get_videos_base_info(ids)
            except SingleDatabaseApiConnectorException as e:
                logger.error(e)
                videos_info = {}
            else:
                videos_info = {i['id']: i for i in items}

            for item in raw_stats:
                youtube_id = item['creative_id']
                info = videos_info.get(youtube_id, {})
                item['id'] = youtube_id
                item['thumbnail'] = info.get('thumbnail_image_url')
                item['label'] = info.get('title', youtube_id)
                item['duration'] = info.get('duration')
                del item['creative_id']
                result[youtube_id].append(item)
        else:
            group_by = ['ad__creative_name']
            raw_stats = self.get_raw_stats(
                AdStatistic.objects.all(), group_by,
                self.params['date'],
            )
            result = defaultdict(list)
            for item in raw_stats:
                uid = item['ad__creative_name']
                item['label'] = uid
                del item['ad__creative_name']
                result[uid].append(item)
        return result

    def _get_ad_data(self):
        group_by = ['ad__creative_name', 'ad_id', 'ad__status']
        raw_stats = self.get_raw_stats(
            AdStatistic.objects.all(), group_by,
            self.params['date'],
        )
        result = defaultdict(list)
        for item in raw_stats:
            uid = item['ad_id']
            item['label'] = "{} #{}".format(item['ad__creative_name'], uid)
            item['status'] = item['ad__status']
            del item['ad__creative_name'], item['ad_id'], item['ad__status']
            result[uid].append(item)
        return result

    def _get_device_data(self):
        group_by = ['device_id']
        raw_stats = self.get_raw_stats(
            AdGroupStatistic.objects.all(), group_by,
            self.params['date']
        )
        result = defaultdict(list)
        for item in raw_stats:
            label = Devices[item['device_id']]
            del item['device_id']
            result[label].append(item)

        return result

    def _get_age_data(self):

        group_by = ['age_range_id']
        raw_stats = self.get_raw_stats(
            AgeRangeStatistic.objects.all(), group_by,
        )
        result = defaultdict(list)
        for item in raw_stats:
            label = AgeRanges[item['age_range_id']]
            del item['age_range_id']
            result[label].append(item)

        return result

    def _get_gender_data(self):
        group_by = ['gender_id']
        raw_stats = self.get_raw_stats(
            GenderStatistic.objects.all(), group_by,
        )
        result = defaultdict(list)
        for item in raw_stats:
            label = Genders[item['gender_id']]
            del item['gender_id']
            result[label].append(item)

        return result

    def _get_video_data(self, **kwargs):
        raw_stats = self.get_top_data(
            YTVideoStatistic.objects.all(),
            'yt_id'
        )
        connector = SingleDatabaseApiConnector()
        try:
            ids = [s['yt_id'] for s in raw_stats]
            items = connector.get_videos_base_info(ids)
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
            videos_info = {}
        else:
            videos_info = {i['id']: i for i in items}

        result = defaultdict(list)
        for item in raw_stats:
            youtube_id = item['yt_id']
            del item['yt_id']
            info = videos_info.get(youtube_id, {})
            item['id'] = youtube_id
            item['label'] = info.get('title', youtube_id)
            item['thumbnail'] = info.get('thumbnail_image_url')
            item['duration'] = info.get('duration')
            title = info.get('title', youtube_id)
            result[title].append(item)
        return result

    def _get_channel_data(self):
        raw_stats = self.get_top_data(
            YTChannelStatistic.objects.all(),
            'yt_id',
        )

        connector = SingleDatabaseApiConnector()
        try:
            ids = list(set(s['yt_id'] for s in raw_stats))
            items = connector.get_channels_base_info(ids)
        except SingleDatabaseApiConnectorException as e:
            logger.error(e)
            channels_info = {}
        else:
            channels_info = {i['id']: i for i in items}

        result = defaultdict(list)
        for item in raw_stats:
            channel_id = item['yt_id']
            del item['yt_id']
            item['id'] = channel_id
            info = channels_info.get(channel_id, {})
            item['thumbnail'] = info.get('thumbnail_image_url')
            label = info.get("title", channel_id)
            result[label].append(item)
        return result

    def _get_interest_data(self):
        raw_stats = self.get_top_data(
            AudienceStatistic.objects.all(),
            'audience_id',
        )

        ids = set(s['audience_id'] for s in raw_stats)
        labels = {
            c['id']: c['name']
            for c in Audience.objects.filter(
            pk__in=ids).values('id', 'name')
        }
        result = defaultdict(list)
        for item in raw_stats:
            audience_id = item['audience_id']
            label = labels.get(audience_id, audience_id)
            del item['audience_id']
            result[label].append(item)

        return result

    def _get_remarketing_data(self):
        raw_stats = self.get_top_data(
            RemarkStatistic.objects.all(),
            'remark_id',
        )
        ids = set(s['remark_id'] for s in raw_stats)
        labels = dict(
            RemarkList.objects.filter(pk__in=ids).values_list('id', 'name')
        )
        result = defaultdict(list)
        for item in raw_stats:
            item_id = item['remark_id']
            label = labels.get(item_id, item_id)
            del item['remark_id']
            result[label].append(item)

        return result

    def _get_topic_data(self):

        stats = self.get_top_data(
            TopicStatistic.objects.all(),
            'topic_id',
        )
        ids = set(s['topic_id'] for s in stats)
        labels = dict(
            Topic.objects.filter(pk__in=ids).values_list('id', 'name')
        )

        result = defaultdict(list)
        for item in stats:
            item_id = item['topic_id']
            label = labels.get(item_id, item_id)
            del item['topic_id']
            result[label].append(item)

        return result

    def _get_keyword_data(self):

        stats = self.get_top_data(
            KeywordStatistic.objects.all(),
            'keyword',
        )

        result = defaultdict(list)
        for item in stats:
            label = item['keyword']
            del item['keyword']
            result[label].append(item)

        return result

    def _get_location_data(self):

        raw_stats = self.get_top_data(
            CityStatistic.objects.all(),
            'city_id',
        )

        ids = set(c['city_id'] for c in raw_stats)
        cities = GeoTarget.objects.only('canonical_name').in_bulk(ids)

        result = defaultdict(list)
        for item in raw_stats:
            city = cities.get(item['city_id'])
            label = city.canonical_name if city else item['city_id']
            del item['city_id']
            result[label].append(item)

        return result

    def get_account_segmented_data(self):

        yesterday = now_in_default_tz().date() - timedelta(days=1)
        four_days_ago = yesterday - timedelta(days=4)

        values_func = self.get_values_func()

        if self.params['breakdown'] == Breakdown.HOURLY:
            queryset = CampaignHourlyStatistic.objects.all()
            account_id_field = "campaign__account_id"
            account_name_field = "campaign__account__name"
            order_by = [account_id_field, 'date', 'hour']
        else:
            queryset = AdGroupStatistic.objects.all()
            account_id_field = "ad_group__campaign__account_id"
            account_name_field = "ad_group__campaign__account__name"
            order_by = [account_id_field, 'date']

        values = order_by + [account_name_field]

        queryset = self.filter_queryset(queryset)
        queryset = queryset.values(*values).order_by(*order_by)
        raw_data = self.add_annotate(queryset)
        # response
        data = []
        if raw_data:
            current_account = None
            item = None
            sum_1d = count_1d = sum_5d = count_5d = 0

            for s in raw_data:
                account_id = s[account_id_field]
                if account_id != current_account:
                    if item:
                        item.update(
                            average_1d=sum_1d / count_1d if count_1d else None,
                            average_5d=sum_5d / count_5d if count_5d else None,
                        )
                        data.append(item)
                    # next account
                    current_account = account_id
                    item = dict(
                        id=s[account_id_field],
                        label=s[account_name_field],
                        trend=[],
                    )
                    sum_1d = count_1d = sum_5d = count_5d = 0

                value = values_func(s)
                date = s['date']
                if value and date >= four_days_ago:
                    sum_5d += value
                    count_5d += 1

                    if date >= yesterday:
                        sum_1d += value
                        count_1d += 1

                if "hour" in s:
                    date = s['date']
                    point_label = datetime(
                        year=date.year, month=date.month,
                        day=date.day, hour=s['hour'],
                    )
                else:
                    point_label = s['date']

                item['trend'].append(
                    dict(
                        label=point_label,
                        value=value,
                    )
                )
            else:
                if item:
                    item.update(
                        average_1d=sum_1d / count_1d if count_1d else None,
                        average_5d=sum_5d / count_5d if count_5d else None,
                    )
                    data.append(item)
        return sorted(data, key=lambda i: i['trend'][-1]['label'],
                      reverse=True)
