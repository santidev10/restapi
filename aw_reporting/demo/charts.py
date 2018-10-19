import math
import random
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, time

from pytz import utc

from aw_reporting.models import *
from aw_reporting.utils import get_dates_range

CLICKS_TYPES_DIMENSIONS = (
    "gender",
    "age",
    "device",
    "topic",
    "interest",
    "keyword",
    "ad",
    "remarketing"
)


class DemoChart:

    def __init__(self, account, filters,
                 summary_label="Summary", goal_units=None,
                 cumulative=False):
        self.today = datetime.now().date()
        self.account = account
        self.filters = filters
        self.summary_label = summary_label
        self.goal_units = goal_units
        self.cumulative = cumulative

    @property
    def chart_items(self):
        dimension = self.filters.get('dimension')
        segmented = self.filters.get('segmented')
        if segmented:
            result = []
            if len(self.account.children) > 1:
                result.append(
                    dict(
                        title="Summary for %d campaigns" % len(
                            self.account.children),
                        data=self._get_items(self.account, dimension),
                    )
                )
            for campaign in self.account.children:
                result.append(
                    dict(
                        title="Demo Campaign %s" % campaign.name,
                        data=self._get_items(campaign, dimension),
                    )
                )
            return result
        else:
            return self._get_items(self.account, dimension)

    def _get_items(self, item, dimension):
        dimensions = deepcopy(getattr(item, dimension))
        all_stats = (
            'clicks', 'cost',  'impressions', 'video_impressions', 'video_views',
            'video25rate', 'video50rate', 'video75rate', 'video100rate',
            'view_through', 'conversions', 'all_conversions')
        if dimension in CLICKS_TYPES_DIMENSIONS:
            all_stats += CLICKS_STATS
        summary = {}
        items = {i['label']: i for i in dimensions}
        dim_len = len(dimensions)
        for stat in all_stats:
            value = getattr(item, stat)
            summary[stat] = value

            if stat in VIEW_RATE_STATS or stat in CLICKS_STATS:
                for item_stat in items.values():
                    item_stat[stat] = value
            else:
                values = self.explode_value(value, stat, dim_len)
                for item_stat, v in zip(items.values(), values):
                    item_stat[stat] = v

        res_items = []
        for name, stats in items.items():
            stats['name'] = name
            dict_add_calculated_stats(stats)
            del stats['label'], stats['video_impressions']
            res_items.append(stats)

        return dict(
            summary=summary,
            items=res_items
        )

    @property
    def charts(self):
        indicator = self.filters.get('indicator', 'average_cpv')
        dimension = self.filters.get('dimension')
        segmented = self.filters.get('segmented')

        chart_type_kwargs = dict(
            additional_chart=bool(dimension),
            additional_chart_type='pie'
            if indicator in SUM_STATS and dimension in (
                'age', 'gender', 'creative', 'device') else 'bar',
        )
        charts = []
        if segmented:
            if indicator in SUM_STATS:
                def red(a, b):
                    return a + b

            else:
                def red(a, b):
                    return a + b / 2
            summary = defaultdict(dict)
            for campaign in self.account.children:
                data = self.chart_lines(campaign, self.filters)
                charts.append(
                    dict(
                        title=campaign.name,
                        data=data,
                        **chart_type_kwargs
                    )
                )
                for line in data:
                    item = summary[line['label']]
                    item['label'] = line['label']
                    if 'value' in item:
                        item['value'] = red(item['value'], line['value'])
                    else:
                        item['value'] = line['value']

                    if 'trend' in item:
                        new_trend = []
                        for a, b in zip(line['trend'], item['trend']):
                            new_trend.append(
                                dict(
                                    label=a['label'],
                                    value=red(a['value'], b['value'])
                                )
                            )
                        item['trend'] = new_trend
                    else:
                        item['trend'] = deepcopy(line['trend'])
            if len(charts) > 1:
                sum_key = 'Summary for %d campaigns' % len(charts)
                data = sorted(list(summary.values()), key=lambda i: i['label'])

                charts.insert(
                    0,
                    dict(
                        title=sum_key,
                        data=data,
                        **chart_type_kwargs
                    )
                )
        else:
            data = self.chart_lines(self.account, self.filters)
            charts.append(
                dict(
                    title="",
                    data=data,
                    **chart_type_kwargs
                )
            )
        return charts

    def chart_lines(self, item, filters):
        lines = []
        dimension = filters.get('dimension')
        indicator = filters.get('indicator', 'average_cpv')
        breakdown = filters.get('breakdown')

        start = filters['start_date'] or item.start_date
        end = filters['end_date'] or item.end_date

        if start <= self.today:
            end = min(self.today, end)

            # get every days values
            value = getattr(item, indicator)

            if breakdown == "hourly":
                time_points = []
                for date in get_dates_range(start, end):
                    max_hour = datetime.now().hour \
                        if date == self.today else 24
                    for hour in range(max_hour):
                        time_points.append(
                            datetime.combine(
                                date, time(hour)).replace(tzinfo=utc)
                        )
            else:
                time_points = list(get_dates_range(start, end))
            time_points_len = len(time_points)

            if not dimension:
                values = self.explode_value_random(
                    value, indicator, time_points_len,
                )
                if self.cumulative and indicator in SUM_STATS:
                    current = 0
                    new_values = []
                    for v in values:
                        current += v
                        new_values.append(current)
                    values = new_values

                lines.append(
                    dict(
                        label=self.summary_label,
                        trend=[dict(label=l, value=v)
                               for l, v in zip(
                                    time_points,
                                    values
                               )],
                        value=value,
                    )
                )
            else:
                dimensions = deepcopy(getattr(item, dimension))
                dim_len = len(dimensions)
                dim_values = self.explode_value(value, indicator, dim_len)

                for dim, sum_value in zip(dimensions, dim_values):

                    daily_vs = self.explode_value_random(
                        sum_value, indicator, time_points_len,
                    )
                    line = dict(
                        average=None,
                        trend=[dict(label=l, value=v)
                               for l, v in zip(
                                    time_points,
                                    daily_vs
                               )],
                        value=sum_value,
                    )
                    line.update(dim)
                    lines.append(line)

            if self.goal_units:
                daily = math.ceil(self.goal_units / time_points_len)
                values = [
                    min(daily * (i + 1), self.goal_units)
                    for i in range(time_points_len)
                ]
                lines.append(
                    dict(
                        label="View Goal",
                        trend=[dict(label=l, value=v)
                               for l, v in zip(
                                    time_points,
                                    values
                               )],
                        value=value,
                    )
                )

        return lines

    @staticmethod
    def explode_value(initial_value, indicator, length):

        if initial_value is None:
            return [None for i in range(length)]

        value = initial_value // 2
        if indicator in SUM_STATS:
            if indicator != 'cost':
                daily = value // length if length else 0
            else:
                daily = value / length if length else 0
        else:
            daily = value

        # chart values
        values = [daily for i in range(length)]

        bonus = initial_value - value
        for n, i in enumerate(values):
            part = bonus // 2
            values[n] += part
            bonus -= part

        if bonus and values:
            values[-1] += bonus

        return values

    @staticmethod
    def explode_value_random(value, indicator, n):

        def get_val(val):
            return val

        if indicator in SUM_STATS:
            daily = value / n
            if indicator != 'cost':
                def get_val(val):
                    return int(val)
        else:
            daily = value

        # chart values
        values = [daily for i in range(n)]

        val_len = len(values)

        for n, i in enumerate(values):
            change = i/random.randint(3, 6)
            values[n] = get_val(values[n] - change)
            random_pos = random.randint(0, val_len - 1)
            values[random_pos] = get_val(values[random_pos] + change)

        return values
