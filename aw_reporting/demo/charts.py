from aw_reporting.models import *
from aw_reporting.utils import get_dates_range
from collections import defaultdict
from copy import deepcopy
import random


class DemoChart:

    def __init__(self, account, filters):
        self.account = account
        self.filters = filters

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
            'average_cpm', 'average_cpv', 'clicks', 'cost', 'ctr',
            'ctr_v', 'impressions', 'video_view_rate', 'video_views',
            'video25rate', 'video50rate', 'video75rate', 'video100rate',
            'view_through', 'conversions', 'all_conversions',
        )
        summary = {}
        items = {i['label']: i for i in dimensions}
        dim_len = len(dimensions)
        sum_stats = SUM_STATS + CONVERSIONS
        for stat in all_stats:
            value = getattr(item, stat)
            summary[stat] = value

            if stat in sum_stats:
                values = self.explode_value(value, stat, dim_len)
                for item_stat, v in zip(items.values(), values):
                    item_stat[stat] = v

            elif stat in VIEW_RATE_STATS:
                for item_stat in items.values():
                    item_stat[stat] = value

        res_items = []
        for name, stats in items.items():
            stats['name'] = name
            del stats['label']
            dict_add_calculated_stats(stats)
            res_items.append(stats)

        return dict(
            summary=summary,
            items=res_items
        )

    @property
    def charts(self):
        indicator = self.filters.get('indicator', 'average_cpv')
        dimension = self.filters.get('dimension')

        chart_type_kwargs = dict(
            additional_chart=bool(dimension),
            additional_chart_type='pie'
            if indicator in SUM_STATS and dimension in (
                'age', 'gender', 'creative', 'device') else 'bar',
        )
        charts = []
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
        return charts

    def chart_lines(self, item, filters):
        lines = []
        dimension = filters.get('dimension')
        indicator = filters.get('indicator', 'average_cpv')

        start = filters['start_date'] or item.start_date
        end = filters['end_date'] or item.end_date

        # get every days values
        value = getattr(item, indicator)
        days = (end - start).days + 1

        if not dimension:
            values = self.explode_value_random(value, indicator, days)
            lines.append(
                dict(
                    average=None,
                    label="Summary",
                    trend=[dict(label=l, value=v)
                           for l, v in zip(
                                get_dates_range(start, end),
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

                daily_vs = self.explode_value_random(sum_value, indicator, days)
                line = dict(
                    average=None,
                    trend=[dict(label=l, value=v)
                           for l, v in zip(
                                get_dates_range(start, end),
                                daily_vs
                           )],
                    value=sum_value,
                )
                line.update(dim)
                lines.append(line)

        return lines

    @staticmethod
    def explode_value(value, indicator, n):

        if value is None:
            return [None for i in range(n)]

        def get_val(val):
            return val

        value /= 2

        if indicator in SUM_STATS:
            daily = value / n if n else 0
            if indicator != 'cost':
                def get_val(val):
                    return int(val)
        else:
            daily = value

        # chart values
        values = [daily for i in range(n)]

        val_len = len(values)

        bonus = value
        for n, i in enumerate(values):
            bonus /= 2
            values[n] = get_val(values[n] + bonus
                                if i < val_len - 1 else bonus * 2)

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
