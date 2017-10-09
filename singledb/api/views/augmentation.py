import csv
from collections import defaultdict
from io import StringIO
from itertools import groupby
from operator import itemgetter

from django.http import StreamingHttpResponse
from rest_framework.views import APIView

from keyword_tool.models import Interest
from segment.models import SegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector, SingleDatabaseApiConnectorException


class AugmentationChannelListApiView(APIView):
    connector = Connector()
    values_to_keys = defaultdict(set)

    def fill_interests(self):
        interests_obj = dict(Interest.objects.values_list('id', 'name'))
        for key, value in interests_obj.items():
            self.values_to_keys[value].add(key)

    def make_genre_pretty(self, genre):
        if genre is None:
            return None

        result = []

        def walker(item, parent_cat=None):
            if isinstance(item, dict):
                if item.get('size'):
                    yield item
                if item.get('children'):
                    yield from walker(item['children'], parent_cat=item['name'])
            if isinstance(item, list):
                for i in item:
                    if parent_cat:
                        i['name'] = parent_cat + '/' + i['name']
                    yield from walker(i, parent_cat=parent_cat)

        for item in walker(genre):
            if item.get('name').endswith('other'):
                continue
            item['name'] = '/' + item['name']
            item_id = list(self.values_to_keys.get(item['name']))[0]
            item_size = item['size']
            result.append('{item_id}-{item_size}'.format(item_id=item_id,
                                                         item_size=item_size))
        return result

    def gen_channel_from(self, query_params):
        while True:

            try:
                response_data = self.connector.get_channel_list(query_params)
            except SingleDatabaseApiConnectorException:
                raise StopIteration

            result = []
            for channel in response_data['items']:
                result.append([channel['id'], self.make_genre_pretty(channel['genre'])])

            yield result

            if not response_data['current_page'] == response_data['max_page']:
                query_params['page'] += 1
                continue

            raise StopIteration

    def handler(self, request):
        query_params = request.query_params
        query_params._mutable = True
        query_params['page'] = 1
        yield from self.gen_channel_from(query_params)

    def get(self, request, *args, **kwargs):
        self.fill_interests()
        file_name = 'augmentation_channels'

        csvfile = StringIO()
        csvwriter = csv.writer(csvfile)

        def read_and_flush():
            csvfile.seek(0)
            data = csvfile.read()
            csvfile.seek(0)
            csvfile.truncate()
            return data

        def data():
            for row in self.handler(request):
                for elem in row:
                    if elem[1] is not None:
                        elem[1] = ",".join(elem[1])
                    csvwriter.writerow(elem)
            data = read_and_flush()
            yield data

        response = StreamingHttpResponse(data(),
                                         content_type="text/csv")
        response['Content-Disposition'] = 'attachment; filename="{file_name}.csv"'.format(file_name=file_name)
        return response


class AugmentationChannelSegmentListApiView(APIView):
    segments = list(SegmentRelatedChannel.objects.values('segment_id', 'related_id'))

    def get_segment_from(self):
        for k, v in groupby(self.segments, key=itemgetter('segment_id')):
            yield k, ','.join([d.get('related_id') for d in list(v)])

    def get(self, request, *args, **kwargs):
        file_name = 'augmentation_segments'
        csvfile = StringIO()
        csvwriter = csv.writer(csvfile)

        def read_and_flush():
            csvfile.seek(0)
            data = csvfile.read()
            csvfile.seek(0)
            csvfile.truncate()
            return data

        def data():
            for row in self.get_segment_from():
                csvwriter.writerow(row)
            data = read_and_flush()
            yield data

        response = StreamingHttpResponse(data(),
                                         content_type="text/csv")
        response['Content-Disposition'] = 'attachment; filename="{file_name}.csv"'.format(file_name=file_name)
        return response
