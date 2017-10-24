import csv
from collections import defaultdict
from io import StringIO
from itertools import groupby
from operator import itemgetter

from django.http import StreamingHttpResponse
from rest_framework.views import APIView

from keyword_tool.models import Interest
from segment.models.channel import SegmentRelatedChannel
from singledb.connector import SingleDatabaseApiConnector as Connector, SingleDatabaseApiConnectorException


class AugmentationChannelListApiView(APIView):
    permission_classes = tuple()
    connector = Connector()
    values_to_keys = defaultdict(set)

    def fill_interests(self):
        interests_obj = dict(Interest.objects.values_list('id', 'name'))
        for key, value in interests_obj.items():
            self.values_to_keys[value].add(key)

    def make_genre_pretty(self, raw_genre):
        if raw_genre is None:
            return None

        data = []
        for k, v in raw_genre.items():
            if k != '-2':
                data.append('{}-{}'.format(k, v))
        return data

    def gen_channel_from(self, query_params):
        while True:

            try:
                response_data = self.connector.get_channel_list(query_params)
            except SingleDatabaseApiConnectorException:
                raise StopIteration

            result = []
            for channel in response_data['items']:
                result.append([channel['id'], self.make_genre_pretty(channel['raw_genre'])])

            yield result

            if not response_data['current_page'] == response_data['max_page']:
                query_params['page'] += 1
                continue

            raise StopIteration

    def handler(self, request):
        query_params = request.query_params
        query_params._mutable = True
        query_params['page'] = 1
        query_params['limit'] = 5000
        query_params['min_subscribers_yt'] = 10000
        query_params['fields'] = 'id,raw_genre'
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
                    else:
                        continue
                    csvwriter.writerow(elem)
            data = read_and_flush()
            yield data

        response = StreamingHttpResponse(data(),
                                         content_type="text/csv")
        response['Content-Disposition'] = 'attachment; filename="{file_name}.csv"'.format(file_name=file_name)
        return response


class AugmentationChannelSegmentListApiView(APIView):

    def __init__(self):
        self.segments = list(SegmentRelatedChannel.objects.values('segment_id', 'related_id'))

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
