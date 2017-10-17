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

        data = []

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

        result = {}
        for item in walker(genre):
            item['name'] = '/' + item['name']
            result[item['name']] = item['size']

        dt = DisassembleTree.init_from_flat_dict(result)
        full_genre = list(dt.as_dict()['children'])

        for item in walker(full_genre):
            if item.get('name').endswith('other'):
                continue
            name = '/' + item['name']
            item_id = list(self.values_to_keys.get(name))[0]
            item_size = item['size']
            data.append('{item_id}-{item_size}'.format(item_id=item_id,
                                                       item_size=item_size))
        return data

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
        query_params['limit'] = 5000
        query_params['fields'] = 'id,genre'
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


class DisassembleTree(object):
    name = None
    size = None
    children = None
    parent = None

    def __init__(self, name, size=None, parent=None):
        self.name = name
        self.size = size
        self.children = {}
        self.parent = parent
        self.completed = None

    @classmethod
    def init_from_flat_dict(cls, data):
        root = cls('', None)
        for name, size in data.items():
            root.update(name, size)
        root.update_parents()
        root.recalc_stats()
        return root

    def update(self, key, size):
        names = key.split('/')
        assert names[0] == self.name, 'Invalid root node'
        obj = self
        for name in names[1:]:
            parent = obj
            obj = parent.children.get(name, None)
            if obj is None:
                obj = self.__class__(name, parent=parent)
                parent.children[name] = obj
        obj.size = size

    def as_dict(self):
        result = {'name': self.name}
        result['size'] = self.size
        if self.children:
            result['children'] = [obj.as_dict() for obj in self.children.values()]
        return result

    @property
    def parents(self):
        obj = self.parent
        while obj is not None:
            yield obj
            obj = obj.parent

    def update_parents(self):
        for child in self.children.values():
            if child.children:
                child.update_parents()
            else:
                sizes = [child.size for child in self.children.values()]
                if None not in sizes:
                    self.size = sum(sizes)

        if self.size is None:
            self.size = sum([child.size for child in self.children.values()])

    def recalc_stats(self):
        for child in self.children.values():
            if child.children:
                child.recalc_stats()
            else:
                parents = list(filter(lambda x: bool(x.name), [p for p in child.parents]))
                if child.name == 'other':
                    continue
                if len(parents):
                    child.size = child.size / parents[0].size
                while parents:
                    if len(parents) < 2:
                        break
                    first = parents.pop(0)
                    if first.completed:
                        break
                    first.size = first.size / parents[0].size
                    first.completed = True
