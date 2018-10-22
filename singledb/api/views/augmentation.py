import csv
from io import StringIO
from itertools import groupby
from operator import itemgetter

from django.http import StreamingHttpResponse
from rest_framework.views import APIView

from segment.models.channel import SegmentRelatedChannel


class AugmentationChannelSegmentListApiView(APIView):
    permission_classes = tuple()

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
            # for row in self.get_segment_from():
            #     csvwriter.writerow(row)
            from segment.models.channel import SegmentChannel
            for segment in SegmentChannel.objects.all():
                row = (segment.id,
                       ','.join(
                           [d for d in segment.related.all().values_list('related_id', flat=True)])
                       )
                csvwriter.writerow(row)
            data = read_and_flush()
            yield data

        response = StreamingHttpResponse(data(),
                                         content_type="text/csv")
        response['Content-Disposition'] = 'attachment; filename="{file_name}.csv"'.format(file_name=file_name)
        return response
