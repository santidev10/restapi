from utils.youtube_api import YoutubeAPIConnector
from blacklist_video.models import BlacklistVideo
from queue import Queue
from threading import Thread
import csv
import re

class BlacklistVideos(object):
    video_batch_size = 5

    def __init__(self):
        self.yt_connector = YoutubeAPIConnector()

    def run(self):
        print('Starting video blacklist...')
        self.extract_video_id_seeds()
        self.get_videos_from_db()

        all_video_data = BlacklistVideo\
            .objects.all()\
            .values_list('video_id', 'channel_id', 'channel_title', 'title', 'description', flat=True)
        self.export_csv(data=all_video_data, headers=['Video ID', 'Channel ID', 'Channel Title', 'Video Title', 'Video Description'])

    def extract_video_id_seeds(self):
        video_id_seeds = []
        csv_file_path = '/Users/kennethoh/Desktop/blacklist.csv'

        with open(csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            csv_reader = csv.reader(csv_file)

            for row in csv_reader:
                video_id = row[0]
                video_id_seeds.append(video_id)

        print('Video seeds extracted: {}'.format(len(video_id_seeds)))

        while video_id_seeds:
            video_batch = video_id_seeds[:self.video_batch_size]
            video_ids_term = ','.join(video_batch)
            # Get metadata for the videos
            video_data = self.yt_connector.obtain_video_metadata(video_ids_term)['items']

            print('Creating {} videos.'.format(len(video_data)))
            self._bulk_create(video_data)

            video_id_seeds = video_id_seeds[self.video_batch_size - 1:]

        print('All seed ids added', BlacklistVideo.objects.all().count())

    def get_videos_from_db(self):
        video_ids = BlacklistVideo\
            .objects\
            .filter(scanned=False)\
            .distinct('video_id')\
            .values_list('video_id', flat=True)

        print('Videos to scan: {}'.format(len(video_ids)))

        while video_ids:
            for id in video_ids:
                video_data = self.yt_connector.get_related_videos(id)['items']
                print('Related videos retrieved: {}'.format(len(video_data)))

                self._bulk_create(video_data)

                total_videos = BlacklistVideo.objects.all().count()
                print('Total videos stored: {}'.format(total_videos))
                BlacklistVideo.objects.filter(video_id__in=video_ids).update(scanned=True)

                video_ids = BlacklistVideo \
                    .objects \
                    .filter(scanned=False) \
                    .distinct('video_id') \
                    .values_list('video_id', flat=True)

        print('Audit complete!')
        total_videos = BlacklistVideo.objects.all().count()
        print('Total videos stored: {}'.format(total_videos))

    def get_related_videos_worker(self):
        while True:
            # Get video ids list from queue
            video_id = self.get_related_videos_queue.get()
            video_data = self.yt_connector.get_related_videos(video_id)['items']

            print('Related videos retrieved: {}'.format(len(video_data)))

            self._bulk_create(video_data)
            self.get_videos_from_db()

            self.get_related_videos_queue.task_done()

    def _bulk_create(self, video_data):
        blacklist_to_create = [
            BlacklistVideo(
                video_id=video['id'] if type(video['id']) is str else video['id']['videoId'],
                channel_id=video['snippet']['channelId'],
                channel_title=video['snippet']['channelTitle'],
                title=video['snippet']['title'],
                description=video['snippet']['description'],
                scanned=False
            ) for video in video_data
        ]

        BlacklistVideo.objects.bulk_create(blacklist_to_create)

    def extract_channel_ids(self):
        csv_file_path = '/Users/kennethoh/Desktop/blacklist.csv'

        with open(csv_file_path) as csv_file:
            csv_reader = csv.reader(csv_file)

            for row in csv_reader:
                try:
                    # Extract channel id and add to queue to get all videos for channel
                    channel_id = re.search(r'(?<=channel/).*', row[0]).group()
                    self.get_videos_from_channel_queue.put(channel_id)

                except AttributeError:
                    pass


    def export_csv(self, data=None, headers=None, csv_export_path=None):
        if data is None:
            raise ValueError('You must provide a data source.')
        if headers is None:
            raise ValueError('You must provide a list of heaeders.')
        if csv_export_path is None:
            raise ValueError('You must provide a csv output path.')

        print('Exporting CSV to: {}'.format(csv_export_path))

        video_data = (video for video in data)

        with open(csv_export_path, mode='w') as csv_export:
            writer = csv.writer(csv_export, delimiter=',')
            writer.writerow(headers)

            for video in video_data:
                row = [item for item in video]
                writer.writerow(row)

        print('CSV export complete.')
